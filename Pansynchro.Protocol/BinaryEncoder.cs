using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;

using Pansynchro.Core;
using Pansynchro.Core.CustomTypes;
using Pansynchro.Core.DataDict;
#if DEBUG
using Pansynchro.Core.Streams;
#endif
namespace Pansynchro.Protocol
{
    public class BinaryEncoder : IWriter
    {
        private readonly Stream _output;
        private DataDictionary _dataDict = null!;
        private readonly TcpListener? _server;
        private readonly TcpClient? _client;
        private const int VERSION = 5;
        private readonly MemoryStream _bufferStream = new(BUFFER_SIZE);
        private readonly BinaryWriter _outputWriter;
#if DEBUG
        private readonly MeteredStream _meter;
#endif

        public BinaryEncoder(Stream output, int compressionLevel = 4)
        {
            var cl = GetCompressionLevel(compressionLevel);
            _output = new BrotliStream(output, cl, false);
#if DEBUG
            _meter = new MeteredStream(_output);
            _output = _meter;
#endif
            _outputWriter = new BinaryWriter(_output, Encoding.UTF8);
        }

        public BinaryEncoder(TcpListener server, DataDictionary dict, int compressionLevel = 4)
        {
            this._server = server;
            _server.Start();
            _client = _server.AcceptTcpClient();
            var buffer = new BufferedStream(_client.GetStream());
            var cl = GetCompressionLevel(compressionLevel);
            _output = new BrotliStream(buffer, cl, false);
#if DEBUG
            _meter = new MeteredStream(_output);
            _output = _meter;
#endif
            _outputWriter = new BinaryWriter(_output, Encoding.UTF8);
            _dataDict = dict;
        }

        // NOTE: This will no longer be valid in .NET 7, which introduces a breaking change (and a better
        // way of dealing with compression levels).  Fix it during the upgrade once .NET 7 is RTM.
        // https://github.com/dotnet/runtime/issues/42820
        private static CompressionLevel GetCompressionLevel(int level)
            => level switch {
                0 => CompressionLevel.NoCompression,
                1 or 2 or 3 => CompressionLevel.Fastest,
                4 or 5 or 6 or 7 or 8 or 9 or 10 => (CompressionLevel) level,
                11 => CompressionLevel.SmallestSize,
                _ => throw new ArgumentException("Valid Brotli compression levels are 0 - 11")
            };

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            _dataDict ??= dest;
            WriteVersion(_outputWriter);
            WriteDataDictHash(_outputWriter, _dataDict);
            using var bufferWriter = new BinaryWriter(_bufferStream, Encoding.UTF8);
            await foreach (var (name, settings, reader) in streams) {
                try {
#if DEBUG
                    var progress = _meter.TotalBytesWritten;
#endif
                    _outputWriter.Write((byte)Markers.Stream);
                    _outputWriter.Write(name.Namespace ?? "");
                    _outputWriter.Write(name.Name);
                    var schema = _dataDict.GetStream(name.ToString());
                    Debug.Assert(_bufferStream.Length == 0);
                    WriteStreamContents(reader, bufferWriter, schema,
                        settings.HasFlag(StreamSettings.UseRcf));
                    _outputWriter.Write((byte)0);
#if DEBUG
                    Console.WriteLine($"Stream {name} written with settings ({settings}): {(_meter.TotalBytesWritten - progress).ToString("N0")} bytes");
#endif
                }
                finally { 
                    reader.Dispose();
                }
            }
            _outputWriter.Write((byte)Markers.End);
        }

        private static bool RcfChanged(object l, object r)
        {
            if (l == null && r == null) {
                return false;
            }
            if (l == null || r == null) {
                return true;
            }
            return !l.Equals(r);
        }

        private const int BUFFER_LENGTH = 16 * 1024;
        private readonly object[][] _buffer = new object[BUFFER_LENGTH][];

        private void WriteStreamContents(IDataReader reader, BinaryWriter writer, StreamDefinition schema, bool useRcf)
        {
            _outputWriter.Write((byte)StreamMode.InsertOnly);
            Action<object, BinaryWriter>[] columnWriters = BuildColumnWriters(reader, schema);
            //loop until it returns false
            while (ProcessBlock(reader,
                columnWriters,
                useRcf ? schema.RareChangeFields.Length : 0,
                schema.SeqIdIndex))
            { }
            _outputWriter.Write((byte)0);
        }

        private const int BUFFER_SIZE = 1024 * 1024;

        private void FlushBuffer()
        {
            _outputWriter.Write7BitEncodedInt((int)_bufferStream.Length + sizeof(int));
            _bufferStream.WriteTo(_output);
            _outputWriter.Write(Crc32(_bufferStream.GetBuffer(), _bufferStream.Length));
            _bufferStream.SetLength(0);
        }

        private bool ProcessBlock(IDataReader reader, Action<object, BinaryWriter>[] rowWriters, int rcfCount, int? sequentialID)
        {
            var rowSize = reader.FieldCount;
            int i = 0;
            var result = true;
            while (i < BUFFER_LENGTH) {
                if (!reader.Read()) {
                    result = false;
                    break;
                }
                var arr = GetBufferRow(i, rowSize);
                reader.GetValues(arr);
                ++i;
            }
            WriteBlock(i, rowSize, rowWriters, rcfCount, sequentialID);
            return result;
        }

        private void WriteBlock(int count, int rowSize, Action<object, BinaryWriter>[] rowWriters,
            int rcfCount, int? sequentialID)
        {
            using var writer = new BinaryWriter(_bufferStream, Encoding.UTF8, true);
            writer.Write7BitEncodedInt(count);
            var normalColumnCount = rowSize - rcfCount;
            for (int i = 0; i < normalColumnCount; ++i) {
                if (i == sequentialID) {
                    WriteSequentialIDColumn(i, count, writer);
                } else WriteColumn(i, rowWriters[i], count, writer);
            }
            for (int i = normalColumnCount; i < rowSize; ++i) {
                WriteRcfColumn(i, rowWriters[i], count, writer);
            }
            FlushBuffer();
        }

        private void WriteColumn(int column, Action<object, BinaryWriter> action, int count,
            BinaryWriter writer)
        {
            for (int i = 0; i < count; ++i) {
                action(_buffer[i][column], writer);
            }
        }

        private void WriteRcfColumn(int column, Action<object, BinaryWriter> action, int count,
            BinaryWriter writer)
        {
            int lastIdx = 0;
            while (lastIdx < count) {
                var value = _buffer[lastIdx][column];
                action(value, writer);
                var runEnd = lastIdx;
                for (int i = lastIdx + 1; i < count; ++i) {
                    if (RcfChanged(value, _buffer[i][column])) {
                        runEnd = i;
                        break;
                    }
                }
                if (runEnd == lastIdx) {
                    runEnd = count;
                }
                writer.Write7BitEncodedInt(runEnd - lastIdx);
                lastIdx = runEnd;
            }
        }

        private void WriteSequentialIDColumn(int column, int count, BinaryWriter writer)
        {
            if (count > 0) {
                var type = _buffer[0][column].GetType();
                if (type == typeof(int)) {
                    WriteSequentialIntColumn(column, count, writer);
                } else {
                    if (type != typeof(long)) {
                        throw new DataException($"Unknown sequential ID column type: {type.FullName}");
                    }
                    WriteSequentialLongColumn(column, count, writer);
                }
            }
        }

        private void WriteSequentialIntColumn(int column, int count, BinaryWriter writer)
        {
            var last = (int)_buffer[0][column];
            writer.Write7BitEncodedInt(last);
            for (int i = 1; i < count; ++i) {
                var next = (int)_buffer[i][column];
                writer.Write7BitEncodedInt(next - last);
                last = next;
            }
        }

        private void WriteSequentialLongColumn(int column, int count, BinaryWriter writer)
        {
            var last = (long)_buffer[0][column];
            writer.Write7BitEncodedInt64(last);
            for (int i = 1; i < count; ++i) {
                var next = (long)_buffer[i][column];
                writer.Write7BitEncodedInt64(next - last);
                last = next;
            }
        }

        private object[] GetBufferRow(int idx, int rowSize)
        {
            var result = _buffer[idx];
            if (result == null) {
                result = new object[rowSize];
                _buffer[idx] = result;
            } else if (result.Length != rowSize) {
                Array.Resize(ref result, rowSize);
                _buffer[idx] = result;
            }
            return result;
        }

        private static void WriteVersion(BinaryWriter writer)
        {
            writer.Write("PANSYNCHRO");
            writer.Write((byte)Markers.Version);
            writer.Write7BitEncodedInt(VERSION);
        }

        private static void WriteDataDictHash(BinaryWriter writer, DataDictionary dict)
        {
            writer.Write((byte)Markers.Schema);
            writer.Write(dict.Name);
            using var hasher = MD5.Create();
            var text = DataDictionaryWriter.Write(dict).Replace("\r\n", "\n");
            var textBytes = Encoding.UTF8.GetBytes(text);
            //WriteBin(textBytes);
            using var stream = new MemoryStream(textBytes);
            var hash = hasher.ComputeHash(stream);
            writer.Write7BitEncodedInt(hash.Length);
            writer.Write(hash);
        }

        private static void WriteBin(byte[] bytes)
        {
            int i = 1;
            foreach(var b in bytes) {
                Console.Write(b);
                if (i % 25 == 0) {
                    Console.WriteLine();
                } else {
                    Console.Write(' ');
                }
                ++i;
            }
            Console.WriteLine();
        }

        /*
        private static void WriteDataDict(BinaryWriter writer, DataDictionary dict)
        {
            writer.Write((byte)Markers.Schema);
            writer.Write(dict.Name);
            writer.Write7BitEncodedInt(dict.Streams.Length);
            for (int i = 0; i < dict.Streams.Length; ++i)
            {
                writer.Write7BitEncodedInt(i + 1);
                WriteSchema(writer, dict.Streams[i]);
            }
            writer.Write((byte)0);
            var counter = 0;
            writer.Write7BitEncodedInt(dict.CustomTypes.Count);
            foreach (var pair in dict.CustomTypes)
            {
                ++counter;
                writer.Write7BitEncodedInt(counter);
                writer.Write(pair.Key);
                WriteType(writer, pair.Value);
            }
            writer.Write((byte)0);
            writer.Write7BitEncodedInt(dict.DependencyOrder.Length);
            for (int i = 0; i < dict.DependencyOrder.Length; ++i)
            {
                writer.Write7BitEncodedInt(i + 1);
                WriteDeps(writer, dict.DependencyOrder[i]);
            }
            var incremental = dict.Incremental.ToLookup(kvp => kvp.Value, kvp => kvp.Key);
            var streamNames = dict.Streams.Select(s => s.Name).ToArray();
            writer.Write((byte)incremental.Count);
            byte count = 0;
            foreach (var grouping in incremental)
            {
                ++count;
                writer.Write(count);
                writer.Write((byte)grouping.Key);
                var values = grouping.ToArray();
                writer.Write7BitEncodedInt(values.Length);
                foreach (var name in values)
                {
                    writer.Write7BitEncodedInt(Array.IndexOf(streamNames, name));
                }
                writer.Write((byte)0);
            }
            writer.Write((byte)0);
        }

        private static void WriteDeps(BinaryWriter writer, StreamDescription[] desc)
        {
            writer.Write7BitEncodedInt(desc.Length);
            for (int i = 0; i < desc.Length; ++i)
            {
                writer.Write7BitEncodedInt(i + 1);
                writer.Write(desc[i].Namespace ?? "");
                writer.Write(desc[i].Name);
            }
        }

        private static void WriteSchema(BinaryWriter writer, StreamDefinition schema)
        {
            writer.Write(schema.Name.Namespace ?? string.Empty);
            writer.Write(schema.Name.Name);
            writer.Write7BitEncodedInt(schema.Fields.Length);
            var count = 0;
            foreach (var field in schema.Fields)
            {
                ++count;
                writer.Write7BitEncodedInt(count);
                writer.Write(field.Name);
                WriteType(writer, field.Type);
            }
            writer.Write((byte)0);
            writer.Write7BitEncodedInt(schema.Identity.Length);
            foreach (var column in schema.Identity)
            {
                writer.Write(column);
            }
            writer.Write((byte)0);
        }

        private static void WriteType(BinaryWriter writer, FieldType type)
        {
            writer.Write7BitEncodedInt((int)type.Type);
            writer.Write((byte)type.CollectionType);
            writer.Write(type.Nullable);
        }
        */

        private static Action<object, BinaryWriter>[] BuildColumnWriters(IDataReader reader, StreamDefinition schema)
        {
            Debug.Assert(reader.FieldCount == schema.Fields.Length);
            var len = schema.Fields.Length;
            var result = new Action<object, BinaryWriter>[len];
            var drs = new Dictionary<string, long>(schema.DomainReductions);
            for (int i = 0; i < len; ++i) {
                var field = schema.Fields[i];
                var type = field.Type;
                drs.TryGetValue(field.Name, out var dr);
                Action<object, BinaryWriter> writer = GetWriter(i, type, dr);
                result[i] = writer;
            }
            return result;
        }

        private static Action<object, BinaryWriter> GetWriter(int i, FieldType type, long domainReduction)
        {
            Action<object, BinaryWriter> writer = type.Type switch
            {
                TypeTag.Unstructured => Unimplemented(type, i),
                TypeTag.Custom => Unimplemented(type, i),
                TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext or TypeTag.Xml
                    => (o, s) => s.Write((string)o),
                TypeTag.Json => (o, s) => s.Write(((JToken)o).ToString()),
                TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob
                    => MakeBytesWriter(),
                TypeTag.Boolean => (o, s) => s.Write((bool)o),
                TypeTag.Byte => (o, s) => s.Write((byte)o),
                TypeTag.Short => (o, s) => s.Write7BitEncodedInt((short)o),
                TypeTag.Int => (o, s) => s.Write7BitEncodedInt((int)o),
                TypeTag.Long => (o, s) => s.Write7BitEncodedInt64((long)o),
                TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney => (o, s) => s.Write((decimal)o),
                TypeTag.Single => (o, s) => s.Write((float)o),
                TypeTag.Float or TypeTag.Double => (o, s) => s.Write((double)o),
                TypeTag.TimeTZ => Unimplemented(type, i),
                TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime => MakeDateTimeWriter(domainReduction),
                TypeTag.DateTimeTZ => MakeDateTimeTZWriter(domainReduction),
                TypeTag.VarDateTime => Unimplemented(type, i),
                TypeTag.Guid => MakeGuidWriter(),
                TypeTag.Time or TypeTag.Interval => MakeTimeSpanWriter(),
                TypeTag.Bits => Unimplemented(type, i),
                _ => Unimplemented(type, i)
            };
            if (type.Nullable) {
                writer = MakeNullable(writer);
            }

            return writer;
        }

        private static Action<object, BinaryWriter> MakeTimeSpanWriter() => (o, s) => 
            s.Write7BitEncodedInt64(((TimeSpan)o).Ticks);

        private static Action<object, BinaryWriter> MakeGuidWriter() => (o, s) =>
        {
            var g = new GuidConverter((Guid)o);
            s.Write(g.High);
            s.Write(g.Low);
        };

        private static Action<object, BinaryWriter> MakeDateTimeTZWriter(long domainReduction) => (o, s) =>
        {
            var dto = (DateTimeOffset)o;
            s.Write7BitEncodedInt64(dto.DateTime.Ticks - domainReduction);
            s.Write((short)dto.Offset.TotalMinutes);
        };

        private static Action<object, BinaryWriter> MakeDateTimeWriter(long domainReduction) => (o, s) => s.Write7BitEncodedInt64(((DateTime)o).Ticks - domainReduction);

        private static Action<object, BinaryWriter> MakeNullable(Action<object, BinaryWriter> writer)
            => (o, s) =>
        {
            if (o is null || o == System.DBNull.Value)
            {
                s.Write(false);
            } else {
                s.Write(true);
                writer(o, s);
            }
        };

        private static Action<object, BinaryWriter> MakeBytesWriter() => (o, s) =>
        {
            var data = (byte[])o;
            s.Write7BitEncodedInt(data.Length);
            s.Write(data);
        };

        private static Action<object, BinaryWriter> Unimplemented(FieldType type, int i)
        {
            var customType = CustomTypeRegistry.GetType(type.Type);
            if (customType == null) {
                throw new NotImplementedException($"No writer implemented for '{type.Type}'.");
            }
            return customType.ProtocolWriter;
        }

        private static uint Crc32(byte[] row, long length) => Force.Crc32.Crc32CAlgorithm.Compute(row, 0, (int)length);

        public void Dispose()
        {
            _output.Dispose();
            if (_server != null) {
                _client!.Dispose();
                _server.Stop();
            }
            GC.SuppressFinalize(this);
        }

        public static BinaryEncoder Connect(string connectionString)
        {
            var parts = connectionString.Split(';');
            var server = new TcpListener(IPAddress.Parse(parts[0]), NetworkInfo.TCP_PORT);
            var srcdict = DataDictionary.LoadFromFile(parts[1]);
            return new BinaryEncoder(server, srcdict);
        }
    }
}
