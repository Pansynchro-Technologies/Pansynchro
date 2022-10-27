using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;

using Pansynchro.Core;
using Pansynchro.Core.CustomTypes;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Streams;

namespace Pansynchro.Protocol
{
    public class BinaryDecoder : IReader
    {
        private readonly TcpClient? _client;
        private readonly MeteredStream _meter;
        private readonly BrotliStream _decompressor;
        private readonly BinaryReader _reader;
        private readonly DataDictionary _sourceDict;

        private const int VERSION = 5;

        public BinaryDecoder(TcpClient client, DataDictionary sourceDict) : this(client.GetStream(), sourceDict)
        {
            _client = client;
        }

        public BinaryDecoder(Stream source, DataDictionary sourceDict)
        {
            _meter = new MeteredStream(source);
            _decompressor = new BrotliStream(_meter, CompressionMode.Decompress);
            _reader = new BinaryReader(_decompressor, Encoding.UTF8);
            _sourceDict = sourceDict;
        }

        public IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            ReadVersion();
            ReadDictHash();
            return ReadStreams(source.Streams.Length, source);
        }

        private void ReadVersion()
        {
            CheckEqual(_reader.ReadString(), "PANSYNCHRO", "Server validation check failed");
            CheckEqual(_reader.ReadByte(), (byte)Markers.Version, "Protocol version marker missing");
            CheckEqual(_reader.Read7BitEncodedInt(), VERSION, "Protocol version check failed");
        }

        private void ReadDictHash()
        {
            CheckEqual(_reader.ReadByte(), (byte)Markers.Schema, "Data dictionary not found");
            CheckEqual(_reader.ReadString(), _sourceDict.Name, "Data dictionary name check failed");
            using var hasher = MD5.Create();
            var text = DataDictionaryWriter.Write(_sourceDict).Replace("\r\n", "\n");
            var textBytes = Encoding.UTF8.GetBytes(text);
            //WriteBin(textBytes);
            using var stream = new MemoryStream(textBytes);
            var hash = hasher.ComputeHash(stream);
            var srcHash = _reader.ReadBytes(_reader.Read7BitEncodedInt());
            CheckEqual(hash, srcHash, "Data dictionary hash check failed");
        }

        private static void WriteBin(byte[] bytes)
        {
            int i = 1;
            foreach (var b in bytes) {
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
        private DataDictionary ReadDict()
        {
            CheckEqual(_reader.ReadByte(), (byte)Markers.Schema, "Data dictionary not found");
            var name = _reader.ReadString();
            var len = _reader.Read7BitEncodedInt();
            var streams = new StreamDefinition[len];
            for (int i = 0; i < len; ++i)
            {
                CheckEqual(_reader.Read7BitEncodedInt(), i + 1, "Data dictionary schema check failed");
                streams[i] = ReadSchema();
            }
            CheckEqual(_reader.ReadByte(), 0, "Data dictionary end of stream section check failed");
            len = _reader.Read7BitEncodedInt();
            var cTypes = new Dictionary<string, FieldType>();
            for (int i = 0; i < len; ++i)
            {
                CheckEqual(_reader.Read7BitEncodedInt(), i + 1, "Data dictionary custom type order check failed");
                var typeName = _reader.ReadString();
                var type = ReadType();
                cTypes.Add(typeName, type);
            }
            CheckEqual(_reader.ReadByte(), 0, "Data dictionary end of custom types section check failed");
            len = _reader.Read7BitEncodedInt();
            var deps = new StreamDescription[len][];
            for (int i = 0; i < len; ++i)
            {
                CheckEqual(_reader.Read7BitEncodedInt(), i + 1, "Data dictionary dependency order check failed");
                deps[i] = ReadDeps();
            }
            var incremental = new Dictionary<StreamDescription, IncrementalStrategy>();
            len = _reader.ReadByte();
            for (int i = 0; i < len; ++i)
            {
                CheckEqual(_reader.ReadByte(), i + 1, "Data dictionary incremental data check failed");
                var strat = (IncrementalStrategy)_reader.ReadByte();
                var count = _reader.Read7BitEncodedInt();
                for (int j = 0; j < count; ++j)
                {
                    var idx = _reader.Read7BitEncodedInt();
                    incremental.Add(streams[idx].Name, strat);
                }
                CheckEqual(_reader.ReadByte(), 0, "Data dictionary incremental group end check failed");
            }
            CheckEqual(_reader.ReadByte(), 0, "Data dictionary end check failed");
            return new DataDictionary(name, streams, deps, cTypes, incremental);
        }

        private FieldType ReadType()
        {
            var tag = (TypeTag)_reader.Read7BitEncodedInt();
            var collType = (CollectionType)_reader.ReadByte();
            var nullable = _reader.ReadBoolean();
            return new FieldType(tag, nullable, collType, null);
        }

        private StreamDefinition ReadSchema()
        {
            var ns = _reader.ReadString();
            var name = _reader.ReadString();
            var len = _reader.Read7BitEncodedInt();
            var fields = new FieldDefinition[len];
            for (int i = 0; i < len; ++i)
            {
                CheckEqual(_reader.Read7BitEncodedInt(), i + 1, "Field order check failed");
                var fieldName = _reader.ReadString();
                var type = ReadType();
                fields[i] = new FieldDefinition(fieldName, type);
            }
            CheckEqual(_reader.ReadByte(), 0, "Field list end check failed");
            len = _reader.Read7BitEncodedInt();
            var pks = new List<string>();
            for (int i = 0; i < len; ++i)
            {
                pks.Add(_reader.ReadString());
            }
            CheckEqual(_reader.ReadByte(), 0, "PK end check failed");
            return new StreamDefinition(new StreamDescription(ns == "" ? null : ns, name), fields.ToArray(), pks.ToArray()); ;
        }

        private StreamDescription[] ReadDeps()
        {
            var len = _reader.Read7BitEncodedInt();
            var result = new StreamDescription[len];
            for (int i = 0; i < len; ++i)
            {
                CheckEqual(_reader.Read7BitEncodedInt(), i + 1, "Stream description order check failed");
                var ns = _reader.ReadString();
                var name = _reader.ReadString();
                result[i] = new StreamDescription(ns == "" ? null : ns, name);
            }
            return result;
        }
*/

        private static void CheckEqual(int value, int expected, string errorMsg)
        {
            if (value != expected)
            {
                throw new InvalidDataException($"{errorMsg}.  Expected {expected} but read {value}.");
            }
        }

        private static void CheckEqual(string value, string expected, string errorMsg)
        {
            if (!value.Equals(expected, StringComparison.Ordinal))
            {
                throw new InvalidDataException($"{errorMsg}.  Expected {expected} but read {value}.");
            }
        }

        private static void CheckEqual<T>(T[] value, T[] expected, string errorMsg)
        {
            if (!value.SequenceEqual(expected))
            {
                throw new InvalidDataException(errorMsg);
            }
        }

        private async IAsyncEnumerable<DataStream> ReadStreams(int length, DataDictionary dict)
        {
            var count = 0;
            bool locked = false;
            while (count < length) {
                ++count;
                if (locked) {
                    throw new IOException("Attempted to retrieve a new DataStream before exhausting the previous one.");
                }
                CheckEqual(_reader.ReadByte(), (byte)Markers.Stream, "Stream marker not found.");
                var ns = _reader.ReadString();
                var name = _reader.ReadString();
                // change this once differential updates are a thing
                var mode = (StreamMode)_reader.ReadByte();
                if (mode != StreamMode.InsertOnly) {
                    throw new InvalidDataException("Invalid stream mode.");
                }
                var streamName = new StreamDescription(ns == "" ? null : ns, name);
                var schema = dict.GetStream(streamName.ToString());
                locked = true;
                yield return new DataStream(streamName, StreamSettings.None, new StreamingReader(_reader, schema, dict, mode, () => locked = false));
            }
            CheckEqual(_reader.ReadByte(), (byte)Markers.End, "Expected end of stream not found.");
            Console.WriteLine($"Bytes read: {_meter.TotalBytesRead.ToString("N0")}");
            await Task.CompletedTask; // just to shut the compiler up
        }

        internal static Func<BinaryReader, object>[] BuildBufferDecoders(StreamDefinition schema, DataDictionary dict)
        {
            var len = schema.Fields.Length;
            var result = new Func<BinaryReader, object>[schema.Fields.Length];
            var drs = new Dictionary<string, long>(schema.DomainReductions);
            for (int i = 0; i < len; ++i) {
                var type = schema.Fields[i].Type;
                drs.TryGetValue(schema.Fields[i].Name, out var dr);
                result[i] = GetReader(i, type, dr, dict);
            }
            return result;
        }

        private static Func<BinaryReader, object> GetReader(int i, FieldType type, long domainReduction, DataDictionary dict)
        {
            Func<BinaryReader, object> reader = type.Type switch
            {
                TypeTag.Unstructured => Unimplemented(type),
                TypeTag.Custom => GetCustomTypeReader(i, type, domainReduction, dict),
                TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext or TypeTag.Xml
                    => StringReader,
                TypeTag.Json => JsonReader,
                TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob => BytesReader,
                TypeTag.Boolean => BoolReader,
                TypeTag.Byte => ByteReader,
                TypeTag.Short => ShortReader,
                TypeTag.Int => IntReader,
                TypeTag.Long => LongReader,
                TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney => DecimalReader,
                TypeTag.Single => SingleReader,
                TypeTag.Float or TypeTag.Double => DoubleReader,
                TypeTag.TimeTZ => Unimplemented(type),
                TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime => MakeDateTimeReader(i, domainReduction),
                TypeTag.DateTimeTZ => MakeDateTimeTZReader(i, domainReduction),
                TypeTag.VarDateTime => Unimplemented(type),
                TypeTag.Guid => GuidReader,
                TypeTag.Time or TypeTag.Interval => TimeSpanReader,
                TypeTag.Bits => Unimplemented(type),
                _ => Unimplemented(type)
            };
            if (type.Nullable) {
                reader = MakeNullable(reader);
            }

            return reader;
        }

        private static Func<BinaryReader, object> GetCustomTypeReader(int i, FieldType type, long domainReduction, DataDictionary dict)
        {
            if (dict.CustomTypes.TryGetValue(type.Info!, out var ft)) {
                return GetReader(i, ft, domainReduction, dict);
            }
            return Unimplemented(type);
        }

        private static Func<BinaryReader, object> MakeNullable(Func<BinaryReader, object> reader) => r =>
            r.ReadBoolean() ? reader(r) : DBNull.Value;

        private static object BytesReader(BinaryReader r) => r.ReadBytes(r.Read7BitEncodedInt());

        private static object StringReader(BinaryReader r) => r.ReadString();

        private static object BoolReader(BinaryReader r) => r.ReadBoolean();

        private static object ByteReader(BinaryReader r) => r.ReadByte();

        private static object ShortReader(BinaryReader r) => (short)r.Read7BitEncodedInt();

        private static object IntReader(BinaryReader r) => r.Read7BitEncodedInt();

        private static object LongReader(BinaryReader r) => r.Read7BitEncodedInt64();

        private static object DecimalReader(BinaryReader r) => r.ReadDecimal();

        private static object SingleReader(BinaryReader r) => r.ReadSingle();

        private static object DoubleReader(BinaryReader r) => r.ReadDouble();

        private static Func<BinaryReader, object> MakeDateTimeReader(int i, long dr) => r => new DateTime(r.Read7BitEncodedInt64() + dr);

        private static Func<BinaryReader, object> MakeDateTimeTZReader(int i, long dr) => r =>
        {
            var dt = new DateTime(r.Read7BitEncodedInt64() + dr);
            var o = TimeSpan.FromMinutes(r.ReadInt16());
            return new DateTimeOffset(dt, o);
        };

        private static object GuidReader(BinaryReader r)
        {
            var high = r.ReadUInt64();
            var low = r.ReadUInt64();
            var conv = new GuidConverter(low, high);
            return conv.Value;
        }

        private static object TimeSpanReader(BinaryReader r) => TimeSpan.FromTicks(r.Read7BitEncodedInt64());

        private static object JsonReader(BinaryReader r) => JToken.Parse(r.ReadString());
        
        private static Func<BinaryReader, object> Unimplemented(FieldType type)
        {
            var customType = CustomTypeRegistry.GetType(type.Type);
            if (customType == null) {
                throw new NotImplementedException($"No reader implemented for '{type.Type}'.");
            }
            return customType.ProtocolReader;
        }

        Task<Exception?> IReader.TestConnection() => Task.FromResult<Exception?>(null);

        public void Dispose()
        {
            var source = _reader.BaseStream;
            _reader.Dispose();
            if (_client != null) {
                source.Dispose();
                _client.Dispose();
            }
            GC.SuppressFinalize(this);
        }

        public static BinaryDecoder Connect(string connectionString)
        {
            var parts = connectionString.Split(';');
            var client = new TcpClient(parts[0], NetworkInfo.TCP_PORT);
            var srcdict = DataDictionary.LoadFromFile(parts[1]);
            return new BinaryDecoder(client, srcdict);
        }
    }
}
