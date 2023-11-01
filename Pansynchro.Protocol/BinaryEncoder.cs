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
using System.Threading.Channels;
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
		private const int VERSION = 6;

		private readonly Stream _output;
		private DataDictionary _dataDict = null!;
		private readonly TcpListener? _server;
		private readonly TcpClient? _client;
		private readonly MemoryStream _bufferStream = new(BUFFER_SIZE);
		private readonly BinaryWriter _outputWriter;
		//private readonly BinaryWriter _incompressibleWriter;
		private readonly BrotliStream _compressor;
		private readonly MeteredStream _meter;
#endif

		public BinaryEncoder(Stream output, int compressionLevel = 4)
		{
			_compressor = new(output, CompressionLevel.Optimal);
			_output = _compressor;
#if DEBUG
			_meter = new MeteredStream(_output);
			_output = _meter;
#endif
			_outputWriter = new BinaryWriter(_output, Encoding.UTF8);
			//_incompressibleWriter = new BinaryWriter(output, Encoding.UTF8);
		}

		public BinaryEncoder(TcpListener server, DataDictionary dict, int compressionLevel = 4)
		{
			this._server = server;
			_server.Start();
			_client = _server.AcceptTcpClient();
			var buffer = new BufferedStream(_client.GetStream());
			_compressor = new(buffer, CompressionLevel.Optimal);
			_output = _compressor;
#if DEBUG
			_meter = new MeteredStream(_output);
			_output = _meter;
#endif
			_outputWriter = new BinaryWriter(_output, Encoding.UTF8);
			//_incompressibleWriter = new BinaryWriter(buffer, Encoding.UTF8);
			_dataDict = dict;
		}

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
					await WriteStreamContents(reader, bufferWriter, schema,
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
			_outputWriter.Flush();
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
		private object[][] _buffer = new object[BUFFER_LENGTH][];
		private static readonly BoundedChannelOptions CHANNEL_OPTIONS
			= new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = true };

		private async Task WriteStreamContents(
			IDataReader reader, BinaryWriter writer, StreamDefinition schema, bool useRcf)
		{
			_outputWriter.Write((byte)StreamMode.InsertOnly);
			var incompressibles = new List<int>();
			var rcfWriters = new List<IRcfWriter>();
			Action<object, BinaryWriter>[] columnWriters = BuildColumnWriters(reader, schema, _dataDict, incompressibles, rcfWriters);
			var ch = Channel.CreateBounded<WriteJob>(CHANNEL_OPTIONS);
			var writeTask = Task.Run(() => WriteBlocks(ch.Reader));
			try
			{
				//loop until it returns false
				while (await ProcessBlock(reader, columnWriters, schema.SeqIdIndex, ch.Writer, incompressibles, rcfWriters))
				{ }
				ch.Writer.Complete();
			} catch (Exception e) {
				ch.Writer.Complete(e);
				throw;
			}
			await writeTask;
			_outputWriter.Write((byte)0);
		}

		private const int BUFFER_SIZE = 1024 * 1024;

		private void FlushBuffer(BinaryWriter writer)
		{
			writer.Write7BitEncodedInt((int)_bufferStream.Length + sizeof(int));
			_bufferStream.WriteTo(writer.BaseStream);
			writer.Write(Crc32(_bufferStream.GetBuffer(), (int)_bufferStream.Length));
			_bufferStream.SetLength(0);
		}

		private async ValueTask<bool> ProcessBlock(
			IDataReader reader, Action<object, BinaryWriter>[] rowWriters,
			int? sequentialID, ChannelWriter<WriteJob> writer, List<int> incompressibles, List<IRcfWriter> rcfWriters)
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
			await WriteBlock(i, rowSize, rowWriters, sequentialID, writer, !incompressibles.Contains(i), rcfWriters);
			return result;
		}

		private record WriteJob(object[][] Buffer, int Count, int RowSize,
			Action<object, BinaryWriter>[] RowWriters, int? SequentialID, BinaryWriter Writer, List<IRcfWriter> RcfWriters);

		private async ValueTask WriteBlock(int count, int rowSize, Action<object, BinaryWriter>[] rowWriters,
			int? sequentialID, ChannelWriter<WriteJob> writer, bool useCompression, List<IRcfWriter> rcfWriters)
		{
			var job = new WriteJob(_buffer, count, rowSize, rowWriters, sequentialID, _outputWriter, rcfWriters);
			await writer.WriteAsync(job);
			_buffer = new object[BUFFER_LENGTH][];
			//DoWriteBlock(_buffer, count, rowSize, rowWriters, rcfCount, sequentialID);
		}

		private async Task WriteBlocks(ChannelReader<WriteJob> reader)
		{
			await foreach (var job in reader.ReadAllAsync()) {
				DoWriteBlock(job.Buffer, job.Count, job.RowSize, job.RowWriters, job.SequentialID, job.Writer, job.RcfWriters);
			}
		}

		private void DoWriteBlock(object[][] buffer, int count, int rowSize,
			Action<object, BinaryWriter>[] rowWriters, int? sequentialID, BinaryWriter outputWriter, List<IRcfWriter> rcfWriters)
		{
			using var writer = new BinaryWriter(_bufferStream, Encoding.UTF8, true);
			writer.Write7BitEncodedInt(count);
			for (int i = 0; i < rowSize; ++i) {
				if (i == sequentialID) {
					WriteSequentialIDColumn(buffer, i, count, writer);
				} else WriteColumn(buffer, i, rowWriters[i], count, writer);
			}
			FlushBuffer(outputWriter);
			if (rcfWriters.Count > 0) {
				UpdateRcfData(rcfWriters, outputWriter);
			}
		}

		private static void UpdateRcfData(List<IRcfWriter> rcfWriters, BinaryWriter outputWriter)
		{
			using var ms = new MemoryStream();
			using var bw = new BinaryWriter(ms);
			var written = 0;
			for (int i = 0; i < rcfWriters.Count; ++i) {
				var writer = rcfWriters[i];
				if (writer.NewData > 0) {
					++written;
					bw.Write7BitEncodedInt(i);
					bw.Write7BitEncodedInt(writer.NewData);
					writer.FinishBlock(bw);
				}
			}
			if (written > 0) {
				outputWriter.Write7BitEncodedInt(written);
				ms.WriteTo(outputWriter.BaseStream);
			} else {
				outputWriter.Write((byte)0);
			}
		}

		private static void WriteColumn(object[][] buffer, int column,
			Action<object, BinaryWriter> action, int count, BinaryWriter writer)
		{
			for (int i = 0; i < count; ++i) {
				action(buffer[i][column], writer);
			}
		}

		private static void WriteRcfColumn(object[][] buffer, int column,
			Action<object, BinaryWriter> action, int count, BinaryWriter writer)
		{
			int lastIdx = 0;
			while (lastIdx < count) {
				var value = buffer[lastIdx][column];
				action(value, writer);
				var runEnd = lastIdx;
				for (int i = lastIdx + 1; i < count; ++i) {
					if (RcfChanged(value, buffer[i][column])) {
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

		private static void WriteSequentialIDColumn(object[][] buffer, int column, int count,
			BinaryWriter writer)
		{
			if (count > 0) {
				var type = buffer[0][column].GetType();
				if (type == typeof(int)) {
					WriteSequentialIntColumn(buffer, column, count, writer);
				} else {
					if (type != typeof(long)) {
						throw new DataException($"Unknown sequential ID column type: {type.FullName}");
					}
					WriteSequentialLongColumn(buffer, column, count, writer);
				}
			}
		}

		private static void WriteSequentialIntColumn(object[][] buffer, int column, int count,
			BinaryWriter writer)
		{
			var last = (int)buffer[0][column];
			writer.Write7BitEncodedInt(last);
			for (int i = 1; i < count; ++i) {
				var next = (int)buffer[i][column];
				writer.Write7BitEncodedInt(next - last);
				last = next;
			}
		}

		private static void WriteSequentialLongColumn(object[][] buffer, int column, int count,
			BinaryWriter writer)
		{
			var last = (long)buffer[0][column];
			writer.Write7BitEncodedInt64(last);
			for (int i = 1; i < count; ++i) {
				var next = (long)buffer[i][column];
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

		private static Action<object, BinaryWriter>[] BuildColumnWriters(
			IDataReader reader, StreamDefinition schema, DataDictionary dict, List<int> incompressibles, List<IRcfWriter> rcfFinalizers)
		{
			Debug.Assert(reader.FieldCount == schema.Fields.Length);
			var len = schema.Fields.Length;
			var result = new Action<object, BinaryWriter>[len];
			var drs = new Dictionary<string, long>(schema.DomainReductions);
			for (int i = 0; i < len; ++i) {
				var field = schema.Fields[i];
				var type = field.Type;
				drs.TryGetValue(field.Name, out var dr);
				var isRcf = schema.RareChangeFields.Contains(field.Name);
				// for the moment, arrays will not be treated as RCFs, in the interest of ease of implementation.
				// This may change if anyone comes up with a valid use case
				Action<object, BinaryWriter> writer = (isRcf && type.CollectionType == CollectionType.None)
					? GetRcfWriter(i, type, dr, dict, rcfFinalizers)
					: GetWriter(i, type, dr, dict);
				result[i] = writer;
				if (type.Incompressible) {
					incompressibles.Add(i);
				}
			}
			return result;
		}

		private static Action<object, BinaryWriter> GetRcfWriter(int i, FieldType type, long dr, DataDictionary dict, List<IRcfWriter> rcfFinalizers)
		{
			var baseWriter = GetWriter(i, type, dr, dict);
			return type.Nullable ? GetNullableRcfWriter(baseWriter!, type, rcfFinalizers) : GetPlainRcfWriter(baseWriter, type, rcfFinalizers);
		}

		private static Action<object, BinaryWriter> GetPlainRcfWriter(Action<object, BinaryWriter> baseWriter, FieldType type, List<IRcfWriter> rcfFinalizers)
		{
			switch (type.Type) {
				case TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext or TypeTag.Xml:
					var result = new RcfWriter<string>(baseWriter);
					rcfFinalizers.Add(result);
					return result.Write;
				case TypeTag.Json:
					result = new RcfWriter<string>(baseWriter);
					rcfFinalizers.Add(result);
					return (o, b) => result.Write(o.ToString()!, b);
				case TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob:
					var result2 = new RcfWriter<byte[]>(baseWriter);
					rcfFinalizers.Add(result2);
					return result2.Write;
				case TypeTag.Boolean:
					var result3 = new RcfWriter<bool>(baseWriter);
					rcfFinalizers.Add(result3);
					return result3.Write;
				case TypeTag.Byte:
					var result4 = new RcfWriter<byte>(baseWriter);
					rcfFinalizers.Add(result4);
					return result4.Write;
				case TypeTag.Short:
					var result5 = new RcfWriter<short>(baseWriter);
					rcfFinalizers.Add(result5);
					return result5.Write;
				case TypeTag.Int:
					var result6 = new RcfWriter<int>(baseWriter);
					rcfFinalizers.Add(result6);
					return result6.Write;
				case TypeTag.Long:
					var result7 = new RcfWriter<long>(baseWriter);
					rcfFinalizers.Add(result7);
					return result7.Write;
				case TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney:
					var result8 = new RcfWriter<decimal>(baseWriter);
					rcfFinalizers.Add(result8);
					return result8.Write;
				case TypeTag.Single:
					var result9 = new RcfWriter<int>(baseWriter);
					rcfFinalizers.Add(result9);
					return result9.Write;
				case TypeTag.Float or TypeTag.Double:
					var result10 = new RcfWriter<double>(baseWriter);
					rcfFinalizers.Add(result10);
					return result10.Write;
				case TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime:
					var result11 = new RcfWriter<DateTime>(baseWriter);
					rcfFinalizers.Add(result11);
					return result11.Write;
				case TypeTag.DateTimeTZ:
					var result12 = new RcfWriter<DateTimeOffset>(baseWriter);
					rcfFinalizers.Add(result12);
					return result12.Write;
				case TypeTag.Guid:
					var result13 = new RcfWriter<Guid>(baseWriter);
					rcfFinalizers.Add(result13);
					return result13.Write;
				case TypeTag.Time or TypeTag.Interval:
					var result14 = new RcfWriter<TimeSpan>(baseWriter);
					rcfFinalizers.Add(result14);
					return result14.Write;
				default: throw new NotImplementedException();
			}
		}

		private static Action<object, BinaryWriter> GetNullableRcfWriter(Action<object?, BinaryWriter> baseWriter, FieldType type, List<IRcfWriter> rcfFinalizers)
		{
			switch (type.Type) {
				case TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext or TypeTag.Xml:
					var result = new NullableRcfWriter<string>(baseWriter);
					rcfFinalizers.Add(result);
					return result.Write;
				case TypeTag.Json:
					result = new NullableRcfWriter<string>(baseWriter);
					rcfFinalizers.Add(result);
					return (o, b) => result.Write(o.ToString()!, b);
				case TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob:
					var result2 = new NullableRcfWriter<byte[]>(baseWriter);
					rcfFinalizers.Add(result2);
					return result2.Write;
				case TypeTag.Boolean:
					var result3 = new NullableRcfWriter<bool>(baseWriter);
					rcfFinalizers.Add(result3);
					return result3.Write;
				case TypeTag.Byte:
					var result4 = new NullableRcfWriter<byte>(baseWriter);
					rcfFinalizers.Add(result4);
					return result4.Write;
				case TypeTag.Short:
					var result5 = new NullableRcfWriter<short>(baseWriter);
					rcfFinalizers.Add(result5);
					return result5.Write;
				case TypeTag.Int:
					var result6 = new NullableRcfWriter<int>(baseWriter);
					rcfFinalizers.Add(result6);
					return result6.Write;
				case TypeTag.Long:
					var result7 = new NullableRcfWriter<long>(baseWriter);
					rcfFinalizers.Add(result7);
					return result7.Write;
				case TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney:
					var result8 = new NullableRcfWriter<decimal>(baseWriter);
					rcfFinalizers.Add(result8);
					return result8.Write;
				case TypeTag.Single:
					var result9 = new NullableRcfWriter<int>(baseWriter);
					rcfFinalizers.Add(result9);
					return result9.Write;
				case TypeTag.Float or TypeTag.Double:
					var result10 = new NullableRcfWriter<double>(baseWriter);
					rcfFinalizers.Add(result10);
					return result10.Write;
				case TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime:
					var result11 = new NullableRcfWriter<DateTime>(baseWriter);
					rcfFinalizers.Add(result11);
					return result11.Write;
				case TypeTag.DateTimeTZ:
					var result12 = new NullableRcfWriter<DateTimeOffset>(baseWriter);
					rcfFinalizers.Add(result12);
					return result12.Write;
				case TypeTag.Guid:
					var result13 = new NullableRcfWriter<Guid>(baseWriter);
					rcfFinalizers.Add(result13);
					return result13.Write;
				case TypeTag.Time or TypeTag.Interval:
					var result14 = new NullableRcfWriter<TimeSpan>(baseWriter);
					rcfFinalizers.Add(result14);
					return result14.Write;
				default: throw new NotImplementedException();
			}
		}

		private static Action<object, BinaryWriter> GetWriter(
			int i, FieldType type, long domainReduction, DataDictionary dict)
		{
			Action<object, BinaryWriter> writer = type.Type switch {
				TypeTag.Unstructured => Unimplemented(type, i),
				TypeTag.Custom => GetCustomTypeWriter(i, type, domainReduction, dict),
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
			writer = type.CollectionType switch {
				CollectionType.None => writer,
				CollectionType.Array => MakeArrayWriter(writer),
				_ => throw new NotImplementedException()
			};

			return writer;
		}

		private static Action<object, BinaryWriter> MakeArrayWriter(Action<object, BinaryWriter> writer) => (o, w) =>
			{
				// there exists ambiguity as to whether a nullable array denotes an array that can be null, or an array of
				// nullable values.  Playing it safe here.
				if (o == null || o == DBNull.Value) {
					w.Write(false);
				} else {
					w.Write(true);
					var arr = (Array)o;
					w.Write7BitEncodedInt(arr.Length);
					foreach (var element in arr) {
						writer(element, w);
					}
				}
			};

		private static Action<object, BinaryWriter> GetCustomTypeWriter(
			int i, FieldType type, long domainReduction, DataDictionary dict)
		{
			if (dict.CustomTypes.TryGetValue(type.Info!, out var ft)) {
				return GetWriter(i, ft, domainReduction, dict);
			}
			return Unimplemented(type, i);
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
			=> (o, s) => {
			if (o is null || o == System.DBNull.Value) {
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

		private static uint Crc32(byte[] row, int length) => System.IO.Hashing.Crc32.HashToUInt32(row.AsSpan(0, length));

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
