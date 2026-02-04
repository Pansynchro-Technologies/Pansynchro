using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.CustomTypes;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.Core.EventsSystem;
using Pansynchro.Core.Streams;

namespace Pansynchro.Protocol
{
	public class BinaryDecoder : IReader
	{
		private readonly TcpClient? _client;
		private readonly MeteredStream _meter;
		private readonly BrotliStream _decompressor;
		private readonly BinaryReader _reader;
		private DataDictionary? _sourceDict;

		private const int VERSION = 6;

		public BinaryDecoder(TcpClient client, DataDictionary? sourceDict) : this(client.GetStream(), sourceDict)
		{
			_client = client;
		}

		public BinaryDecoder(Stream source, DataDictionary? sourceDict)
		{
			_meter = new MeteredStream(source);
			_decompressor = new BrotliStream(_meter, CompressionMode.Decompress);
			_reader = new BinaryReader(_decompressor, Encoding.UTF8);
			_sourceDict = sourceDict;
		}

		public IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
		{
			if (_sourceDict == null) {
				_sourceDict = source;
			} else if (!_sourceDict.Equals(source)) {
				throw new InvalidDataException("Data dictionary mismatch");
			}
			ReadVersion();
			ReadDictHash();
			return ReadStreams(source.Streams.Length, source);
		}

		private void ReadVersion()
		{
			CheckEqual(_reader.ReadString(), "PANSYNCHRO", "Protocol validation check failed");
			CheckEqual(_reader.ReadByte(), (byte)Markers.Version, "Protocol version marker missing");
			CheckEqual(_reader.Read7BitEncodedInt(), VERSION, "Protocol version check failed");
		}

		private void ReadDictHash()
		{
			CheckEqual(_reader.ReadByte(), (byte)Markers.Schema, "Data dictionary not found");
			CheckEqual(_reader.ReadString(), _sourceDict!.Name, "Data dictionary name check failed");
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
			if (value != expected) {
				throw new InvalidDataException($"{errorMsg}.  Expected {expected} but read {value}.");
			}
		}

		private static void CheckEqual(string value, string expected, string errorMsg)
		{
			if (!value.Equals(expected, StringComparison.Ordinal)) {
				throw new InvalidDataException($"{errorMsg}.  Expected {expected} but read {value}.");
			}
		}

		private static void CheckEqual<T>(T[] value, T[] expected, string errorMsg)
		{
			if (!value.SequenceEqual(expected)) {
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
			EventLog.Instance.AddInformationEvent($"Bytes read: {_meter.TotalBytesRead.ToString("N0")}");
			await Task.CompletedTask; // just to shut the compiler up
		}

		internal static (Func<BinaryReader, object>[], IRcfReader[]) BuildBufferDecoders(StreamDefinition schema, DataDictionary dict)
		{
			var len = schema.Fields.Length;
			var result = new Func<BinaryReader, object>[schema.Fields.Length];
			var rcfReaders = new List<IRcfReader>();
			var drs = new Dictionary<string, long>(schema.DomainReductions);
			for (int i = 0; i < len; ++i) {
				var field = schema.Fields[i];
				var type = field.Type;
				drs.TryGetValue(field.Name, out var dr);
				var isRcf = schema.RareChangeFields.Contains(field.Name);
				// for the moment, arrays will not be treated as RCFs, in the interest of ease of implementation.
				// This may change if anyone comes up with a valid use case
				result[i] = (isRcf && type is BasicField bf)
					? GetRcfReader(bf, dr, dict, rcfReaders)
					: GetReader(type, dr, dict);
			}
			return (result, rcfReaders.ToArray());
		}

		private static Func<BinaryReader, object> GetRcfReader(BasicField type, long dr, DataDictionary dict, List<IRcfReader> rcfReaders)
		{
			var baseReader = GetReader(type, dr, dict);
			return type.Nullable ? GetNullableRcfReader(baseReader!, type, rcfReaders) : GetPlainRcfReader(baseReader, type, rcfReaders);
		}

		private static Func<BinaryReader, object> GetPlainRcfReader(Func<BinaryReader, object> baseReader, BasicField type, List<IRcfReader> rcfReaders)
		{
			switch (type.Type) {
				case TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext or TypeTag.Xml:
					var result = new RcfReader<string>(baseReader);
					rcfReaders.Add(result);
					return result.Read;
				case TypeTag.Json:
					result = new RcfReader<string>(baseReader);
					rcfReaders.Add(result);
					return b => JsonNode.Parse((string)result.Read(b))!;
				case TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob:
					var result2 = new RcfReader<byte[]>(baseReader);
					rcfReaders.Add(result2);
					return result2.Read;
				case TypeTag.Boolean:
					var result3 = new RcfReader<bool>(baseReader);
					rcfReaders.Add(result3);
					return result3.Read;
				case TypeTag.Byte:
					var result4 = new RcfReader<byte>(baseReader);
					rcfReaders.Add(result4);
					return result4.Read;
				case TypeTag.Short:
					var result5 = new RcfReader<short>(baseReader);
					rcfReaders.Add(result5);
					return result5.Read;
				case TypeTag.Int:
					var result6 = new RcfReader<int>(baseReader);
					rcfReaders.Add(result6);
					return result6.Read;
				case TypeTag.Long:
					var result7 = new RcfReader<long>(baseReader);
					rcfReaders.Add(result7);
					return result7.Read;
				case TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney:
					var result8 = new RcfReader<decimal>(baseReader);
					rcfReaders.Add(result8);
					return result8.Read;
				case TypeTag.Single:
					var result9 = new RcfReader<int>(baseReader);
					rcfReaders.Add(result9);
					return result9.Read;
				case TypeTag.Float or TypeTag.Double:
					var result10 = new RcfReader<double>(baseReader);
					rcfReaders.Add(result10);
					return result10.Read;
				case TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime:
					var result11 = new RcfReader<DateTime>(baseReader);
					rcfReaders.Add(result11);
					return result11.Read;
				case TypeTag.DateTimeTZ:
					var result12 = new RcfReader<DateTimeOffset>(baseReader);
					rcfReaders.Add(result12);
					return result12.Read;
				case TypeTag.Guid:
					var result13 = new RcfReader<Guid>(baseReader);
					rcfReaders.Add(result13);
					return result13.Read;
				case TypeTag.Time or TypeTag.Interval:
					var result14 = new RcfReader<TimeSpan>(baseReader);
					rcfReaders.Add(result14);
					return result14.Read;
				default: throw new NotImplementedException();
			}
		}

		private static Func<BinaryReader, object> GetNullableRcfReader(Func<BinaryReader, object> baseReader, BasicField type, List<IRcfReader> rcfReaders)
		{
			switch (type.Type) {
				case TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext or TypeTag.Xml:
					var result = new NullableRcfReader<string>(baseReader);
					rcfReaders.Add(result);
					return result.Read;
				case TypeTag.Json:
					result = new NullableRcfReader<string>(baseReader);
					rcfReaders.Add(result);
					return b => JsonNode.Parse((string)result.Read(b));
				case TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob:
					var result2 = new NullableRcfReader<byte[]>(baseReader);
					rcfReaders.Add(result2);
					return result2.Read;
				case TypeTag.Boolean:
					var result3 = new NullableRcfReader<bool>(baseReader);
					rcfReaders.Add(result3);
					return result3.Read;
				case TypeTag.Byte:
					var result4 = new NullableRcfReader<byte>(baseReader);
					rcfReaders.Add(result4);
					return result4.Read;
				case TypeTag.Short:
					var result5 = new NullableRcfReader<short>(baseReader);
					rcfReaders.Add(result5);
					return result5.Read;
				case TypeTag.Int:
					var result6 = new NullableRcfReader<int>(baseReader);
					rcfReaders.Add(result6);
					return result6.Read;
				case TypeTag.Long:
					var result7 = new NullableRcfReader<long>(baseReader);
					rcfReaders.Add(result7);
					return result7.Read;
				case TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney:
					var result8 = new NullableRcfReader<decimal>(baseReader);
					rcfReaders.Add(result8);
					return result8.Read;
				case TypeTag.Single:
					var result9 = new NullableRcfReader<int>(baseReader);
					rcfReaders.Add(result9);
					return result9.Read;
				case TypeTag.Float or TypeTag.Double:
					var result10 = new NullableRcfReader<double>(baseReader);
					rcfReaders.Add(result10);
					return result10.Read;
				case TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime:
					var result11 = new NullableRcfReader<DateTime>(baseReader);
					rcfReaders.Add(result11);
					return result11.Read;
				case TypeTag.DateTimeTZ:
					var result12 = new NullableRcfReader<DateTimeOffset>(baseReader);
					rcfReaders.Add(result12);
					return result12.Read;
				case TypeTag.Guid:
					var result13 = new NullableRcfReader<Guid>(baseReader);
					rcfReaders.Add(result13);
					return result13.Read;
				case TypeTag.Time or TypeTag.Interval:
					var result14 = new NullableRcfReader<TimeSpan>(baseReader);
					rcfReaders.Add(result14);
					return result14.Read;
				default: throw new NotImplementedException();
			}
		}

		private static Func<BinaryReader, object> GetReader(IFieldType type, long domainReduction, DataDictionary dict) 
			=> new ReaderBuilder(domainReduction, dict).Visit(type);

		private readonly struct ReaderBuilder : IFieldTypeVisitor<Func<BinaryReader, object>>
		{
			private readonly long _domainReduction;
			private readonly DataDictionary _dict;

			public ReaderBuilder(long domainReduction, DataDictionary dict)
			{
				_domainReduction = domainReduction;
				_dict = dict;
			}

			public Func<BinaryReader, object> Visit(IFieldType type)
			{
				var result = type.Accept(this);
				if (type.Nullable) {
					result = MakeNullable(result);
				}
				return result;
			}

			public Func<BinaryReader, object> VisitBasicField(BasicField type) => type.Type switch {
				TypeTag.Unstructured => Unimplemented(type),
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
				TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime => MakeDateTimeReader(_domainReduction),
				TypeTag.DateTimeTZ => MakeDateTimeTZReader(_domainReduction),
				TypeTag.VarDateTime => Unimplemented(type),
				TypeTag.Guid => GuidReader,
				TypeTag.Time or TypeTag.Interval => TimeSpanReader,
				TypeTag.Bits => Unimplemented(type),
				_ => Unimplemented(type)
			};

			public Func<BinaryReader, object> VisitCollection(CollectionField type)
			{
				var reader = Visit(type.BaseType);
				return MakeArrayReader(reader);
			}

			public Func<BinaryReader, object> VisitCustomField(CustomField type) 
				=> GetCustomTypeReader(type, _domainReduction, _dict);

			public Func<BinaryReader, object> VisitTupleField(TupleField type)
			{
				throw new NotImplementedException();
			}
		}

		//Typing this as object[] is inefficient, but hopefully it will work!
		private static Func<BinaryReader, object> MakeArrayReader(Func<BinaryReader, object> reader) => r => {
			// there exists ambiguity as to whether a nullable array denotes an array that can be null, or an array of
			// nullable values.  Playing it safe here.
			var isNull = r.ReadBoolean();
			if (isNull) {
				return DBNull.Value;
			}
			var size = r.Read7BitEncodedInt();
			var result = new object[size];
			for (int i = 0; i < size; ++i) {
				result[i] = reader(r);
			}
			return result;
		};

		private static Func<BinaryReader, object> GetCustomTypeReader(CustomField type, long domainReduction, DataDictionary dict)
		{
			if (dict.CustomTypes.TryGetValue(type.Name, out var ft)) {
				return GetReader(ft, domainReduction, dict);
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

		private static Func<BinaryReader, object> MakeDateTimeReader(long dr) => r => new DateTime(r.Read7BitEncodedInt64() + dr);

		private static Func<BinaryReader, object> MakeDateTimeTZReader(long dr) => r => {
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

		private static object JsonReader(BinaryReader r) => JsonNode.Parse(r.ReadString())!;

		private static Func<BinaryReader, object> Unimplemented(IFieldType type)
		{
			var customType = type is BasicField bf ? CustomTypeRegistry.GetType(bf.Type) : null;
			if (customType == null) {
				throw new NotImplementedException($"No reader implemented for '{type}'.");
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

		internal static IReader Archive(string connectionString)
		{
			var file = File.OpenRead(connectionString);
			return new BinaryDecoder(file, null);
		}
	}
}
