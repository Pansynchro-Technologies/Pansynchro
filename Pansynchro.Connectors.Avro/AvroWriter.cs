using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Avro;
using Avro.File;
using Avro.Generic;
using Newtonsoft.Json.Linq;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;

namespace Pansynchro.Connectors.Avro
{
	public class AvroWriter : IWriter, ISinkConnector
	{
		private IDataSink? _sink;

		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			EventLog.Instance.AddStartSyncEvent();
			if (_sink == null) {
				throw new DataException("Must call SetDataSink before calling Sync");
			}
			await foreach (var (name, _, reader) in streams) {
				EventLog.Instance.AddStartSyncStreamEvent(name);
				try {
					using var stream = await _sink.WriteData(name.ToString());
					var schema = BuildAvroSchema(dest.GetStream(name.ToString()));
					using var writer = DataFileWriter<GenericRecord>.OpenWriter(new GenericDatumWriter<GenericRecord>(schema), stream, Codec.CreateCodec(Codec.Type.Deflate), false);
					WriteReader(reader, writer, stream, schema);
				} catch (Exception ex) {
					EventLog.Instance.AddErrorEvent(ex, name);
					if (!ErrorManager.ContinueOnError)
						throw;
				} finally {
					reader.Dispose();
				}
				EventLog.Instance.AddEndSyncStreamEvent(name);
			}
			EventLog.Instance.AddEndSyncEvent();
		}

		private static readonly long AVRO_EPOCH = new DateTime(1970, 1, 1, 0, 0, 0).Ticks;
		private const long MICRO_RATIO = 10;

		private static void WriteReader(IDataReader reader, IFileWriter<GenericRecord> writer, Stream stream, RecordSchema schema)
		{
			var rec = new GenericRecord(schema);
			var len = reader.FieldCount;
			var buffer = new object[len];
			var names = Enumerable.Range(0, len).Select(i => reader.GetName(i)).ToArray();
			while (reader.Read()) {
				reader.GetValues(buffer);
				for (int i = 0; i < len; ++i) {
					var value = buffer[i];
					rec.Add(i, value == DBNull.Value ? null : value);
				}
				writer.Append(rec);
			}
		}

		private static RecordSchema BuildAvroSchema(StreamDefinition stream)
		{
			var obj = new JObject();
			obj["type"] = "record";
			obj["name"] = stream.Name.Name;
			if (stream.Name.Namespace != null) {
				obj["namespace"] = stream.Name.Namespace;
			}
			var fields = BuildAvroFieldList(stream);
			obj["fields"] = fields;
			return (RecordSchema)Schema.Parse(obj.ToString());
		}

		private static JArray BuildAvroFieldList(StreamDefinition stream)
		{
			var result = new JArray();
			foreach (var field in stream.Fields) {
				result.Add(BuildAvroField(field));
			}
			return result;
		}

		private static JObject BuildAvroField(FieldDefinition field)
		{
			var name = field.Name;
			var type = BuildAvroType(field.Type);
			var result = new JObject();
			result["name"] = name;
			result["type"] = type;
			return result;
		}

		private static JToken BuildAvroType(FieldType type)
		{
			JToken result = type.Type switch {
				TypeTag.Boolean => "boolean",
				TypeTag.Byte or TypeTag.SByte or TypeTag.Short or TypeTag.UShort or TypeTag.Int => "int",
				TypeTag.UInt or TypeTag.Long => "long",
				TypeTag.Blob or TypeTag.Binary or TypeTag.Varbinary => "bytes",
				TypeTag.Ntext or TypeTag.Text or TypeTag.Varchar or TypeTag.Nvarchar or TypeTag.Char =>
					"string",
				TypeTag.Single or TypeTag.Float => "float",
				TypeTag.Double => "double",
				TypeTag.Guid => JObject.Parse("{\"type\": \"string\", \"logicalType\": \"uuid\"}"),
				TypeTag.DateTimeTZ => JObject.Parse("{\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}"),
				TypeTag.DateTime => JObject.Parse("{\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}"),
				_ => throw new NotSupportedException($"Field data type '{type.Type}' is not supported")
			};
			if (type.Nullable) {
				var arr = new JArray();
				arr.Add("null");
				arr.Add(result);
				result = arr;
			}
			return result;
		}

		public void SetDataSink(IDataSink sink)
		{
			_sink = sink;
		}

		public void Dispose()
		{
			(_sink as IDisposable)?.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
