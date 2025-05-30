using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;

namespace Pansynchro.Connectors.TextFile.JSON;
public class JsonWriter : IWriter, ISinkConnector
{
	private IDataSink? _sink;

	public JsonWriter(string config)
	{ }

	public void SetDataSink(IDataSink sink) => _sink = sink;

	public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
	{
		if (_sink == null) {
			throw new DataException("Must call SetDataSource before calling ReadFrom");
		}
		EventLog.Instance.AddStartSyncEvent();
		await foreach (var (name, settings, stream) in streams) {
			try {
				var defn = dest.GetStream(name, NameStrategy.Get(NameStrategyType.Identity));
				using var tw = await _sink.WriteText(name.ToString());
				Write(stream, tw, defn);
			} catch (Exception ex) {
				EventLog.Instance.AddErrorEvent(ex, name);
				if (!ErrorManager.ContinueOnError)
					throw;
			} finally {
				stream.Dispose();
			}
		}
		EventLog.Instance.AddEndSyncEvent();
	}

	private static readonly JsonSerializerOptions OPTIONS = new JsonSerializerOptions() {
		WriteIndented = true,
	};

	private static void Write(IDataReader reader, TextWriter tw, StreamDefinition defn)
	{
		var writer = JsonWriter.BuildJsonWriter(defn, reader);
		var arr = new JsonArray();
		while (reader.Read()) {
			arr.Add(writer(reader));
		}
		tw.Write(arr.ToJsonString(OPTIONS));
	}

	private static Func<IDataReader, JsonObject> BuildJsonWriter(StreamDefinition defn, IDataReader reader)
	{
		var list = new List<Action<IDataReader, JsonObject>>();
		for (int i = 0; i < reader.FieldCount; ++i) {
			var fieldName = reader.GetName(i);
			var field = defn.Fields.FirstOrDefault(f => f.Name == fieldName);
			if (field != null) {
				Action<IDataReader, JsonObject> propWriter = BuildPropWriter(fieldName, i, field.Type);
				list.Add(propWriter);
			}
		}
		return r => {
			var result = new JsonObject();
			foreach (var writer in list) {
				writer(r, result);
			}
			return result;
		};
	}

	private static Action<IDataReader, JsonObject> BuildPropWriter(string fieldName, int i, IFieldType type)
	{
		var builder = new PropWriterBuilder(fieldName, i);
		return builder.Visit(type);
	}

	private class PropWriterBuilder(string fieldName, int index) : IFieldTypeVisitor<Action<IDataReader, JsonObject>>
	{
		public Action<IDataReader, JsonObject> Visit(IFieldType type)
		{
			var result = type.Accept(this);
			if (type.Nullable) {
				result = (r, o) => {
					if (r.IsDBNull(index)) {
						result(r, o);
					}
				};
			}
			return result;
		}

		public Action<IDataReader, JsonObject> VisitBasicField(BasicField type) => type.Type switch {
			TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext =>
				(r, o) => o.Add(fieldName, r.GetString(index)),
			TypeTag.Boolean => (r, o) => o.Add(fieldName, r.GetBoolean(index)),
			TypeTag.Byte => (r, o) => o.Add(fieldName, r.GetByte(index)),
			TypeTag.Short => (r, o) => o.Add(fieldName, r.GetInt16(index)),
			TypeTag.Int => (r, o) => o.Add(fieldName, r.GetInt32(index)),
			TypeTag.Long => (r, o) => o.Add(fieldName, r.GetInt64(index)),
			TypeTag.UShort => (r, o) => o.Add(fieldName, (ushort)r.GetInt16(index)),
			TypeTag.UInt => (r, o) => o.Add(fieldName, (uint)r.GetInt32(index)),
			TypeTag.ULong => (r, o) => o.Add(fieldName, (ulong)r.GetInt64(index)),
			TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney
				=> (r, o) => o.Add(fieldName, r.GetDecimal(index)),
			TypeTag.Float or TypeTag.Single => (r, o) => o.Add(fieldName, r.GetFloat(index)),
			TypeTag.Double => (r, o) => o.Add(fieldName, r.GetDouble(index)),
			TypeTag.Date or TypeTag.Time or TypeTag.DateTime or TypeTag.SmallDateTime or TypeTag.TimeTZ or TypeTag.DateTimeTZ
				=> (r, o) => o.Add(fieldName, r.GetDateTime(index)),
			TypeTag.Guid => (r, o) => o.Add(fieldName, r.GetGuid(index)),
			TypeTag.Json => (r, o) => o.Add(fieldName, JsonNode.Parse(r.GetString(index))),
			_ => throw new NotImplementedException($"No JSON extractor implemented for '{type.Type}'.")
		};

		public Action<IDataReader, JsonObject> VisitCollection(CollectionField type) => throw new NotImplementedException();

		public Action<IDataReader, JsonObject> VisitCustomField(CustomField type) => throw new NotImplementedException();

		public Action<IDataReader, JsonObject> VisitTupleField(TupleField type) => throw new NotImplementedException();
	}

	public void Dispose()
	{
		(_sink as IDisposable)?.Dispose();
		GC.SuppressFinalize(this);
	}
}
