using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using DuckDB.NET.Data;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.Core.EventsSystem;
using Pansynchro.Core.Helpers;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.DuckDB;

internal class DuckDBWriter : SqlDbWriter
{
	public DuckDBWriter(string connectionString) : base(new DuckDBConnection(connectionString))
	{ }

	protected override ISqlFormatter Formatter => DuckDBFormatter.Instance;

	protected override void FullStreamSync(StreamDescription name, StreamSettings settings, IDataReader reader)
	{
		StreamDefinition stream;
		try {
			stream = Dict.GetStream(name, NameStrategy.Get(NameStrategyType.LowerCase));
		} catch (KeyNotFoundException) {
			throw new Exception($"No table named '{name}' exists in the DuckDB destination.");
		}
		var writer = GetWriter(reader, stream);
		using var appender = ((DuckDBConnection)_conn).CreateAppender(stream.ToString());
		while (reader.Read()) {
			appender.AppendRow(reader, writer);
		}
	}

	private class WriterBuilder : IFieldTypeVisitor<Action<IDuckDBAppenderRow, IDataReader>>
	{
		public int Column { get; set; }

		public Action<IDuckDBAppenderRow, IDataReader> Visit(IFieldType type)
		{
			var result = type.Accept(this);
			if (type.Nullable) {
				result = MakeNullable(result, Column);
			}
			return result;
		}

		private static Action<IDuckDBAppenderRow, IDataReader> MakeNullable(Action<IDuckDBAppenderRow, IDataReader> result, int column)
			=> (row, reader) => {
				if (reader.IsDBNull(column)) {
					row.AppendNullValue();
				} else {
					result(row, reader);
				}
			};

		public Action<IDuckDBAppenderRow, IDataReader> VisitBasicField(BasicField type) => type.Type switch {
			TypeTag.Unstructured => Unimplemented(type),
			TypeTag.Char or TypeTag.Varchar or TypeTag.Text or TypeTag.Nchar or TypeTag.Nvarchar or TypeTag.Ntext
				=> MakeStringWriter(Column),
			TypeTag.Json => MakeJsonWriter(Column),
			TypeTag.Binary or TypeTag.Varbinary or TypeTag.Blob => MakeBytesWriter(Column),
			TypeTag.Boolean => MakeBoolWriter(Column),
			TypeTag.Byte => MakeByteWriter(Column),
			TypeTag.Short => MakeShortWriter(Column),
			TypeTag.Int => MakeIntWriter(Column),
			TypeTag.Long => MakeLongWriter(Column),
			TypeTag.Decimal or TypeTag.Numeric or TypeTag.Money or TypeTag.SmallMoney => MakeDecimalWriter(Column),
			TypeTag.Single => MakeSingleWriter(Column),
			TypeTag.Float or TypeTag.Double => MakeDoubleWriter(Column),
			TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime => MakeDateTimeWriter(Column),
			TypeTag.DateTimeTZ => MakeDateTimeTZWriter(Column),
			TypeTag.Guid => MakeGuidWriter(Column),
			TypeTag.Time or TypeTag.Interval => MakeTimeSpanWriter(Column),
			_ => Unimplemented(type)
		};

		private static Action<IDuckDBAppenderRow, IDataReader> MakeTimeSpanWriter(int column)
			=> (row, reader) => row.AppendValue((TimeSpan)reader.GetValue(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeGuidWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetGuid(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeDateTimeTZWriter(int column)
			=> (row, reader) => row.AppendValue((DateTimeOffset)reader.GetValue(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeDateTimeWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetDateTime(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeDoubleWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetDouble(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeSingleWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetFloat(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeDecimalWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetDecimal(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeLongWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetInt64(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeIntWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetInt32(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeShortWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetInt16(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeByteWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetByte(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeBoolWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetBoolean(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeBytesWriter(int column)
			=> (row, reader) => row.AppendValue((byte[])reader.GetValue(column));

		private static Action<IDuckDBAppenderRow, IDataReader> MakeJsonWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetValue(column).ToString());

		private static Action<IDuckDBAppenderRow, IDataReader> MakeStringWriter(int column)
			=> (row, reader) => row.AppendValue(reader.GetString(column));

		public Action<IDuckDBAppenderRow, IDataReader> VisitCollection(CollectionField type)
		{
			if (type is { BaseType: BasicField b, CollectionType: CollectionType.Array }) {
				return MakeArrayWriter(b, Column);
			} else {
				throw new NotImplementedException();
			}
		}

		private Action<IDuckDBAppenderRow, IDataReader> MakeArrayWriter(BasicField field, int column)
		{
			var type = TypesHelper.TypeTagToDotNetType(field.Type);
			if (field.Nullable && type.IsValueType) {
				type = typeof(System.Nullable<>).MakeGenericType(type);
			}
			return (Action<IDuckDBAppenderRow, IDataReader>)GetType()
				.GetMethod("BuildArrayWriter", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
				.MakeGenericMethod(type)
				.Invoke(null, [column])!;
		}

		private static Action<IDuckDBAppenderRow, IDataReader> BuildArrayWriter<T>(int column)
			=> (row, reader) => row.AppendValue<T>((IEnumerable<T>)reader.GetValue(column));

		public Action<IDuckDBAppenderRow, IDataReader> VisitCustomField(CustomField type)
		{
			throw new NotImplementedException();
		}

		public Action<IDuckDBAppenderRow, IDataReader> VisitTupleField(TupleField type)
		{
			throw new NotImplementedException();
		}

		private static Action<IDuckDBAppenderRow, IDataReader> Unimplemented(IFieldType type)
		{
			throw new NotImplementedException($"No writer implemented for '{type}'.");
		}

	}

	private static Action<IDuckDBAppenderRow, IDataReader> GetWriter(IDataReader source, StreamDefinition dest)
	{
		var builder = new WriterBuilder();
		var names = Enumerable.Range(0, source.FieldCount)
			.Select(i => KeyValuePair.Create(source.GetName(i), (i, source.GetFieldType(i))))
			.ToDictionary();
		var actions = new Action<IDuckDBAppenderRow, IDataReader>[dest.Fields.Length];
		for (int i = 0; i < dest.Fields.Length; i++) {
			var field = dest.Fields[i];
			if (names.TryGetValue(field.Name, out var incoming)) {
				var (idx, inType) = incoming;
				if (!TypesMatch(field.Type, inType)) {
					throw new Exception($"Incoming field '{field.Name}' is typed as '{inType}', but DuckDB table expects {TypesHelper.FieldTypeToCSharpType(field.Type)}.");
				}
				builder.Column = idx;
				actions[i] = builder.Visit(field.Type);
			} else {
				if (field.Type.Nullable) {
					Pansynchro.Core.EventsSystem.EventLog.Instance.AddWarningEvent($"No match for field '{field.Name}' found in incoming data. Inserting null values.");
					actions[i] = static (row, _) => row.AppendNullValue();
				} else {
					throw new Exception($"No match for non-nullable field '{field.Name}' found in incoming data.");
				}
			}
		}
		return (row, reader) => {
			foreach (var action in actions) {
				action(row, reader);
			}
		};
	}

	private static bool TypesMatch(IFieldType type, Type inType)
	{
		var outType = TypesHelper.FieldTypeToDotNetType(type);
		if (inType.Equals(outType)) {
			return true;
		}
		var ut = Nullable.GetUnderlyingType(outType);
		if (ut != null && ut.Equals(inType) && !ut.IsArray) {
			return true;
		}
		return false;
	}
}
