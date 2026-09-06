using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DuckDB.NET.Data;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.DuckDB;

public class DuckDBSchemaAnalyzer : SqlSchemaAnalyzer
{
	public DuckDBSchemaAnalyzer(string config) : base(new DuckDBConnection(config))
	{ }

	protected override string ColumnsQuery => """
		SELECT
			c.schema_name,
			c.table_name,
			c.column_name,
			c.data_type,
			c.is_nullable
		FROM duckdb_columns() c
		JOIN duckdb_tables() t
			ON c.table_oid = t.table_oid
		WHERE NOT c.internal
			AND NOT t.internal
		ORDER BY c.schema_name, c.table_name, c.column_index;
		""";

	protected override ISqlFormatter Formatter => DuckDBFormatter.Instance;

	protected override (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader)
	{
		var tableName = new StreamDescription(reader.GetString(0), reader.GetString(1));
		var name = reader.GetString(2);
		var type = reader.GetString(3);
		var nullable = reader.GetBoolean(4);
		var column = new FieldDefinition(name, BuildFieldType(type, nullable));
		return (tableName, column);
	}

	private static IFieldType BuildFieldType(string type, bool nullable)
	{
		var field = DuckDbTypeParser.Parse(type);
		if (nullable) {
			field = field.MakeNull();
		}
		return field;
	}

	protected override string PkQuery => """
		SELECT
			schema_name,
			table_name,
			column_name
		FROM duckdb_constraints(),
			UNNEST(constraint_column_names) WITH ORDINALITY AS t(column_name, key_position)
		WHERE constraint_type = 'PRIMARY KEY'
			AND NOT internal
		ORDER BY schema_name, table_name, key_position;
		""";

	protected override (StreamDescription table, string column) BuildPkDefintion(IDataReader reader)
		=> (new StreamDescription(reader.GetString(0), reader.GetString(1)), reader.GetString(2));

	protected override FieldDefinition[] AnalyzeCustomTableFields(IDataReader reader)
	{
		throw new NotImplementedException();
	}

	const string TABLE_QUERY = @"SELECT schema_name, table_name FROM duckdb_tables() WHERE NOT internal";

	const string READ_DEPS = @"SELECT distinct
	schema_name,
	table_name,
	referenced_table
FROM duckdb_constraints() c
JOIN duckdb_tables() t on t.table_oid != c.table_oid
WHERE
  constraint_type = 'FOREIGN KEY' AND c.table_name <> c.referenced_table AND NOT t.internal
order by schema_name, table_name";

	protected override async Task<StreamDescription[][]> BuildStreamDependencies()
	{
		var names = new List<StreamDescription>();
		var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
		await foreach (var sd in SqlHelper.ReadValuesAsync(_conn, TABLE_QUERY, r => new StreamDescription(r.GetString(0), r.GetString(1)))) {
			names.Add(sd);
		}
		await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, READ_DEPS, r => KeyValuePair.Create(new StreamDescription(r.GetString(0), r.GetString(1)), new StreamDescription(r.GetString(2), r.GetString(3))))) {
			deps.Add(pair);
		}
		return OrderDeps(names, deps).Reverse().ToArray();
	}

	protected override string GetDistinctCountQuery(string fieldList, string tableName, long threshold)
		=> $"select {fieldList} from (select * from {tableName} limit {threshold}) a;";

	private static class DuckDbTypeParser
	{
		public static IFieldType Parse(string typeString)
		{
			int pos = 0;
			var result = ParseType(typeString, ref pos);
			SkipWhitespace(typeString, ref pos);
			if (pos != typeString.Length)
				throw new FormatException($"Unexpected trailing input at {pos} in '{typeString}': '{typeString[pos..]}'");
			return result;
		}

		private static IFieldType ParseType(string s, ref int pos)
		{
			var baseType = ParseBaseType(s, ref pos);

			// Trailing [] / [N], possibly stacked for multi-dimensional lists/arrays
			while (true) {
				SkipWhitespace(s, ref pos);
				if (Peek(s, pos) != '[') {
					break;
				}

				pos++; // consume '['
				SkipWhitespace(s, ref pos);
				if (Peek(s, pos) == ']') {
					pos++; // consume ']'
					baseType = new CollectionField(baseType, CollectionType.Array, false);
				} else {
					int start = pos;
					while (pos < s.Length && char.IsDigit(s[pos])) pos++;
					var size = int.Parse(s[start..pos]);
					SkipWhitespace(s, ref pos);
					Expect(s, ref pos, ']');
					baseType = new CollectionField(baseType, CollectionType.Array, false, size);
				}
			}
			return baseType;
		}

		private static IFieldType ParseBaseType(string s, ref int pos)
		{
			var word = ReadIdentifier(s, ref pos);
			var upper = word.ToUpperInvariant();

			if (upper == "STRUCT") {
				Expect(s, ref pos, '(');
				var fields = ParseFieldList(s, ref pos);
				Expect(s, ref pos, ')');
				return new TupleField(null, fields, false);
			}
			if (upper == "UNION") {
				Expect(s, ref pos, '(');
				var fields = ParseFieldList(s, ref pos);
				Expect(s, ref pos, ')');
				throw new Exception("Union data types are not currently supported by Pansynchro");
			}
			if (upper == "MAP") {
				Expect(s, ref pos, '(');
				var key = ParseType(s, ref pos);
				SkipWhitespace(s, ref pos);
				Expect(s, ref pos, ',');
				var value = ParseType(s, ref pos);
				SkipWhitespace(s, ref pos);
				Expect(s, ref pos, ')');
				throw new Exception("Map data types are not currently supported by Pansynchro");
			}

			// Scalar: may have trailing multi-word continuation (TIMESTAMP WITH TIME ZONE)
			// or a parameter list (DECIMAL(18,3)) - never both in practice.
			var raw = word;
			var args = "";
			SkipWhitespace(s, ref pos);
			while (pos < s.Length && char.IsLetter(s[pos])) {
				raw += " " + ReadIdentifier(s, ref pos);
				SkipWhitespace(s, ref pos);
			}
			if (Peek(s, pos) == '(') {
				args = ReadTypeParams(s, ref pos);
			}
			return BuildBasicType(raw.ToUpperInvariant(), args);
		}

		private static BasicField BuildBasicType(string raw, string args)
		{
			// Based on types documented at https://duckdb.org/docs/lts/sql/data_types/overview
			TypeTag type = raw switch {
				"BIGINT" or "INT8" or "LONG" => TypeTag.Long,
				"BIT" or "BITSTRING" => TypeTag.Bits,
				"BLOB" or "BYTEA" or "BINARY" or "VARBINARY" => TypeTag.Blob,
				"BOOLEAN" or "BOOL" or "LOGICAL" => TypeTag.Boolean,
				"DATE" => TypeTag.Date,
				"DECIMAL" or "NUMERIC" => TypeTag.Decimal,
				"DOUBLE" or "FLOAT8" => TypeTag.Double,
				"FLOAT" or "FLOAT4" or "REAL" => TypeTag.Single,
				"HUGEINT" => TypeTag.Int128,
				"INTEGER" or "INT4" or "INT" or "SIGNED" => TypeTag.Int,
				"INTERVAL" => TypeTag.Interval,
				"JSON" => TypeTag.Json,
				"SMALLINT" or "INT2" or "SHORT" => TypeTag.Short,
				"TIME" => TypeTag.Time,
				"TIMESTAMP WITH TIME ZONE" or "TIMESTAMPTZ" => TypeTag.DateTimeTZ,
				"TIMESTAMP" or "DATETIME" => TypeTag.DateTime,
				"TINYIINT" or "INT1" => TypeTag.SByte,
				"UBIGINT" => TypeTag.ULong,
				"UINTEGER" => TypeTag.UInt,
				"USMALLINT" => TypeTag.UShort,
				"UTINYINT" => TypeTag.Byte,
				"UUID" => TypeTag.Guid,
				"VARCHAR" or "CHAR" or "BPCHAR" or "TEXT" or "STRING" => TypeTag.Text,
				_ => throw new NotImplementedException($"Unknown data type: '{raw}'")
			};
			return new BasicField(type, false, string.IsNullOrEmpty(args) ? null : args, type == TypeTag.Guid);
		}

		private static KeyValuePair<string, IFieldType>[] ParseFieldList(string s, ref int pos)
		{
			var fields = new List<KeyValuePair<string, IFieldType>>();
			SkipWhitespace(s, ref pos);
			if (Peek(s, pos) != ')') {
				while (true) {
					var name = ReadFieldName(s, ref pos);
					SkipWhitespace(s, ref pos);
					var type = ParseType(s, ref pos);
					fields.Add(KeyValuePair.Create(name, type));
					SkipWhitespace(s, ref pos);
					if (Peek(s, pos) == ',') { 
						++pos;
						SkipWhitespace(s, ref pos);
						continue; 
					}
					break;
				}
			}
			return fields.ToArray();
		}

		private static string ReadFieldName(string s, ref int pos)
		{
			SkipWhitespace(s, ref pos);
			if (Peek(s, pos) != '"') {
				return ReadIdentifier(s, ref pos);
			}

			++pos; // opening quote
			var sb = new StringBuilder();
			while (true) {
				if (pos >= s.Length) {
					throw new FormatException("Unterminated quoted identifier");
				}
				char c = s[pos];
				++pos;
				if (c == '"') {
					if (Peek(s, pos) == '"') {
						sb.Append('"');
						++pos; 
						continue; 
					} // escaped ""
					break;
				}
				sb.Append(c);
			}
			return sb.ToString();
		}

		private static string ReadIdentifier(string s, ref int pos)
		{
			SkipWhitespace(s, ref pos);
			int start = pos;
			while (pos < s.Length && (char.IsLetterOrDigit(s[pos]) || s[pos] == '_')) {
				++pos;
			}
			if (pos == start) {
				throw new FormatException($"Expected identifier at {pos} in '{s}'");
			}
			return s[start..pos];
		}

		private static string ReadTypeParams(string s, ref int pos)
		{
			var start = pos;
			Expect(s, ref pos, '(');
			while (s[pos] != ')') {
				if (pos >= s.Length) {
					throw new FormatException("Unclosed parens in type string");
				}
				if (s[pos] == '(') {
					throw new FormatException("Recursive parens in type string");
				}
				++pos;
			}
			var result = s[start..pos];
			++pos;
			return result[1..^1].Trim();
		}

		private static char Peek(string s, int pos) => pos < s.Length ? s[pos] : '\0';

		private static void Expect(string s, ref int pos, char c)
		{
			SkipWhitespace(s, ref pos);
			if (Peek(s, pos) != c) {
				throw new FormatException($"Expected '{c}' at {pos} in '{s}', got '{Peek(s, pos)}'");
			}
			++pos;
		}

		private static void SkipWhitespace(string s, ref int pos)
		{
			while (pos < s.Length && char.IsWhiteSpace(s[pos])) {
				++pos;
			}
		}
	}
}
