using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Npgsql;

using Pansynchro.Core;
using Pansynchro.SQL;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;

namespace Pansynchro.Connectors.Postgres
{
	public class PostgresSchemaAnalyzer : SqlSchemaAnalyzer
	{
		public PostgresSchemaAnalyzer(string connectionString) : base(new NpgsqlConnection(connectionString))
		{ }

		protected override ISqlFormatter Formatter => PostgresFormatter.Instance;

		protected override string ColumnsQuery =>
@"SELECT
	n.nspname as schema_name,
	t.relname as table_name,
	a.attname AS colname,
	pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype,
	a.attnotnull
FROM pg_catalog.pg_attribute a
join pg_catalog.pg_class t on t.oid = a.attrelid
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
WHERE a.attnum > 0
	AND NOT a.attisdropped
	and a.attgenerated = ''
	and t.relkind = 'r'
	and nspname not in ('information_schema', 'pg_catalog', 'pansynchro')
	and nspname not like 'pg_toast%'
	and nspname not like 'pg_temp_%'
ORDER BY a.attnum";

		protected override string PkQuery =>
@"SELECT n.nspname, c.relname, a.attname
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid
					 AND a.attnum = ANY(i.indkey)
JOIN   pg_class c on c.oid = i.indrelid
join   pg_namespace n on n.oid = c.relnamespace
WHERE  i.indisprimary
	and nspname not in ('information_schema', 'pg_catalog', 'pansynchro')
	and nspname not like 'pg_toast%'
	and nspname not like 'pg_temp_%'";

		const string CUSTOM_TYPE_QUERY =
@"SELECT n.nspname AS schema
	 , t.typname AS name
	 , t.typtype
	 , pg_catalog.format_type(t.typbasetype, t.typtypmod) AS type
	 , not t.typnotnull AS nullable
	 --, t.typdefault AS default
FROM   pg_catalog.pg_type t
LEFT   JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE  t.typtype = 'd'  -- domains
AND    n.nspname <> 'pg_catalog'
AND    n.nspname <> 'information_schema'
AND    pg_catalog.pg_type_is_visible(t.oid)";


		protected override async Task<Dictionary<string, IFieldType>> LoadCustomTypes()
		{
			await foreach (var type in SqlHelper.ReadValuesAsync(_conn, CUSTOM_TYPE_QUERY, ReadCustomType)) {
				_customTypes.Add(type.Name, type.Type);
			}
			return _customTypes;
		}

		private FieldDefinition ReadCustomType(IDataReader input)
		{
			var name = $"{input.GetString(1)}";
			var type = GetColumnType(input);
			return new FieldDefinition(name, type);
		}

		protected override (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader)
		{
			var table = new StreamDescription(reader.GetString(0), reader.GetString(1));
			var name = reader.IsDBNull(2) ? "" : reader.GetString(2);
			var type = GetColumnType(reader);
			var column = new FieldDefinition(name, type);

			return (table, column);
		}

		protected override (StreamDescription table, string column) BuildPkDefintion(IDataReader reader)
		{
			return (new StreamDescription(reader.GetString(0), reader.GetString(1)), reader.GetString(2));
		}

		private readonly Dictionary<string, IFieldType> _customTypes = new();

		private IFieldType GetColumnType(IDataReader reader)
		{
			var typeName = reader.GetString(3);
			var formalType = typeName.StartsWith('"');
			if (formalType) {
				if (_customTypes.TryGetValue(typeName[1..^1], out var cTyp)) {
					return cTyp;
				}
			}
			var isArr = typeName.EndsWith("[]");
			if (isArr) {
				typeName = typeName[..^2];
			}
			string? info = null;
			if (typeName.EndsWith(')')) {
				var startPos = typeName.LastIndexOf('(');
				info = typeName[(startPos + 1)..^1];
				typeName = typeName.Substring(0, startPos);
			}
			var type = GetTagType(typeName);
			var nullable = !reader.GetBoolean(4);
			var result = new BasicField(type, nullable, info, false);
			return isArr ? new CollectionField(result.MakeNull(), CollectionType.Array, false) : result;
		}

		private static readonly Dictionary<string, TypeTag> TYPE_MAP = new()
		{
			{ "bigint", TypeTag.Long },
			{ "bigserial", TypeTag.Long },
			{ "bit", TypeTag.Bits },
			{ "bit varying", TypeTag.Bits },
			{ "bool", TypeTag.Boolean },
			{ "boolean", TypeTag.Boolean },
			{ "bytea", TypeTag.Varbinary },
			{ "char", TypeTag.Nchar },
			{ "character", TypeTag.Nchar },
			{ "character varying", TypeTag.Nvarchar },
			{ "date", TypeTag.Date },
			{ "double precision", TypeTag.Double },
			{ "decimal", TypeTag.Decimal },
			{ "float4", TypeTag.Single },
			{ "float8", TypeTag.Double },
			{ "int", TypeTag.Int },
			{ "int2", TypeTag.Short },
			{ "int4", TypeTag.Int },
			{ "integer", TypeTag.Int },
			{ "json", TypeTag.Json},
			{ "jsonb", TypeTag.Json },
			{ "money", TypeTag.Money },
			{ "numeric", TypeTag.Numeric },
			{ "real", TypeTag.Single },
			{ "serial", TypeTag.Int },
			{ "serial2", TypeTag.Short },
			{ "serial4", TypeTag.Int },
			{ "smallint", TypeTag.Short },
			{ "smallserial", TypeTag.Short },
			{ "text", TypeTag.Ntext },
			{ "time", TypeTag.Time },
			{ "time without time zone", TypeTag.Time },
			{ "time with time zone", TypeTag.TimeTZ },
			{ "timetz", TypeTag.TimeTZ },
			{ "timestamp without time zone", TypeTag.DateTime },
			{ "timestamp with time zone", TypeTag.DateTimeTZ },
			{ "timestamptz", TypeTag.DateTimeTZ },
			{ "uuid", TypeTag.Guid },
			{ "xml", TypeTag.Xml }
		};

		private static TypeTag GetTagType(string v)
		{
			if (TYPE_MAP.TryGetValue(v, out var result)) {
				return result;
			}
			throw new ArgumentException($"Unknown SQL data type '{v}'.");
		}

		const string READ_DEPS =
@"SELECT distinct
  (SELECT nspname FROM pg_namespace WHERE oid=f.relnamespace) AS dependency_schema,
  f.relname AS dependency_table,
  (SELECT nspname FROM pg_namespace WHERE oid=m.relnamespace) AS dependent_schema,
  m.relname AS dependent_table
FROM
  pg_constraint o
LEFT JOIN pg_class f ON f.oid = o.confrelid
LEFT JOIN pg_class m ON m.oid = o.conrelid
WHERE
  o.contype = 'f' AND o.conrelid IN (SELECT oid FROM pg_class c WHERE c.relkind = 'r') and o.conrelid <> o.confrelid
order by f.relname";

		const string TABLE_QUERY =
@"select table_schema, table_name
from information_schema.tables
where table_type = 'BASE TABLE' and table_schema !~ 'pg_' and table_schema != 'information_schema' and table_schema != 'pansynchro'";

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

		protected override FieldDefinition[] AnalyzeCustomTableFields(IDataReader reader)
		{
			var columnschema = ((NpgsqlDataReader)reader).GetColumnSchemaAsync().GetAwaiter().GetResult();
			var table = reader.GetSchemaTable()!;
			var columns = table.Select();
			var fields = columns.Select(BuildFieldDef).ToArray();
			return fields;
		}

		private FieldDefinition BuildFieldDef(DataRow row)
		{
			var name = (string)row["ColumnName"];
			return new FieldDefinition(name, BuildFieldType(row));
		}

		private static IFieldType BuildFieldType(DataRow row)
		{
			var typeName = (string)row["DataTypeName"];
			var isArr = typeName.EndsWith("[]");
			if (isArr) {
				typeName = typeName[..^2];
			}
			string? info = null;
			if (typeName.EndsWith(')')) {
				var startPos = typeName.LastIndexOf('(');
				info = typeName[(startPos + 1)..^1];
				typeName = typeName.Substring(0, startPos);
			}
			var type = GetTagType(typeName);
			var nullable = row["AllowDBNull"] is bool b ? b : true;
			var result = new BasicField(type, nullable, info, false);
			return isArr ? new CollectionField(result.MakeNull(), CollectionType.Array, false) : result;
		}
	}
}