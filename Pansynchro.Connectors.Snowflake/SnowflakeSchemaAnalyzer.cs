using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Tortuga.Data.Snowflake;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.Snowflake
{
    public class SnowflakeSchemaAnalyzer : SqlSchemaAnalyzer
    {
        public SnowflakeSchemaAnalyzer(string connectionString) 
            : base(new SnowflakeDbConnection { ConnectionString = connectionString })
        { }

        protected override string ColumnsQuery =>
@$"select 
   c.TABLE_SCHEMA as Schema,
   c.TABLE_NAME as TableName,
   c.COLUMN_NAME as Name,
   c.IS_NULLABLE as Nullable,
   c.DATA_TYPE as Type,
   c.IS_IDENTITY as Identity,
   c.CHARACTER_MAXIMUM_LENGTH as StringLength,
   c.NUMERIC_PRECISION as Precision,
   c.NUMERIC_SCALE as Scale,
   c.DATETIME_PRECISION as DtPrecision
from INFORMATION_SCHEMA.COLUMNS c
join INFORMATION_SCHEMA.TABLES T 
   on c.TABLE_CATALOG = t.TABLE_CATALOG and c.TABLE_SCHEMA = t.TABLE_SCHEMA and c.TABLE_NAME = t.TABLE_NAME
where c.TABLE_CATALOG = '{DatabaseName}'
   and c.TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
   and t.IS_TRANSIENT = 'NO'
   and t.TABLE_TYPE = 'BASE TABLE'
order by c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;";

        private const string PK_SETUP =
@"create or replace function GET_PK_COLUMNS(DATABASE_DDL string)
returns table (""SCHEMA_NAME"" string, ""TABLE_NAME"" string, PK_COLUMN string)
language javascript
as
$$
{
    processRow: function get_params(row, rowWriter, context)
        {

            var startTableLine = -1;
            var endTableLine = -1;

            var dbDDL = row.DATABASE_DDL.replace(/'[\s\S]*'/gm, '');


        var lines = dbDDL.split(""\n"");
            var currentSchema = """";
            var currentTable = """";

            var ln = 0;
            var tableDDL = """";
            var pkCols = null;
            var c = 0;

            for (var i = 0; i < lines.length; i++)
            {

                if (lines[i].match(/ ^create.* schema /))
                {
                    currentSchema = lines[i].split(""schema"")[1].replace(/;/, '');
            //rowWriter.writeRow({PK_COLUMN: ""currentSchema = "" + currentSchema});
        }
        
        
            if (lines[i].match(/^create or replace TABLE /)) {
                startTableLine = i;
            }
            
            if (startTableLine != -1 && lines[i] == "");"") {
                endTableLine = i;
            }

if (startTableLine != -1 && endTableLine != -1)
{
    // We found a table. Now, join it and send it for parsing
    tableDDL = """";
    for (ln = startTableLine; ln <= endTableLine; ln++)
    {
        if (ln > 0) tableDDL += ""\n"";
        tableDDL += lines[ln];
    }
    startTableLine = -1;
    endTableLine = -1;
    currentTable = getTableName(tableDDL);
    pkCols = getPKs(tableDDL);

    for (c = 0; c < pkCols.length; c++)
    {
        rowWriter.writeRow({ PK_COLUMN: pkCols[c], SCHEMA_NAME: currentSchema, TABLE_NAME: currentTable});
}
            }
        }

        function getTableName(tableDDL)
{
    var lines = tableDDL.split(""\n"");
    var s = lines[1];
    s = s.substring(s.indexOf("" TABLE "") + "" TABLE "".length);
    s = s.split("" ("")[0];
    return s;
}

function getPKs(tableDDL)
{
    var c;
    var keyword = ""primary key"";
    var ins = -1;
    var s = tableDDL.split(""\n"");
    for (var i = 0; i < s.length; i++)
    {
        ins = s[i].indexOf(keyword);
        if (ins != -1)
        {
            var colList = s[i].substring(ins + keyword.length);
            colList = colList.replace(""("", """");
            colList = colList.replace("")"", """");
            var colArray = colList.split("","");
            for (pkc = 0; c < colArray.length; pkc++)
            {
                colArray[pkc] = colArray[pkc].trim();
            }
            return colArray;
        }
    }
    return [];  // No PK
}
    }
}
$$; ";

        protected override string PkQuery => $"select SCHEMA_NAME, TABLE_NAME, PK_COLUMN from table(get_pk_columns(get_ddl('database', '{DatabaseName}')));";

        private readonly Dictionary<StreamDescription, string> _identity = new();

        protected override Task<ILookup<StreamDescription, string>> GetPKs(
            IEnumerable<StreamDescription> tables)
        {
            _conn.Execute(PK_SETUP);
            return base.GetPKs(tables);
        }

        protected override (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader)
        {
            var table = new StreamDescription(reader.GetString(0), reader.GetString(1));
            var column = new FieldDefinition(reader.GetString(2), GetFieldType(reader));
            if (reader.GetString(5) == "YES") {
                _identity.Add(table, reader.GetString(2));
            }
            return (table, column);
        }

        protected override async Task<StreamDefinition[]> BuildStreamDefinitions()
        {
            _identity.Clear();
            var result = await base.BuildStreamDefinitions();
            foreach (var pair in _identity) {
                var sd = result.First(d => d.Name == pair.Key);
                sd.SeqIdIndex = Array.IndexOf(sd.NameList, pair.Value);
            }
            return result;
        }

        private static readonly HashSet<TypeTag> _typesWithInfo = new()
        {
            TypeTag.Varbinary,
            TypeTag.Varchar,
            TypeTag.Char,
            TypeTag.Binary,
            TypeTag.Nvarchar,
            TypeTag.Nchar,
            TypeTag.Time,
            TypeTag.DateTimeTZ,
            TypeTag.VarDateTime,
            TypeTag.Decimal,
            TypeTag.Float,
        };

        private static FieldType GetFieldType(IDataReader reader)
        {
            var nullable = reader.GetString(3) == "YES";
            var tag = GetTagType(reader.GetString(4));
            if (tag == TypeTag.Unstructured) {
                tag = reader.GetByte(8) > 0 ? TypeTag.Decimal : TypeTag.Long;
            }
            var info = _typesWithInfo.Contains(tag) ? GetInfo(tag, reader) : null;
            if (tag == TypeTag.Decimal) {
                if (info == "51,48") {
                    tag = TypeTag.Int;
                    info = null;
                }
                else
                {

                }
            }
            return new FieldType(tag, nullable, CollectionType.None, info);
        }

        private static string? GetInfo(TypeTag tag, IDataReader reader) => tag switch
        {
            TypeTag.Varbinary or TypeTag.Varchar or TypeTag.Char or TypeTag.Binary or TypeTag.Nvarchar or TypeTag.Nchar
                => reader.GetInt16(6) < 0 ? null : reader.GetInt16(6).ToString(),
            TypeTag.Time or TypeTag.DateTimeTZ or TypeTag.VarDateTime => reader.GetByte(9).ToString(),
            TypeTag.Decimal or TypeTag.Float => $"{reader.GetByte(7)},{reader.GetByte(8)}",
            _ => throw new ArgumentOutOfRangeException($"Type tag '{tag}' does not support extended info")
        };

        protected override (StreamDescription table, string column) BuildPkDefintion(IDataReader reader)
            => (new StreamDescription(reader.GetString(0), reader.GetString(1)), reader.GetString(2));

        private string DepsQuery =>
$@"select fk_tco.table_schema as foreign_schema,
       fk_tco.table_name as foreign_table,
       pk_tco.table_schema as referenced_schema,
       pk_tco.table_name as referenced_table
from information_schema.referential_constraints rco
join information_schema.table_constraints fk_tco 
     on fk_tco.constraint_name = rco.constraint_name
     and fk_tco.constraint_schema = rco.constraint_schema
join information_schema.table_constraints pk_tco
     on pk_tco.constraint_name = rco.unique_constraint_name
     and pk_tco.constraint_schema = rco.unique_constraint_schema
where fk_tco.table_catalog = '{DatabaseName}'
    and fk_tco.table_schema <> 'INFORMATION_SCHEMA'
order by fk_tco.table_schema,
         fk_tco.table_name; ";

        private string TableQuery =>
$@"select table_schema, table_name
from INFORMATION_SCHEMA.TABLES
where table_catalog = '{DatabaseName}'
  and table_type = 'BASE TABLE'";

        protected override async Task<StreamDescription[][]> BuildStreamDependencies()
        {
            var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
            var names = await SqlHelper.ReadValuesAsync(_conn,
                    TableQuery,
                    r => new StreamDescription(r.GetString(0).Trim(), r.GetString(1).Trim()))
                .ToListAsync();
            await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, DepsQuery,
                r => KeyValuePair.Create(
                    new StreamDescription(r.GetString(0).Trim(), r.GetString(1).Trim()),
                    new StreamDescription(r.GetString(2).Trim(), r.GetString(3).Trim())))) 
            {
                deps.Add(pair);
            }
            return OrderDeps(names, deps).Reverse().ToArray();
        }

        protected override string GetDistinctCountQuery(string fieldList, string tableName, long threshold)
            => $"select {fieldList} from (select * from {tableName} limit {threshold}) a;";

        private static readonly Dictionary<string, TypeTag> TYPE_MAP = new()
        {
            { "NUMBER", TypeTag.Unstructured },
            { "NUMERIC", TypeTag.Unstructured },
            { "DECIMAL", TypeTag.Decimal },
            { "INT", TypeTag.Int },
            { "INTEGER", TypeTag.Int },
            { "BIGINT", TypeTag.Long },
            { "SMALLINT", TypeTag.Short },
            { "TINYINT", TypeTag.SByte },
            { "BYTEINT", TypeTag.Byte },
            { "REAL", TypeTag.Double },
            { "FLOAT", TypeTag.Double },
            { "FLOAT4", TypeTag.Double },
            { "FLOAT8", TypeTag.Double },
            { "DOUBLE", TypeTag.Double },
            { "DOUBLE PRECISION", TypeTag.Double },
            { "TEXT", TypeTag.Ntext },
            { "STRING", TypeTag.Ntext },
            { "VARCHAR", TypeTag.Nvarchar },
            { "CHAR", TypeTag.Nchar },
            { "CHARACTER", TypeTag.Nchar },
            { "BOOLEAN", TypeTag.Boolean },
            { "BINARY", TypeTag.Blob },
            { "VARBINARY", TypeTag.Blob },
            { "DATE", TypeTag.DateTime },
            { "DATETIME", TypeTag.DateTime },
            { "TIME", TypeTag.DateTime },
            { "TIMESTAMP", TypeTag.DateTime },
            { "TIMESTAMP_NTZ", TypeTag.DateTime },
            { "TIMESTAMP_LTZ", TypeTag.DateTimeTZ },
            { "TIMESTAMP_TZ", TypeTag.DateTimeTZ },
        };

        protected static TypeTag GetTagType(string v)
        {
            if (TYPE_MAP.TryGetValue(v, out var result)) {
                return result;
            }
            throw new ArgumentException($"Unknown SQL data type '{v}'.");
        }

        protected override string GetTableRowCount(StreamDescription name)
            => @$"select t.row_count
from information_schema.tables t
where t.table_schema = '{name.Namespace}' and t.table_name = '{name.Name}'";

    }
}