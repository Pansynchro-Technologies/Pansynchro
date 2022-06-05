using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

using MySqlConnector;

namespace Pansynchro.Connectors.MySQL
{
    public class MySqlSchemaAnalyzer : SqlSchemaAnalyzer
    {
        public MySqlSchemaAnalyzer(string connectionString) : base(new MySqlConnection(connectionString))
        { }

        protected override string ColumnsQuery =>
$@"select TABLE_NAME, COLUMN_NAME,
    IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
from INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = '{DatabaseName}' and GENERATION_EXPRESSION = ''
order by TABLE_NAME, COLUMN_NAME";

        protected override (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader)
        {
            var table = new StreamDescription(null, reader.GetString(0));
            var column = new FieldDefinition(reader.GetString(1), GetFieldType(reader));
            return (table, column);
        }

        protected override (StreamDescription table, string column) BuildPkDefintion(IDataReader reader)
        {
            return (new StreamDescription(null, reader.GetString(0)), reader.GetString(1));
        }

        private FieldType GetFieldType(IDataReader reader)
        {
            var nullable = reader.GetString(2) == "YES";
            var tag = GetTagType(reader.GetString(3));
            var info = _typesWithInfo.Contains(tag) ? GetInfo(tag, reader) : null;
            return new FieldType(tag, nullable, CollectionType.None, info);
        }

        private static readonly Dictionary<string, TypeTag> TYPE_MAP = new()
        {
            { "bit", TypeTag.Bits },
            { "blob", TypeTag.Blob },
            { "datetime", TypeTag.DateTime },
            { "decimal", TypeTag.Decimal },
            { "double", TypeTag.Double },
            { "int", TypeTag.Int },
            { "mediumtext", TypeTag.Text },
            { "numeric", TypeTag.Decimal },
            { "smallint", TypeTag.Short },
            { "text", TypeTag.Text },
            { "time", TypeTag.Time },
            { "timestamp", TypeTag.DateTime },
            { "tinyint", TypeTag.Byte },
            { "varbinary", TypeTag.Varbinary },
            { "varchar", TypeTag.Varchar },
        };

        protected override TypeTag GetTagType(string v)
        {
            if (TYPE_MAP.TryGetValue(v, out var result))
            {
                return result;
            }
            throw new ArgumentException($"Unknown SQL data type '{v}'.");
        }

        private static string? GetInfo(TypeTag tag, IDataReader reader) => tag switch
        {
            TypeTag.Varbinary or TypeTag.Varchar or TypeTag.Char or TypeTag.Binary or TypeTag.Nvarchar or TypeTag.Nchar
                => reader.GetInt16(4) < 0 ? null : reader.GetInt16(4).ToString(),
            TypeTag.Time or TypeTag.DateTimeTZ or TypeTag.VarDateTime => reader.GetByte(7).ToString(),
            TypeTag.Decimal or TypeTag.Float => $"{reader.GetByte(5)},{reader.GetByte(6)}",
            TypeTag.Bits => reader.GetByte(5).ToString(),
            _ => throw new ArgumentOutOfRangeException($"Type tag '{tag}' does not support extended info")
        };

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
            TypeTag.Bits,
        };

        private string DepsQuery =>
$@"select distinct TABLE_NAME, REFERENCED_TABLE_NAME
from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
where CONSTRAINT_SCHEMA = '{DatabaseName}'
  and TABLE_NAME <> REFERENCED_TABLE_NAME";

        private string TableQuery =>
$@"select table_name
from INFORMATION_SCHEMA.TABLES
where table_schema = '{DatabaseName}'
  and table_type = 'BASE TABLE'";

        protected override string PkQuery => 
$@"SELECT t.table_name, k.column_name
FROM information_schema.table_constraints t
JOIN information_schema.key_column_usage k
USING(constraint_name,table_schema,table_name)
WHERE t.constraint_type='PRIMARY KEY'
  AND t.table_schema='{DatabaseName}';";

        protected override async Task<StreamDescription[][]> BuildStreamDependencies()
        {
            var names = new List<StreamDescription>();
            var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
            await foreach (var name in SqlHelper.ReadStringsAsync(_conn, TableQuery))
            {
                names.Add(new StreamDescription(null, name.Trim()));
            }
            await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, DepsQuery,
                r => KeyValuePair.Create(
                    new StreamDescription(null, r.GetString(0).Trim()),
                    new StreamDescription(null, r.GetString(1).Trim()))))
            {
                deps.Add(pair);
            }
            return OrderDeps(names, deps).Reverse().ToArray();
        }

        protected override string GetDistinctCountQuery(string fieldList, string tableName, long threshold)
            => $"select {fieldList} from (select * from {tableName} limit {threshold}) a;";
    }
}
