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

		protected override ISqlFormatter Formatter => MySqlFormatter.Instance;

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

        private static FieldType GetFieldType(IDataReader reader)
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

        protected static TypeTag GetTagType(string v)
        {
            if (TYPE_MAP.TryGetValue(v, out var result)) {
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
            await foreach (var name in SqlHelper.ReadStringsAsync(_conn, TableQuery)) {
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

        protected override FieldDefinition[] AnalyzeCustomTableFields(IDataReader reader)
        {
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

        private static FieldType BuildFieldType(DataRow row)
        {
            var msType = (MySqlDbType)row["ProviderType"];
            var info = HasInfo(msType) ? TypeInfo(msType, row) : null;
            var type = GetTypeTag(msType);
            var nullable = (bool)row["AllowDBNull"];
            return new FieldType(type, nullable, CollectionType.None, info);
        }

        private static TypeTag GetTypeTag(MySqlDbType type) => type switch
        {
            MySqlDbType.Bool => TypeTag.Boolean,
            MySqlDbType.Decimal => TypeTag.Decimal,
            MySqlDbType.Byte => TypeTag.SByte,
            MySqlDbType.Int16 => TypeTag.Short,
            MySqlDbType.Int32 => TypeTag.Int,
            MySqlDbType.Int64 => TypeTag.Long,
            MySqlDbType.Float => TypeTag.Float,
            MySqlDbType.Double => TypeTag.Double,
            MySqlDbType.Timestamp or MySqlDbType.DateTime => TypeTag.DateTime,
            MySqlDbType.Date => TypeTag.Date,
            MySqlDbType.Time => TypeTag.Time,
            MySqlDbType.VarString or MySqlDbType.String or MySqlDbType.VarChar => TypeTag.Nvarchar,
            MySqlDbType.Text => TypeTag.Ntext,
            MySqlDbType.Blob or MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob => TypeTag.Blob,
            MySqlDbType.JSON => TypeTag.Json,
            MySqlDbType.UByte => TypeTag.Byte,
            MySqlDbType.UInt16 => TypeTag.UShort,
            MySqlDbType.UInt32 => TypeTag.UInt,
            MySqlDbType.UInt64 => TypeTag.ULong,
            MySqlDbType.Binary or MySqlDbType.VarBinary => TypeTag.Binary,
            MySqlDbType.Text or MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText => TypeTag.Text,
            MySqlDbType.Guid => TypeTag.Guid,
            _ => throw new DataException($"Data type {type} is not supported")
        };

        private static readonly HashSet<MySqlDbType> _infoTypes = new() {
            MySqlDbType.Binary,
            MySqlDbType.VarBinary,
            MySqlDbType.VarChar,
            MySqlDbType.VarString,
            MySqlDbType.String,
            MySqlDbType.Decimal
        };

        private static bool HasInfo(MySqlDbType type) => _infoTypes.Contains(type);

        private static string? TypeInfo(MySqlDbType type, DataRow row) => type switch
        {
            MySqlDbType.Binary or MySqlDbType.VarBinary or MySqlDbType.String or MySqlDbType.VarChar or MySqlDbType.VarString => (bool)row["IsLong"] ? null : row["ColumnSize"].ToString(),
            MySqlDbType.Decimal => $"{((byte)row["NumericPrecision"])},{((byte)row["NumericScale"])}",
            _ => throw new ArgumentOutOfRangeException($"Type tag '{type}' does not support extended info")
        };
    }
}
