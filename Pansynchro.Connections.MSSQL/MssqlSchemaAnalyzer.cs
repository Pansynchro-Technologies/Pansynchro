using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.MSSQL
{
    public class MssqlSchemaAnalyzer : SqlSchemaAnalyzer
    {
        public MssqlSchemaAnalyzer(string connectionString) : base(new SqlConnection(connectionString))
        { }

        protected override string ColumnsQuery =>
@"select SCHEMA_NAME(t.schema_id) as SchemaName, t.Name as TableName, c.name, tp.name as typeName, c.max_length, c.scale, c.precision, c.is_nullable,
	CASE WHEN c.system_type_id != c.user_type_id
		THEN SCHEMA_NAME(tp.schema_id) + '.' + tp.name
		ELSE null
	END as FormalTypeName
from sys.columns c
join sys.tables t on t.object_id = c.object_id
JOIN sys.types tp ON c.user_type_id = tp.user_type_id
where c.is_computed <> 1
  and SCHEMA_NAME(t.schema_id) not like 'pansynchro'
  and t.is_ms_shipped = 0";

        protected override string PkQuery =>
@"SELECT 
	OBJECT_SCHEMA_NAME(c.object_id) as [schema],
	OBJECT_NAME(c.object_id) as [table],
    c.name
FROM sys.indexes i
    inner join sys.index_columns ic  ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    inner join sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
WHERE i.is_primary_key = 1";

        const string CUSTOM_TYPE_QUERY =
@"select SCHEMA_NAME(t.schema_id) SchemaName, t.name, null, bt.name, t.max_length, t.scale, t.precision, t.is_nullable,
	CASE WHEN bt.system_type_id != bt.user_type_id
		THEN SCHEMA_NAME(bt.schema_id) + '.' + bt.name
		ELSE null
	END as FormalTypeName
from sys.types t
join sys.types bt on bt.user_type_id = t.system_type_id
where t.is_user_defined = 1";

        protected override async Task<Dictionary<string, FieldType>> LoadCustomTypes()
        {
            await foreach (var (table, column) in SqlHelper.ReadValuesAsync(_conn, CUSTOM_TYPE_QUERY, BuildFieldDefinition))
            {
                _customTypes.Add(table.ToString(), column.Type);
            }
            return _customTypes;
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

        private readonly Dictionary<string, FieldType> _customTypes = new();

        private FieldType GetColumnType(IDataReader reader)
        {
            var formalType = !reader.IsDBNull(8);
            var typeName = reader.GetString(formalType ? 8 : 3);
            if (formalType) {
                if (_customTypes.ContainsKey(typeName)) {
                    return new FieldType(TypeTag.Custom, false, CollectionType.None, typeName);
                }
            }
            var type = GetTagType(typeName);
            var info = HasInfo(type) ? TypeInfo(type, reader) : null;
            var nullable = reader.GetBoolean(7);
            return new FieldType(type, nullable, CollectionType.None, info);
        }

        private static string? TypeInfo(TypeTag type, IDataReader reader) => type switch
        {
            TypeTag.Varbinary or TypeTag.Varchar or TypeTag.Char or TypeTag.Binary => reader.GetInt16(4) < 0 ? null : reader.GetInt16(4).ToString(CultureInfo.InvariantCulture),
            TypeTag.Nvarchar or TypeTag.Nchar => reader.GetInt16(4) < 0 ? null : (reader.GetInt16(4) / 2).ToString(CultureInfo.InvariantCulture),
            TypeTag.Time or TypeTag.DateTimeTZ or TypeTag.VarDateTime => reader.GetByte(5).ToString(CultureInfo.InvariantCulture),
            TypeTag.Decimal or TypeTag.Float => $"{reader.GetByte(6)},{reader.GetByte(5)}",
            _ => throw new ArgumentOutOfRangeException($"Type tag '{type}' does not support extended info")
        };

        private static readonly HashSet<TypeTag> _typesWithInfo = new() {
            TypeTag.Varbinary, TypeTag.Varchar, TypeTag.Char, TypeTag.Binary, TypeTag.Nvarchar,
            TypeTag.Nchar, TypeTag.Time, TypeTag.DateTimeTZ, TypeTag.VarDateTime,
            TypeTag.Decimal, TypeTag.Float
        };

        private static bool HasInfo(TypeTag type) => _typesWithInfo.Contains(type);

        private static readonly Dictionary<string, TypeTag> TYPE_MAP = new() {
            /*
        Double,
        TimeTZ,
        Interval,
        Json,
             */
            { "binary", TypeTag.Binary},
            { "bigint", TypeTag.Long},
            { "bit", TypeTag.Boolean},
            { "char", TypeTag.Char},
            { "date", TypeTag.Date},
            { "datetime", TypeTag.DateTime},
            { "datetime2", TypeTag.VarDateTime},
            { "datetimeoffset", TypeTag.DateTimeTZ},
            { "decimal", TypeTag.Decimal},
            { "float", TypeTag.Float},
            { "image", TypeTag.Blob},
            { "int", TypeTag.Int },
            { "sys.geography", TypeTag.Geography},
            { "sys.hierarchyid", TypeTag.HierarchyID},
            { "money", TypeTag.Money},
            { "nchar", TypeTag.Nchar},
            { "ntext", TypeTag.Ntext},
            { "numeric", TypeTag.Numeric},
            { "nvarchar", TypeTag.Nvarchar},
            { "real", TypeTag.Single},
            { "smalldatetime", TypeTag.SmallDateTime},
            { "smallint", TypeTag.Short},
            { "smallmoney", TypeTag.SmallMoney},
            { "sys.sysname", TypeTag.Nvarchar},
            { "text", TypeTag.Text},
            { "time", TypeTag.Time},
            { "tinyint", TypeTag.Byte},
            { "uniqueidentifier", TypeTag.Guid},
            { "varbinary", TypeTag.Varbinary},
            { "varchar", TypeTag.Varchar},
            { "xml", TypeTag.Xml}
        };

        private static TypeTag GetTagType(string v)
        {
            if (TYPE_MAP.TryGetValue(v, out var result)) {
                return result;
            }
            throw new ArgumentException($"Unknown SQL data type '{v}'.");
        }

        const string READ_DEPS =
@"select SchemaName as DependencySchema, TableName as Dependency,
         ReferenceSchemaName as DependentSchema, ReferenceTableName as Dependent from (
	SELECT f.name AS ForeignKey, OBJECT_NAME(f.parent_object_id) AS TableName,
		OBJECT_SCHEMA_NAME(f.parent_object_id) AS SchemaName,
		COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
		OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,
		OBJECT_SCHEMA_NAME(f.referenced_object_id) AS ReferenceSchemaName,
		COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName
	FROM sys.foreign_keys AS f
	INNER JOIN sys.foreign_key_columns AS fc
	ON f.OBJECT_ID = fc.constraint_object_id
) t
where t.TableName <> t.ReferenceTableName
order by TableName";

        protected override async Task<StreamDescription[][]> BuildStreamDependencies()
        {
            var names = new List<StreamDescription>();
            var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
            await foreach (var name in SqlHelper.ReadValuesAsync(_conn, "select SCHEMA_NAME(schema_id), name from sys.tables where is_ms_shipped = 0 and SCHEMA_NAME(schema_id) not like 'pansynchro'",
                r => new StreamDescription(r.GetString(0), r.GetString(1))))
            {
                names.Add(name);
            }
            await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, READ_DEPS,
                r => KeyValuePair.Create(
                    new StreamDescription(r.GetString(0), r.GetString(1)),
                    new StreamDescription(r.GetString(2), r.GetString(3)))))
            {
                deps.Add(pair);
            }
            return OrderDeps(names, deps).Reverse().ToArray();
        }

        protected override string GetDistinctCountQuery(string fieldList, string tableName, long threshold)
            => $"select {fieldList} from (select top {threshold} * from {tableName}) a;";

        protected override string GetTableRowCount(StreamDescription name)
            => $@"SELECT SUM(p.rows) FROM sys.partitions AS p
  INNER JOIN sys.tables AS t
  ON p.[object_id] = t.[object_id]
  INNER JOIN sys.schemas AS s
  ON s.[schema_id] = t.[schema_id]
  WHERE t.name = N'{name.Name}'
  AND s.name = N'{name.Namespace}'
  AND p.index_id IN (0,1);";

        protected override FieldDefinition[] AnalyzeCustomTableFields(IDataReader reader)
        {
            var schema = reader.GetSchemaTable()!;
            var columns = schema.Select();
            var fields = columns.Select(BuildFieldDef).ToArray();
            return fields;
        }

        private FieldDefinition BuildFieldDef(DataRow row)
        {
            var name = (string)row["ColumnName"];
            return new FieldDefinition(name, BuildFieldType(row));
        }

        private FieldType BuildFieldType(DataRow row)
        {
            var typeName = (string)row["DataTypeName"];
            if (_customTypes.ContainsKey(typeName)) {
                return new FieldType(TypeTag.Custom, false, CollectionType.None, typeName);
            }
            var type = GetTagType(typeName);
            var info = HasInfo(type) ? TypeInfo(type, row) : null;
            var nullable = (bool)row["AllowDBNull"];
            return new FieldType(type, nullable, CollectionType.None, info);
        }

        private static string? TypeInfo(TypeTag type, DataRow row) => type switch
        {
            TypeTag.Varbinary or TypeTag.Varchar or TypeTag.Char or TypeTag.Binary => (bool)row["IsLong"] ? null : row["ColumnSize"].ToString(),
            TypeTag.Nvarchar or TypeTag.Nchar => (bool)row["IsLong"] ? null : ((int)row["ColumnSize"] / 2).ToString(CultureInfo.InvariantCulture),
            TypeTag.Time or TypeTag.DateTimeTZ or TypeTag.VarDateTime => ((byte)row["NumericScale"]).ToString(CultureInfo.InvariantCulture),
            TypeTag.Decimal or TypeTag.Float => $"{((byte)row["NumericPrecision"])},{((byte)row["NumericScale"])}",
            _ => throw new ArgumentOutOfRangeException($"Type tag '{type}' does not support extended info")
        };
    }
}
