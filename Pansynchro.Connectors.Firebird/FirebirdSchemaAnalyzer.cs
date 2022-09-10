using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using FirebirdSql.Data.FirebirdClient;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.Firebird
{
    public class FirebirdSchemaAnalyzer : SqlSchemaAnalyzer
    {
        public FirebirdSchemaAnalyzer(string connectionString) : base(new FbConnection(connectionString))
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }

        protected override string ColumnsQuery =>
@"select
    f.RDB$RELATION_NAME as TableName, f.RDB$FIELD_NAME name,
    fd.RDB$FIELD_TYPE type, fd.RDB$FIELD_SCALE scale, fd.RDB$FIELD_SUB_TYPE, fd.RDB$DIMENSIONS,
    1 - coalesce(f.RDB$NULL_FLAG, 0) nullable, fd.RDB$FIELD_PRECISION, fd.RDB$CHARACTER_LENGTH
from rdb$relation_fields f
join rdb$relations r on f.RDB$RELATION_NAME = r.RDB$RELATION_NAME
join RDB$FIELDS fd on fd.RDB$FIELD_NAME = f.RDB$FIELD_SOURCE
where f.RDB$UPDATE_FLAG = 1
  and r.RDB$RELATION_TYPE = 0
  and coalesce(r.RDB$SYSTEM_FLAG, 0) = 0
order by f.RDB$RELATION_NAME, f.RDB$FIELD_NAME";

        protected override string PkQuery =>
@"select
    rc.rdb$relation_name as table_name,
    sg.rdb$field_name as field_name
from
    rdb$indices ix
    join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name
    join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name
where
    rc.rdb$constraint_type = 'PRIMARY KEY'";

        protected override (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader)
        {
            var table = new StreamDescription(null, reader.GetString(0).Trim());
            var column = new FieldDefinition(reader.GetString(1).Trim(), GetFieldType(reader));
            return (table, column);
        }

        protected override (StreamDescription table, string column) BuildPkDefintion(IDataReader reader)
        {
            return (new StreamDescription(null, reader.GetString(0).Trim()), reader.GetString(1).Trim());
        }

        private static FieldType GetFieldType(IDataReader reader)
        {
            var (tag, info) = GetFieldTypeDefinition(reader);
            return new FieldType(tag, reader.GetInt16(6) == 1, reader.IsDBNull(5) ? CollectionType.None : CollectionType.Array, info);
        }

        private static (TypeTag tag, string? info) GetFieldTypeDefinition(IDataReader reader)
        {
            var typeCode = reader.GetInt16(2);
            if ((typeCode is 7 or 16 or 26) && !reader.IsDBNull(4)) {
                var subtype = reader.GetInt16(4);
                if (subtype == 1) {
                    return (TypeTag.Numeric, $"{reader.GetInt16(7)},{reader.GetInt16(3)}");
                }
                if (subtype == 2) {
                    return (TypeTag.Decimal, $"{reader.GetInt16(7)},{reader.GetInt16(3)}");
                }
            }
            switch (typeCode) {
                case 7: return (TypeTag.Short, null);
                case 8: return (TypeTag.Int, null);
                case 10: return (TypeTag.Float, null);
                case 12: return (TypeTag.Date, null);
                case 14: return (reader.GetInt16(4) switch {
                    0 => TypeTag.Char,
                    1 => TypeTag.Binary,
                    _ => throw new ArgumentException($"Unsupported blob subtype {reader.GetInt16(4)}.")
                }, reader.GetInt16(8).ToString());
                case 16: return (TypeTag.Long, null);
                case 23: return (TypeTag.Boolean, null);
                case 24: return (TypeTag.Decimal64, null);
                case 25: return (TypeTag.Decimal128, null);
                case 26: return (TypeTag.Int128, null);
                case 27: return (TypeTag.Double, null);
                case 28: return (TypeTag.TimeTZ, null);
                case 29: return (TypeTag.DateTimeTZ, null);
                case 35: return (TypeTag.DateTime, null);
                case 37: return (reader.GetInt16(4) switch {
                    0 => TypeTag.Varchar,
                    1 => TypeTag.Varbinary,
                    _ => throw new ArgumentException($"Unsupported blob subtype {reader.GetInt16(4)}.")
                }, reader.GetInt16(8).ToString());
                case 261: return (reader.GetInt16(4) switch { 
                    0 => TypeTag.Blob,
                    1 => TypeTag.Text,
                    _ => throw new ArgumentException($"Unsupported blob subtype {reader.GetInt16(4)}.")
                }, null);
                default: throw new ArgumentException($"Unsupported data type {typeCode}.");
            }
        }

        private const string DEPS_QUERY =
@"SELECT DISTINCT
       rc.RDB$RELATION_NAME AS tableName,
       d2.RDB$DEPENDED_ON_NAME AS refTableName
FROM RDB$RELATION_CONSTRAINTS AS rc
LEFT JOIN RDB$DEPENDENCIES d1 ON d1.RDB$DEPENDED_ON_NAME = rc.RDB$RELATION_NAME
LEFT JOIN RDB$DEPENDENCIES d2 ON d1.RDB$DEPENDENT_NAME = d2.RDB$DEPENDENT_NAME
WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
  AND d1.RDB$DEPENDED_ON_NAME <> d2.RDB$DEPENDED_ON_NAME
  AND d1.RDB$FIELD_NAME <> d2.RDB$FIELD_NAME
  AND rc.RDB$RELATION_NAME <> d2.RDB$DEPENDED_ON_NAME";

        private const string TABLE_QUERY =
@"select RDB$RELATION_NAME
from rdb$relations
where RDB$RELATION_TYPE = 0
  and coalesce(RDB$SYSTEM_FLAG, 0) = 0";

        protected override async Task<StreamDescription[][]> BuildStreamDependencies()
        {
            var names = new List<StreamDescription>();
            var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
            await foreach (var name in SqlHelper.ReadStringsAsync(_conn, TABLE_QUERY)) {
                names.Add(new StreamDescription(null, name.Trim()));
            }
            await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, DEPS_QUERY,
                r => KeyValuePair.Create(
                    new StreamDescription(null, r.GetString(0).Trim()),
                    new StreamDescription(null, r.GetString(1).Trim())))) {
                deps.Add(pair);
            }
            return OrderDeps(names, deps).Reverse().ToArray();
        }

        protected override string GetDistinctCountQuery(string fieldList, string tableName, long threshold)
            => $"select {fieldList} from (select first {threshold} (*) from {tableName}) a;";

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
            var fbType = (FbDbType)row["ProviderType"];
            var info = HasInfo(fbType) ? TypeInfo(fbType, row) : null;
            var type = GetTypeTag(fbType);
            var nullable = (bool)row["AllowDBNull"];
            return new FieldType(type, nullable, CollectionType.None, info);
        }

        private static TypeTag GetTypeTag(FbDbType type) => type switch
        {
            FbDbType.Binary => TypeTag.Binary,
            FbDbType.Boolean => TypeTag.Boolean,
            FbDbType.Char => TypeTag.Nchar,
            FbDbType.Date => TypeTag.Date,
            FbDbType.Decimal => TypeTag.Decimal,
            FbDbType.Double => TypeTag.Double,
            FbDbType.Float => TypeTag.Float,
            FbDbType.Guid => TypeTag.Guid,
            FbDbType.Integer => TypeTag.Int,
            FbDbType.Numeric => TypeTag.Numeric,
            FbDbType.SmallInt => TypeTag.Short,
            FbDbType.Text => TypeTag.Ntext,
            FbDbType.Time => TypeTag.Time,
            FbDbType.TimeStamp => TypeTag.DateTime,
            FbDbType.VarChar => TypeTag.Nvarchar,
            FbDbType.TimeStampTZ => TypeTag.DateTimeTZ,
            FbDbType.TimeTZ => TypeTag.TimeTZ,
            FbDbType.Int128 => TypeTag.Int128,
            _ => throw new DataException($"Data type {type} is not supported")
        };

        private static readonly HashSet<FbDbType> _infoTypes = new() {
            FbDbType.Array,
            FbDbType.Binary,
            FbDbType.Char,
            FbDbType.Decimal,
            FbDbType.Numeric,
            FbDbType.VarChar,
            FbDbType.Text
        };

        private static bool HasInfo(FbDbType type) => _infoTypes.Contains(type);

        private static string? TypeInfo(FbDbType type, DataRow row) => type switch
        {
            FbDbType.Binary or FbDbType.Char or FbDbType.VarChar => (bool)row["IsLong"] ? null : row["ColumnSize"].ToString(),
            FbDbType.Text => null,
            FbDbType.Decimal or FbDbType.Numeric => $"{((byte)row["NumericPrecision"])},{((byte)row["NumericScale"])}",
            _ => throw new ArgumentOutOfRangeException($"Type tag '{type}' does not support extended info")
        };
    }
}
