using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

using Oracle.ManagedDataAccess.Client;

namespace Pansynchro.Connectors.Oracle
{
    // https://localhost:5500/em
    public class OracleSchemaAnalyzer : SqlSchemaAnalyzer
    {
        public OracleSchemaAnalyzer(string connString) : base(new OracleConnection(connString)) { }

        protected override string ColumnsQuery =>
@"select
    OWNER, TABLE_NAME, COLUMN_NAME,
    DATA_TYPE, CHAR_LENGTH, DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
    NULLABLE
from tab_columns";

        protected override (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader)
        {
            var table = new StreamDescription(reader.GetString(0), reader.GetString(1));
            var name = reader.GetString(2);
            var type = GetColumnType(reader);
            var column = new FieldDefinition(name, type);

            return (table, column);
        }

        private FieldType GetColumnType(IDataReader reader)
        {
            var type = GetTagType(reader.GetString(3));
            var nullable = reader.GetChar(8) == 'Y';
            var info = HasInfo(type) ? TypeInfo(type, reader) : null;
            return new FieldType(type, nullable, CollectionType.None, info);
        }

        private static readonly HashSet<TypeTag> _typesWithInfo = new()
        {
            TypeTag.Varbinary, TypeTag.Varchar, TypeTag.Char, TypeTag.Binary, TypeTag.Nvarchar,
            TypeTag.Nchar, TypeTag.Decimal, TypeTag.Numeric, TypeTag.Float
        };

        private static string TypeInfo(TypeTag type, IDataReader reader) => type switch
        {
            TypeTag.Varchar or TypeTag.Char or TypeTag.Nvarchar or TypeTag.Nchar => reader.GetInt16(4).ToString(),
            TypeTag.Varbinary or TypeTag.Binary => reader.GetInt16(5).ToString(),
            TypeTag.Decimal or TypeTag.Numeric or TypeTag.Float => $"{reader.GetByte(5)},{reader.GetByte(6)}",
            _ => throw new ArgumentOutOfRangeException($"Type tag '{type}' does not support extended info")
        };

        private static bool HasInfo(TypeTag type) => _typesWithInfo.Contains(type);

        const string READ_DEPS =
@"select
    a.owner as foreign_schema, a.table_name as foreign_table,
    b.owner as table_schema, b.table_name
from all_constraints a, all_constraints b
where b.constraint_type = 'R'
  and a.constraint_name = b.r_constraint_name
  and (a.owner <> b.owner or a.table_name <> b.table_name)
order by a.owner||'.'||a.table_name";

        protected override async Task<StreamDescription[][]> BuildStreamDependencies()
        {
            var names = new List<StreamDescription>();
            var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
            await foreach (var name in SqlHelper.ReadValuesAsync(_conn, "select OWNER, TABLE_NAME from ALL_TABLES",
                r => new StreamDescription(r.GetString(0), r.GetString(1)))) {
                names.Add(name);
            }
            await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, READ_DEPS,
                r => KeyValuePair.Create(
                    new StreamDescription(r.GetString(0), r.GetString(1)),
                    new StreamDescription(r.GetString(2), r.GetString(3))))) {
                deps.Add(pair);
            }
            return OrderDeps(names, deps).Reverse().ToArray();
        }

        private static readonly Dictionary<string, TypeTag> TYPE_MAP = new()
        {
            { "INTEGER", TypeTag.Int },
            { "SHORTINTEGER", TypeTag.Short },
            { "LONGINTEGER", TypeTag.Long },
            { "DECIMAL", TypeTag.Decimal },
            { "SHORTDECIMAL", TypeTag.Decimal },
            { "NUMBER", TypeTag.Numeric },
            { "CHAR", TypeTag.Char },
            { "NCHAR", TypeTag.Nchar },
            { "VARCHAR", TypeTag.Varchar },
            { "VARCHAR2", TypeTag.Varchar },
            { "NVARCHAR2", TypeTag.Nvarchar },
            { "CLOB", TypeTag.Text },
            { "NCLOB", TypeTag.Ntext },
            { "LONG", TypeTag.Text },
            { "DATE", TypeTag.Date },
            { "VARCHAR", TypeTag.Varchar },
            { "BLOB", TypeTag.Blob },
            { "RAW", TypeTag.Blob },
            { "LONG RAW", TypeTag.Blob },
        };

        protected override TypeTag GetTagType(string v)
        {
            if (TYPE_MAP.TryGetValue(v, out var result)) {
                return result;
            }
            throw new ArgumentException($"Unknown SQL data type '{v}'.");
        }

        protected override string PkQuery => @"select
   acc.owner as schema_name,
   acc.table_name,
   acc.column_name
from all_constraints ac, all_cons_columns acc
where
   ac.constraint_type = 'P'
   and ac.constraint_name = acc.constraint_name
   and ac.owner = acc.owner
order by
   acc.owner,
   acc.table_name,
   acc.position";

        protected override (StreamDescription table, string column) BuildPkDefintion(IDataReader reader)
        {
            return (new StreamDescription(reader.GetString(0), reader.GetString(1)), reader.GetString(2));
        }

        protected override string GetDistinctCountQuery(string fieldList, string tableName, long threshold)
            => $"select {fieldList} from (select (*) from {tableName} where rownum <= {threshold}) a;";
    }
}
