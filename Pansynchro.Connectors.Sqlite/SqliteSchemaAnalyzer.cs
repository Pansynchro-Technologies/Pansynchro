using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.Sqlite
{
    internal class SqliteSchemaAnalyzer : SqlSchemaAnalyzer
    {
        private const string TABLE_NAME_QUERY = @"select name from sqlite_schema where type = 'table'";

        private string[]? _tables;
        private string[] Tables
        {
            get {
                if (_tables == null)
                {
                    _tables = SqlHelper.ReadStrings(_conn, TABLE_NAME_QUERY).ToArray();
                }
                return _tables;
            }
        }

        public SqliteSchemaAnalyzer(string connectionString) : base(new SqliteConnection(connectionString))
        { }

        protected override string ColumnsQuery =>
@"WITH all_tables AS (SELECT name FROM sqlite_master WHERE type = 'table')
SELECT at.name table_name, pti.name, pti.type, pti.""notnull""
FROM all_tables at INNER JOIN pragma_table_info(at.name) pti
ORDER BY table_name, cid;";

        protected override (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader)
        {
            var table = new StreamDescription(null, reader.GetString(0));
            var name = reader.GetString(1);
            var type = GetColumnType(reader);
            var column = new FieldDefinition(name, type);

            return (table, column);
        }

        private FieldType GetColumnType(IDataReader reader)
        {
            var typeName = reader.GetString(2);
            string? info = null;
            if (typeName.EndsWith(')'))
            {
                var startPos = typeName.LastIndexOf('(');
                info = typeName[(startPos + 1)..^1];
                typeName = typeName.Substring(0, startPos);
            }
            var type = GetTagType(typeName);
            var nullable = !reader.GetBoolean(3);
            return new FieldType(type, nullable, CollectionType.None, info);
        }

        protected override string PkQuery =>
@"WITH all_tables AS (SELECT name FROM sqlite_master WHERE type = 'table')
SELECT at.name table_name, pti.name
FROM all_tables at INNER JOIN pragma_table_info(at.name) pti
WHERE pk <> 0
ORDER BY table_name, pk;";

        protected override (StreamDescription table, string column) BuildPkDefintion(IDataReader reader)
            => (new StreamDescription(null, reader.GetString(0)), reader.GetString(1));

        private const string TABLE_QUERY = "SELECT name FROM sqlite_master WHERE type = 'table'";

        private const string READ_DEPS =
@"WITH all_tables AS (SELECT name FROM sqlite_master WHERE type = 'table') 
SELECT at.name table_name, fk.[table]
FROM all_tables at INNER JOIN pragma_foreign_key_list(at.name) fk
order by table_name;";

        protected override async Task<StreamDescription[][]> BuildStreamDependencies()
        {
            var names = new List<StreamDescription>();
            var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
            await foreach (var sd in SqlHelper.ReadValuesAsync(_conn, TABLE_QUERY, r => new StreamDescription(null, r.GetString(0))))
            {
                names.Add(sd);
            }
            await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, READ_DEPS, r => KeyValuePair.Create(new StreamDescription(r.GetString(0), r.GetString(1)), new StreamDescription(r.GetString(2), r.GetString(3)))))
            {
                deps.Add(pair);
            }
            return OrderDeps(names, deps).Reverse().ToArray();
        }

        protected override string GetDistinctCountQuery(string fieldList, string tableName, long threshold)
            => $"select {fieldList} from (select (*) from {tableName} limit {threshold}) a;";

        private static readonly Dictionary<string, TypeTag> TYPE_MAP = new()
        {
            { "integer", TypeTag.Long },
            { "real", TypeTag.Double },
            { "text", TypeTag.Ntext },
            { "blob", TypeTag.Blob },
        };

        private static TypeTag GetTagType(string v)
            => TYPE_MAP.TryGetValue(v.ToLowerInvariant(), out var result) ? result : TypeTag.Unstructured;
    }
}
