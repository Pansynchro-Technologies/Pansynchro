using System.Collections.Generic;
using System.Linq;

using Microsoft.Data.SqlClient;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.MSSQL
{
    static class MetadataHelper
    {
        const string CREATE_STMT_BUILDER = 
@"DECLARE  
      @object_name SYSNAME  
    , @object_id INT  
    , @SQL NVARCHAR(MAX)  
  
SELECT  
      @object_name = '[Pansynchro].[' + OBJECT_NAME([object_id]) + ']'  
    , @object_id = [object_id]  
FROM (SELECT [object_id] = OBJECT_ID(@tableName, 'U')) o  
  
SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF((  
    SELECT CHAR(13) + '    , [' + c.name + '] ' +   
        CASE WHEN c.system_type_id != c.user_type_id   
            THEN '[' + SCHEMA_NAME(tp.[schema_id]) + '].[' + tp.name + ']'   
            ELSE '[' + UPPER(tp.name) + ']'   
        END  +   
        CASE   
            WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary')  
                THEN '(' + CASE WHEN c.max_length = -1   
                                THEN 'MAX'   
                                ELSE CAST(c.max_length AS VARCHAR(5))   
                            END + ')'  
            WHEN tp.name IN ('nvarchar', 'nchar')  
                THEN '(' + CASE WHEN c.max_length = -1   
                                THEN 'MAX'   
                                ELSE CAST(c.max_length / 2 AS VARCHAR(5))   
                            END + ')'  
            WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')   
                THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'  
            WHEN tp.name = 'decimal'  
                THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'  
            ELSE ''  
        END +  
        CASE WHEN c.collation_name IS NOT NULL AND c.system_type_id = c.user_type_id   
            THEN ' COLLATE ' + c.collation_name  
            ELSE ''  
        END +  
        CASE WHEN c.is_nullable = 1   
            THEN ' NULL'  
            ELSE ' NOT NULL'  
        END 
    FROM sys.columns c
    JOIN sys.types tp ON c.user_type_id = tp.user_type_id  
    WHERE c.[object_id] = @object_id 
        and c.is_computed <> 1
    ORDER BY c.column_id  
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 7, '      ')
    + CHAR(13) + ');'
  EXEC(@SQL)";

        const string ENSURE_SCHEMA =
@"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Pansynchro')
BEGIN
    EXEC( 'CREATE SCHEMA Pansynchro' );
END";

        const string FIND_TABLES =
@"SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'Pansynchro'
AND TABLE_NAME in ({0})";

        const string EXTRACT_PK =
@"SELECT COL_NAME(ic.[object_id], ic.column_id) as Name
FROM sys.indexes i
JOIN sys.index_columns ic ON i.[object_id] = ic.[object_id]
    AND i.index_id = ic.index_id
WHERE i.is_primary_key = 1
    AND i.[object_id] = object_id(@tableName, 'U')";

        const string EXTRACT_COLUMNS =
@"select c.Name
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.[object_id] = object_id(@tableName, 'U')
    and c.is_computed <> 1";

        public static void EnsureScratchTables(SqlConnection conn, DataDictionary dict)
        {
            EnsureScratchSchema(conn);
            var existing = ListScratchTables(conn, dict).ToArray();
            var lookup = dict.Streams.Select(s => s.Name).ToDictionary(s => s with { Namespace = "Pansynchro" });
            var missing = lookup.Keys.Except(existing).ToArray();
            if (missing.Length > 0)
            {
                using var cmd = new SqlCommand(CREATE_STMT_BUILDER, conn);
                foreach (var table in missing)
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@tableName", lookup[table].ToString());
                    cmd.ExecuteNonQuery();
                }
            }
            foreach (var table in existing)
            {
                TruncateTable(conn, table);
            }
        }

        public static void TruncateTable(SqlConnection conn, StreamDescription table)
        {
            conn.Execute($"truncate table [Pansynchro].[{table.Name}]");
        }

        private static void EnsureScratchSchema(SqlConnection conn)
        {
            using var cmd = new SqlCommand(ENSURE_SCHEMA, conn);
            cmd.ExecuteNonQuery();
        }

        private static IEnumerable<StreamDescription> ListScratchTables(SqlConnection conn, DataDictionary dict)
        {
            var streams = dict.Streams;
            var paramList = string.Join(", ", Enumerable.Range(0, dict.Streams.Length).Select(i => $"@n{i}"));
            var sql = string.Format(FIND_TABLES, paramList);
            using var cmd = new SqlCommand(sql, conn);
            for (int i = 0; i < streams.Length; ++i)
            {
                cmd.Parameters.AddWithValue($"@n{i}", streams[i].Name.Name);
            }
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                yield return new StreamDescription("Pansynchro", reader.GetString(0));
            }
        }

        public static void MergeTable(SqlConnection conn, StreamDescription name)
        {
            var columns = SqlHelper.ReadStrings(conn, EXTRACT_COLUMNS, new KeyValuePair<string, object>("tableName", name.ToString())).ToArray();
            var pkColumns = SqlHelper.ReadStrings(conn, EXTRACT_PK, new KeyValuePair<string, object>("tableName", name.ToString())).ToArray();
            var nonPkColumns = columns.Except(pkColumns).ToArray();
            if (nonPkColumns.Length == 0)
            {
                MergeLinkingTable(conn, name, columns, pkColumns);
            }
            else
            {
                MergeNormalTable(conn, name, columns, pkColumns, nonPkColumns);
            }
        }

        const string MERGE_LINK_TEMPLATE =
@"MERGE [{0}] AS TARGET
USING {1} AS SOURCE
ON ({2})
WHEN NOT MATCHED BY TARGET
THEN INSERT ({3}) VALUES ({4});";

        private static void MergeLinkingTable(SqlConnection conn, StreamDescription name, string[] columns, string[] pkColumns)
        {
            var pkMatch = CorrespondColumns(pkColumns, "=", " AND ");
            var inserterCols = string.Join(", ", columns);
            var inserterVals = string.Join(", ", columns.Select(c => "SOURCE." + c));
            var hasID = (int)new SqlCommand($"SELECT OBJECTPROPERTY(OBJECT_ID('{name}'), 'TableHasIdentity')", conn).ExecuteScalar() == 1;
            if (hasID)
            {
                conn.Execute($"set IDENTITY_INSERT {name} ON");
            }
            var SQL = string.Format(MERGE_LINK_TEMPLATE,
                name, $"Pansynchro.[{name}]", pkMatch, inserterCols, inserterVals);
            conn.Execute(SQL);
            if (hasID)
            {
                conn.Execute($"set IDENTITY_INSERT {name} OFF");
            }
        }

        const string MERGE_NORMAL_TEMPLATE =
@"MERGE [{0}].[{1}] AS TARGET
USING {2} AS SOURCE
ON ({3})
WHEN MATCHED
THEN UPDATE SET {4}
WHEN NOT MATCHED BY TARGET
THEN INSERT ({5}) VALUES ({6});";

        public static void MergeNormalTable(SqlConnection conn, StreamDescription name, string[] columns, string[] pkColumns, string[] nonPkColumns)
        { 
            var pkMatch = CorrespondColumns(pkColumns, "=", " AND ");
            var updater = CorrespondColumns(nonPkColumns, "=", ", ");
            var inserterCols = string.Join(", ", columns);
            var inserterVals = string.Join(", ", columns.Select(c => "SOURCE." + c));
            var hasID = (int)new SqlCommand($"SELECT OBJECTPROPERTY(OBJECT_ID('{name}'), 'TableHasIdentity')", conn).ExecuteScalar() == 1;
            if (hasID)
            {
                conn.Execute($"set IDENTITY_INSERT {name} ON");
            }
            var SQL = string.Format(MERGE_NORMAL_TEMPLATE,
                name.Namespace, name.Name, $"Pansynchro.[{name.Name}]", pkMatch, updater, inserterCols, inserterVals);
            conn.Execute(SQL);
            if (hasID)
            {
                conn.Execute($"set IDENTITY_INSERT {name} OFF");
            }
        }

        private static string CorrespondColumns(string[] columns, string separator, string joiner)
        {
            return string.Join(joiner, columns.Select(c => $"TARGET.{c} {separator} SOURCE.{c}"));
        }
    }
}
