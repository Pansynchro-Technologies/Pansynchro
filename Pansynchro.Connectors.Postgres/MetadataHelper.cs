using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Npgsql;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Postgres
{
	internal class MetadataHelper
	{
		const string CREATE_STMT_BUILDER =
@"  WITH table_rec AS (
		SELECT
			c.relname, n.nspname, c.oid
		FROM pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE relkind = 'r'
			AND n.nspname ILIKE :schemaName
			AND c.relname ILIKE :tableName
		ORDER BY c.relname
	),
	col_rec AS (
		SELECT
			a.attname AS colname,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype,
			a.attrelid AS oid,
			' DEFAULT ' || (
				SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
				FROM pg_catalog.pg_attrdef d
				WHERE d.adrelid = a.attrelid
					AND d.adnum = a.attnum
					AND a.atthasdef) AS column_default_value,
			CASE WHEN a.attnotnull = TRUE THEN
				'NOT NULL'
			ELSE
				'NULL'
			END AS column_not_null,
			a.attnum AS attnum
		FROM pg_catalog.pg_attribute a
		WHERE a.attnum > 0
			AND NOT a.attisdropped
		ORDER BY a.attnum
	),
	con_rec AS (
		SELECT
			conrelid::regclass::text AS relname,
			n.nspname,
			conname,
			pg_get_constraintdef(c.oid) AS condef,
			contype,
			conrelid AS oid
		FROM pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
	),
	glue AS (
		SELECT
			format( E'-- %1$I definition\n\nCREATE TABLE pansynchro.%1$I (\n', table_rec.relname) AS top,
			E'\n);' AS bottom,
			oid
		FROM table_rec
	),
	cols AS (
		SELECT
			string_agg(format('    %I %s%s %s', colname, coltype, column_default_value, column_not_null), E',\n') AS lines,
			oid
		FROM col_rec
		GROUP BY oid
	)
	SELECT
		concat(glue.top, cols.lines, glue.bottom, ';')
	FROM
		glue
		JOIN cols ON cols.oid = glue.oid;";

		public static void EnsureScratchTables(NpgsqlConnection conn, DataDictionary dest)
		{
			var name = dest.Streams.Select(s => s.Name).ToArray();
			EnsureScratchSchema(conn);
			var existing = ListScratchTables(conn, name).ToArray();
			var nameList = new List<StreamDescription>(name);
			nameList.RemoveAll(sd => existing.Contains(sd.Name, StringComparer.InvariantCultureIgnoreCase));
			foreach (var table in nameList) {
				string builder;
				using (var cmd = new NpgsqlCommand(CREATE_STMT_BUILDER, conn)) {
					cmd.Parameters.AddWithValue("schemaName", table.Namespace!.ToLower(CultureInfo.InvariantCulture));
					cmd.Parameters.AddWithValue("tableName", table.Name.ToLower(CultureInfo.InvariantCulture));
					cmd.Prepare();
					builder = (string)cmd.ExecuteScalar()!;
				}
				using var cmd2 = new NpgsqlCommand(builder, conn);
				cmd2.ExecuteNonQuery();
			}
			foreach (var table in existing) {
				TruncateTable(conn, table);
			}
		}

		const string ENSURE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS Pansynchro";

		private static void EnsureScratchSchema(NpgsqlConnection conn)
		{
			using var cmd = new NpgsqlCommand(ENSURE_SCHEMA, conn);
			cmd.ExecuteNonQuery();
		}

		const string FIND_TABLES =
@"SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'pansynchro'
AND TABLE_NAME ILIKE ANY(ARRAY[{0}])";

		private static IEnumerable<string> ListScratchTables(NpgsqlConnection conn, StreamDescription[] name)
		{
			var paramList = string.Join(", ", Enumerable.Range(0, name.Length).Select(i => $":n{i}"));
			var sql = string.Format(CultureInfo.InvariantCulture, FIND_TABLES, paramList);
			using var cmd = new NpgsqlCommand(sql, conn);
			for (int i = 0; i < name.Length; ++i) {
				cmd.Parameters.AddWithValue($"n{i}", name[i].Name.ToLower(CultureInfo.InvariantCulture));
			}
			using var reader = cmd.ExecuteReader();
			while (reader.Read()) {
				yield return reader.GetString(0);
			}
		}

		public static void TruncateTable(NpgsqlConnection conn, string table)
		{
			conn.Execute($"truncate table Pansynchro.{PostgresFormatter.Instance.QuoteName(table)}");
		}

		const string EXTRACT_COLUMNS =
@"select column_name as Name
FROM information_schema.columns
WHERE table_name = :tableName
	and table_schema = :tableSchema
	and is_generated = 'NEVER'";

		const string EXTRACT_PK =
@"SELECT c.column_name, c.data_type, c.table_name, tc.table_schema
FROM information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  and tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = :tableName and c.table_schema = :tableSchema;";

		internal static void MergeTable(NpgsqlConnection conn, string table, string namespace_)
		{
			var columns = PostgresHelper.ReadStrings(
					conn, EXTRACT_COLUMNS, 
					new KeyValuePair<string, object>("tableName", table),
					new KeyValuePair<string, object>("tableSchema", namespace_))
				.Select(c => '"' + c + '"')
				.ToArray();
			var pkColumns = PostgresHelper.ReadStrings(
					conn, EXTRACT_PK,
					new KeyValuePair<string, object>("tableName", table),
					new KeyValuePair<string, object>("tableSchema", namespace_))
				.ToArray();
			if (pkColumns.Length == 0)
			{
				CopyData(conn, table, namespace_, columns);
			}
			else
			{
				var nonPkColumns = columns.Except(pkColumns).ToArray();
				if (nonPkColumns.Length == 0) {
					MergeLinkingTable(conn, table, namespace_, columns, pkColumns);
				} else {
					MergeNormalTable(conn, table, namespace_, columns, pkColumns, nonPkColumns);
				}
			}
		}

		const string MERGE_LINK_TEMPLATE =
@"INSERT INTO ""{0}"".""{1}"" ({2})
select {2} from {3}
ON CONFLICT ({4})
DO NOTHING";

		private static void MergeLinkingTable(NpgsqlConnection conn, string name, string namespace_, string[] columns, string[] pkColumns)
		{
			var formatter = PostgresFormatter.Instance;
			var pkCols = string.Join(", ", pkColumns.Select(formatter.QuoteName));
			var inserterCols = string.Join(", ", columns);
			var inserterVals = string.Join(", ", columns.Select(c => "SOURCE." + c));
			var SQL = string.Format(CultureInfo.InvariantCulture, MERGE_LINK_TEMPLATE,
				namespace_, name, inserterCols, $"Pansynchro.{formatter.QuoteName(name)}", pkCols);
			conn.Execute(SQL);
		}

		const string MERGE_NORMAL_TEMPLATE =
@"INSERT INTO ""{0}"".""{1}"" ({2})
select {2} from {3}
ON CONFLICT ({4})
DO UPDATE SET {5}";

		public static void MergeNormalTable(NpgsqlConnection conn, string name, string namespace_, string[] columns, string[] pkColumns, string[] nonPkColumns)
		{
			var formatter = PostgresFormatter.Instance;
			var pkCols = string.Join(", ", pkColumns.Select(formatter.QuoteName));
			var updater = CorrespondColumns(nonPkColumns, "=", ", ");
			var inserterCols = string.Join(", ", columns);
			var inserterVals = string.Join(", ", columns.Select(c => "SOURCE." + c));
			var SQL = string.Format(CultureInfo.InvariantCulture, MERGE_NORMAL_TEMPLATE,
				namespace_, name, inserterCols, $"Pansynchro.{formatter.QuoteName(name)}", pkCols, updater);
			conn.Execute(SQL);
		}

        const string COPY_DATA_TEMPLATE =
@"INSERT INTO ""{0}"".""{1}"" ({2})
select {2} from {3}";

        public static void CopyData(NpgsqlConnection conn, string name, string namespace_, string[] columns)
        {
            var formatter = PostgresFormatter.Instance;
            var inserterCols = string.Join(", ", columns);
            var inserterVals = string.Join(", ", columns.Select(c => "SOURCE." + c));
            var SQL = string.Format(CultureInfo.InvariantCulture, COPY_DATA_TEMPLATE,
                namespace_, name, inserterCols, $"Pansynchro.{formatter.QuoteName(name)}");
            conn.Execute(SQL);
        }

        private static string CorrespondColumns(string[] columns, string separator, string joiner)
		{
			return string.Join(joiner, columns.Select(c => $"{c} {separator} excluded.{c}"));
		}
	}
}