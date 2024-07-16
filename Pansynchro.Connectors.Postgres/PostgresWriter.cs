using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Npgsql;
using NpgsqlTypes;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.EventsSystem;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.Postgres
{
	public class PostgresWriter : SqlDbWriter
	{
		private DataDictionary? _dict;

		public PostgresWriter(string connectionString) : base(new NpgsqlConnection(connectionString))
		{ }

		protected override ISqlFormatter Formatter => PostgresFormatter.Instance;

		protected override void FullStreamSync(StreamDescription name, StreamSettings settings, IDataReader reader)
		{
			BinCopy(name, reader);
		}

		private void BinCopy(StreamDescription name, IDataReader reader)
		{
			var schema = ExtractSchema(name);
			var fields = string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(i => Formatter.QuoteName(reader.GetName(i))));
			var lName = Formatter.QuoteName(name.Name);
			using var importer = ((NpgsqlConnection)_conn).BeginBinaryImport(
				$"COPY Pansynchro.{lName} ({fields}) FROM STDIN (FORMAT BINARY)");
			var loader = BuildLoader(reader, schema);
			var buffer = new object[reader.FieldCount];
			while (reader.Read()) {
				loader(reader, importer, buffer);
			}
			importer.Complete();
		}

		protected override Task Finish()
		{
			var conn = (NpgsqlConnection)_conn;
			foreach (var table in _dict!.DependencyOrder.SelectMany(sd => sd).Reverse()) {
				EventLog.Instance.AddMergingStreamEvent(table);
				MetadataHelper.MergeTable(conn, table.Name, table.Namespace!);
			}

			foreach (var table in _dict.DependencyOrder.SelectMany(sd => sd)) {
				EventLog.Instance.AddTruncatingStreamEvent(table);
				MetadataHelper.TruncateTable(conn, table.Name);
			}
			return Task.CompletedTask;
		}

		private static Action<IDataReader, NpgsqlBinaryImporter, object[]> BuildLoader(IDataReader reader, Dictionary<string, NpgsqlDbType?> schema)
		{
			Action<NpgsqlBinaryImporter, object[]> result = null!;
			for (int i = 0; i < reader.FieldCount; ++i) {
				var fieldName = reader.GetName(i);
				var dataType = schema[fieldName];
				result += MakeReader(i, dataType!.Value);
			}
			return (r, imp, buffer) => {
				reader.GetValues(buffer);
				imp.StartRow();
				result(imp, buffer);
			};
		}

		private static Action<NpgsqlBinaryImporter, object[]> MakeReader(int i, NpgsqlDbType value)
		{
			return (imp, buffer) => imp.Write(buffer[i], value);
		}

		private Dictionary<string, NpgsqlDbType?> ExtractSchema(StreamDescription name)
		{
			using var cmd = _conn.CreateCommand();
			cmd.CommandText = $"select * from Pansynchro.{Formatter.QuoteName(name.Name)} where 1 = 0";
			using var reader = cmd.ExecuteReader();
			var schema = ((NpgsqlDataReader)reader).GetColumnSchema();
			return schema.ToDictionary(c => c.ColumnName, c => c.NpgsqlDbType, StringComparer.InvariantCultureIgnoreCase);
		}

		protected override void Setup(DataDictionary dest)
		{
			_dict = dest;
			_conn.OpenAsync().GetAwaiter().GetResult();
			try {
				MetadataHelper.EnsureScratchTables((NpgsqlConnection)_conn, dest);
			} finally {
				_conn.Close();
			}
		}
	}
}
