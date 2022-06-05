using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Npgsql;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.Postgres
{
    public class PostgresReader : IDbReader
    {
        private readonly NpgsqlConnection _conn;
        //private readonly NpgsqlConnection _perfConn;

        private const int BATCH_SIZE = 10_000;
        private const int REPORT_SIZE = 100_000;
        private const int CAP = 1_000_000;
        private const int STRATEGY_VERSION = 5;

        public PostgresReader(string connectionString)
        {
            _conn = new NpgsqlConnection(connectionString);
            //_perfConn = new NpgsqlConnection(perfConnectionString);
        }

        DbConnection IDbReader.Conn => _conn;

        public void SetIncrementalPlan(Dictionary<StreamDescription, string> plan)
        {
            throw new NotImplementedException();
        }

        public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            _conn.Open();
            try
            {
                var streams = source.Streams.ToDictionary(s => s.Name);
                foreach (var name in source.DependencyOrder.SelectMany(s => s))
                {
                    var stream = streams[name];
                    Console.WriteLine($"{DateTime.Now}: Reading table '{stream.Name}'");
                    var recordSize = PayloadSizeAnalyzer.AverageSize(_conn, stream, PostgresFormatter.Instance);
                    Console.WriteLine($"{DateTime.Now}: Average data size: {recordSize}");
                    var columns = string.Join(", ", stream.NameList.Select(s => '"' + s + '"'));
                    var query = new NpgsqlCommand($"select * from \"{stream.Name.Namespace}\".\"{stream.Name.Name}\"", _conn) { CommandTimeout = 0 };
                    yield return new DataStream(stream.Name, StreamSettings.None, await query.ExecuteReaderAsync());
                }
            }
            finally
            {
                _conn.Close();
            }
        }

        const string READ_DEPS =
@"SELECT distinct
  (SELECT nspname FROM pg_namespace WHERE oid=f.relnamespace) AS dependency_schema,
  f.relname AS dependency_table,
  (SELECT nspname FROM pg_namespace WHERE oid=m.relnamespace) AS dependent_schema,
  m.relname AS dependent_table
FROM
  pg_constraint o
LEFT JOIN pg_class f ON f.oid = o.confrelid
LEFT JOIN pg_class m ON m.oid = o.conrelid
WHERE
  o.contype = 'f' AND o.conrelid IN (SELECT oid FROM pg_class c WHERE c.relkind = 'r') and o.conrelid <> o.confrelid
order by f.relname";

        const string TABLE_QUERY =
@"select table_schema, table_name
from information_schema.tables
where table_type = 'BASE TABLE' and table_schema !~ 'pg_' and table_schema != 'information_schema'";

        public async Task<StreamDescription[]> ListStreams()
        {
            await _conn.OpenAsync();
            //_perfConn.Open();
            try
            {
                var names = new List<StreamDescription>();
                var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
                await foreach (var sd in PostgresHelper.ReadValuesAsync(_conn, TABLE_QUERY, r => new StreamDescription(r.GetString(0), r.GetString(1))))
                {
                    names.Add(sd);
                }
                await foreach (var pair in PostgresHelper.ReadValuesAsync(_conn, READ_DEPS, r => KeyValuePair.Create(new StreamDescription(r.GetString(0), r.GetString(1)), new StreamDescription(r.GetString(2), r.GetString(3)))))
                {
                    deps.Add(pair);
                }
                return OrderDeps(names, deps).Reverse().ToArray();
            }
            finally
            {
                _conn.Close();
            }
        }

        private static IEnumerable<StreamDescription> OrderDeps(
            List<StreamDescription> names, List<KeyValuePair<StreamDescription, StreamDescription>> deps)
        {
            while (names.Count > 0) {
                var uncounted = names.Count;
                var freeList = names.Except(deps.Select(p => p.Value)).ToArray();
                if (freeList.Length == 0) {
                    throw new DataException($"Circular references found in {names.Count} tables.");
                }
                foreach (var free in freeList) {
                    yield return free;
                    names.Remove(free);
                    deps.RemoveAll(p => p.Key == free);
                }
            }
        }

        public void Dispose()
        {
            _conn.Dispose();
            //_perfConn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
