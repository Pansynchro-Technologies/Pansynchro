using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.SQL;
using Pansynchro.State;

namespace Pansynchro.Connectors.MSSQL
{
    public class MSSQLWriter : SqlDbWriter
    {
        private readonly SqlConnection? _perfConn;
        private readonly string _connectionString;

        public MSSQLWriter(string connectionString, string? perfConnectionString) : base(new SqlConnection(connectionString))
        {
            _conn.Open();
            if (perfConnectionString != null) {
                _perfConn = new SqlConnection(perfConnectionString);
            }
            _connectionString = connectionString;
        }

        protected override void Setup(DataDictionary dest)
        {
            base.Setup(dest);
        }

        private readonly List<Task> _cleanup = new();

        protected override async Task Finish()
        {
            await Task.WhenAll(_cleanup);
        }

        private void Finish(StreamDescription table)
        {
            using var conn = new SqlConnection(_connectionString);
            conn.Open();
            Console.WriteLine($"{DateTime.Now}: Merging table '{table.Name}'");
            MetadataHelper.MergeTable(conn, table);
            MetadataHelper.TruncateTable(conn, table);
            Console.WriteLine($"{DateTime.Now}: Finished merging table '{table.Name}'");
        }

        const SqlBulkCopyOptions COPY_OPTIONS = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction;

        protected override void BeginIncrementalSync(StreamDescription name)
        {
            _conn.Execute($"set IDENTITY_INSERT {name} ON");
        }

        protected override void FinishIncrementalSync(StreamDescription name)
        {
            _conn.Execute($"set IDENTITY_INSERT {name} OFF");
        }

        protected override void FullStreamSync(StreamDescription name, StreamSettings settings, IDataReader reader)
        {
            Console.WriteLine($"{ DateTime.Now}: Writing to {name}");
            ulong progress = 0;
            var noStaging = MetadataHelper.TableIsEmpty((SqlConnection)_conn, name);
			if (!noStaging) {
                MetadataHelper.EnsureScratchTable((SqlConnection)_conn, name);
            }
            var destName = noStaging ? name.ToString() : $"Pansynchro.[{name.Name}]";
            using var copy = new SqlBulkCopy((SqlConnection)_conn, COPY_OPTIONS, null) {
                BatchSize = BATCH_SIZE,
                DestinationTableName = destName,
                EnableStreaming = true,
                NotifyAfter = BATCH_SIZE,
            };
            BuildColumnMapping(reader, copy.ColumnMappings);
            copy.SqlRowsCopied += (s, e) => progress = (ulong)e.RowsCopied;
            copy.WriteToServer(reader);
            if (!noStaging) {
                _cleanup.Add(Task.Run(() => Finish(name)));
            }
        }

        protected override ISqlFormatter Formatter => MssqlFormatter.Instance;

        private static void BuildColumnMapping(IDataReader reader, SqlBulkCopyColumnMappingCollection map)
        {
            for (int i = 0; i < reader.FieldCount; ++i) {
                var name = reader.GetName(i);
                var column = new SqlBulkCopyColumnMapping(i, name);
                map.Add(column);
            }
        }

        const int BATCH_SIZE = 100_000;
    }
}
