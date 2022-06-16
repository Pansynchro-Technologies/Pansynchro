using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using Oracle.ManagedDataAccess.Client;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Oracle
{
    public class OracleWriter : IWriter
    {
        private readonly OracleConnection _conn;
        private DataDictionary? _order;

        public OracleWriter(string connectionString)
        {
            _conn = new OracleConnection(connectionString);
        }

        const OracleBulkCopyOptions COPY_OPTIONS = OracleBulkCopyOptions.UseInternalTransaction;
        const int BATCH_SIZE = 100_000;

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dict)
        {
            var order = new List<StreamDescription>();
            await foreach (var (name, _, reader) in streams) {
                order.Add(name);
                ulong progress = 0; // for debugging purposes
                using var copy = new OracleBulkCopy(_conn, COPY_OPTIONS) {
                    BatchSize = BATCH_SIZE,
                    DestinationTableName = name.ToString(),
                    NotifyAfter = BATCH_SIZE
                };
                copy.OracleRowsCopied += (s, e) => progress = (ulong)e.RowsCopied;
                copy.WriteToServer(reader);
                reader.Dispose();
            }
        }

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
