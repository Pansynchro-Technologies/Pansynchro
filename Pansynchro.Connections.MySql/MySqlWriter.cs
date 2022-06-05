using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using MySqlConnector;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.MySQL
{
    public class MySqlWriter : IWriter
    {
        private readonly MySqlConnection _conn;

        public MySqlWriter(string connectionString)
        {
            _conn = new(connectionString);
            _conn.InfoMessage += Conn_InfoMessage;
        }

        private void Conn_InfoMessage(object sender, MySqlInfoMessageEventArgs args)
        {
            foreach (var err in args.Errors)
            {
                Console.WriteLine($"{err.Level}: {err.Message}");
            }
        }

        const int BATCH_SIZE = 100_000;

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            await foreach (var (name, averageSize, reader) in streams) {
                ulong progress = 0;
                var copy = new MySqlBulkCopy(_conn) {
                    DestinationTableName = $"`{name}`",
                    NotifyAfter = BATCH_SIZE
                };
                copy.MySqlRowsCopied += (s, e) => progress = (ulong)e.RowsCopied;
                copy.ColumnMappings.AddRange(GetColumnMapping(reader));
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                copy.WriteToServer(reader);
                stopwatch.Stop();
                reader.Dispose();
            }
            Console.WriteLine("Finished!");
        }

        private static IEnumerable<MySqlBulkCopyColumnMapping> GetColumnMapping(IDataReader reader) =>
            Enumerable.Range(0, reader.FieldCount)
                .Select(i => new MySqlBulkCopyColumnMapping(i, reader.GetName(i)));

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
