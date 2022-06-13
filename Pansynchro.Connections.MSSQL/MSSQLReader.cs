using System;
using System.Data.Common;

using Microsoft.Data.SqlClient;

using Pansynchro.Core.Incremental;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.MSSQL
{
    public class MSSQLReader : SqlDbReader, IDisposable
    {
        private readonly SqlConnection? _perfConn;

        private const int BATCH_SIZE = 10_000;

        public MSSQLReader(string connectionString, string? perfConnectionString) : base(connectionString)
        {
            if (perfConnectionString != null) {
                _perfConn = new SqlConnection(perfConnectionString);
            }
        }

        protected override DbConnection CreateConnection(string connectionString)
            => new SqlConnection(connectionString);

        protected override ISqlFormatter SqlFormatter => MssqlFormatter.Instance;

        protected override bool SupportsIncrementalReader => true;

        public override bool SetIncrementalStrategy(IncrementalStrategy strategy)
        {
            var result = false;
            switch (strategy) {
                case IncrementalStrategy.Cdc:
                    _incrementalReader = new MssqlCdcReader((SqlConnection)Conn, BATCH_SIZE);
                    result = true;
                    break;
                default:
                    break;
            }
            _getReader = _incrementalReader != null ? _incrementalReader.ReadStreamAsync : this.FullSyncReader;
            return result;
        }

        void IDisposable.Dispose()
        {
            base.Dispose();
            _perfConn?.Dispose();
        }
    }
}
