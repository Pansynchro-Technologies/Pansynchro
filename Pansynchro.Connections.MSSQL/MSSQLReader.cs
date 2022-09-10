using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.MSSQL
{
    public class MSSQLReader : SqlDbReader, IDisposable
    {
        private readonly SqlConnection? _perfConn;
        private readonly List<StreamDescription> _cdcStreams;

        private const int BATCH_SIZE = 10_000;

        public MSSQLReader(string connectionString, string? perfConnectionString) : base(connectionString)
        {
            if (perfConnectionString != null) {
                _perfConn = new SqlConnection(perfConnectionString);
            }
            try {
                _cdcStreams = LoadCdcTables();
            } catch {
                _cdcStreams = new();
            }
        }

        protected override DbConnection CreateConnection(string connectionString)
            => new SqlConnection(connectionString);

        protected override ISqlFormatter SqlFormatter => MssqlFormatter.Instance;

        protected override bool SupportsIncrementalReader => true;

        public override Func<StreamDefinition, Task<IDataReader>> GetIncrementalStrategy(StreamDefinition stream)
        {
            if (_cdcStreams.Contains(stream.Name)) {
                _incrementalReader = new MssqlCdcReader((SqlConnection)Conn, BATCH_SIZE);
                return _incrementalReader.ReadStreamAsync;
            }
            return base.GetIncrementalStrategy(stream);
        }

        private const string CDC_QUERY = "select SCHEMA_NAME(schema_id) as ns, name from sys.tables where is_tracked_by_cdc = 1";

        protected List<StreamDescription> LoadCdcTables()
            => SqlHelper.ReadValues(
                    _conn, CDC_QUERY, r => new StreamDescription(r.GetString(0), r.GetString(1)))
                .ToList();

        void IDisposable.Dispose()
        {
            base.Dispose();
            _perfConn?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
