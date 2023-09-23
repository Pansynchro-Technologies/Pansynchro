using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using Pansynchro.Connectors.MSSQL.Incremental;
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
        private readonly List<StreamDescription> _ctStreams;

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
            try {
                _ctStreams = LoadCtTables();
            } catch {
                _ctStreams = new();
            }
        }

        protected override DbConnection CreateConnection(string connectionString)
            => new SqlConnection(connectionString);

        protected override ISqlFormatter SqlFormatter => MssqlFormatter.Instance;

        protected override bool SupportsIncrementalReader => true;

        public override Func<StreamDefinition, Task<IDataReader>> GetIncrementalStrategy(StreamDefinition stream)
        {
            var sn = stream.Name with { Name = stream.Name.Name.ToUpperInvariant(), Namespace = stream.Name.Namespace?.ToUpperInvariant() };
            if (_cdcStreams.Contains(sn)) {
                _incrementalReader = new MssqlCdcReader((SqlConnection)Conn, BATCH_SIZE, (SqlTransaction)_tran);
                return _incrementalReader.ReadStreamAsync;
            }
            if (_ctStreams.Contains(sn)) {
                _incrementalReader = new MssqlCtReader((SqlConnection)Conn, BATCH_SIZE, (SqlTransaction)_tran);
                return _incrementalReader.ReadStreamAsync;
            }
            return base.GetIncrementalStrategy(stream);
        }

        private const string CDC_QUERY = "select SCHEMA_NAME(schema_id) as ns, name from sys.tables where is_tracked_by_cdc = 1";

        protected List<StreamDescription> LoadCdcTables()
        {
            _conn.Open();
            try {
                return SqlHelper.ReadValues(
                        _conn, CDC_QUERY, r => new StreamDescription(r.GetString(0).ToUpperInvariant(), r.GetString(1).ToUpperInvariant()))
                    .ToList();
            } finally {
                _conn.Close();
            }
        }

        private const string CT_QUERY = @"
SELECT s.name as SCHEMA_NAME, t.name as TABLE_NAME 
FROM sys.change_tracking_tables ctt
JOIN sys.tables t on t.object_id = ctt.object_id
JOIN sys.schemas s on s.schema_id = t.schema_id";

        protected List<StreamDescription> LoadCtTables()
        {
            _conn.Open();
            try {
                return SqlHelper.ReadValues(
                        _conn, CT_QUERY, r => new StreamDescription(r.GetString(0).ToUpperInvariant(), r.GetString(1).ToUpperInvariant()))
                    .ToList();
            } finally {
                _conn.Close();
            }
        }

        void IDisposable.Dispose()
        {
            base.Dispose();
            _perfConn?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
