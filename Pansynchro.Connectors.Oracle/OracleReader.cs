using System.Data.Common;

using Oracle.ManagedDataAccess.Client;

using Pansynchro.SQL;

namespace Pansynchro.Connectors.Oracle
{
    public class OracleReader : SqlDbReader
    {
        public OracleReader(string connectionString) : base(connectionString)
        { }

        protected override DbConnection CreateConnection(string connectionString)
            => new OracleConnection(connectionString);

        protected override ISqlFormatter SqlFormatter => OracleFormatter.Instance;
    }
}
