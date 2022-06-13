using System.Data.Common;

using Pansynchro.SQL;

using MySqlConnector;

namespace Pansynchro.Connectors.MySQL
{
    public class MySqlReader : SqlDbReader
    {
        public MySqlReader(string connectionString) : base(connectionString)
        { }

        protected override DbConnection CreateConnection(string connectionString)
            => new MySqlConnection(connectionString);

        protected override ISqlFormatter SqlFormatter => MySqlFormatter.Instance;
    }
}
