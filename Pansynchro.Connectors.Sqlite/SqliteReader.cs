using System.Data.Common;

using Microsoft.Data.Sqlite;

using Pansynchro.SQL;

namespace Pansynchro.Connectors.Sqlite
{
    public class SqliteReader : SqlDbReader
    {
        public SqliteReader(string connectionString) : base(connectionString)
        { }

        protected override DbConnection CreateConnection(string connectionString)
            => new SqliteConnection(connectionString);

        protected override ISqlFormatter SqlFormatter => SqliteFormatter.Instance;
    }
}
