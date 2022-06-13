using System.Data.Common;
using System.Text;

using FirebirdSql.Data.FirebirdClient;

using Pansynchro.SQL;

namespace Pansynchro.Connectors.Firebird
{
    public class FirebirdReader : SqlDbReader
    {
        public FirebirdReader(string connectionString) : base(connectionString)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }

        protected override DbConnection CreateConnection(string connectionString)
            => new FbConnection(connectionString);

        protected override ISqlFormatter SqlFormatter => FirebirdFormatter.Instance;
    }
}
