using System.Data.Common;

using Snowflake.Data.Client;

using Pansynchro.SQL;

namespace Pansynchro.Connectors.Snowflake
{
	public class SnowflakeReader : SqlDbReader
	{
		public SnowflakeReader(string connectionString) : base(connectionString)
		{ }

		protected override ISqlFormatter SqlFormatter => SnowflakeSqlFormatter.Instance;

		protected override DbConnection CreateConnection(string connectionString)
			=> new SnowflakeDbConnection { ConnectionString = connectionString };
	}
}
