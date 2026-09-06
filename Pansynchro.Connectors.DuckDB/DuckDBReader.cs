using System.Data.Common;

using DuckDB.NET.Data;

using Pansynchro.SQL;

namespace Pansynchro.Connectors.DuckDB;

internal class DuckDBReader : SqlDbReader
{
	public DuckDBReader(string connectionString) : base(connectionString)
	{ }

	protected override ISqlFormatter SqlFormatter => DuckDBFormatter.Instance;

	protected override DbConnection CreateConnection(string connectionString)
		=> new DuckDBConnection(connectionString);
}
