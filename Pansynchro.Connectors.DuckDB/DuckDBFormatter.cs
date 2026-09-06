using Pansynchro.SQL;

namespace Pansynchro.Connectors.DuckDB;

internal class DuckDBFormatter : ISqlFormatter
{
	public static DuckDBFormatter Instance { get; } = new();
	public string QuoteName(string name) => $"\"{name}\"";
}
