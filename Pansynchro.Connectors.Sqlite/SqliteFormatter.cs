using Pansynchro.SQL;

namespace Pansynchro.Connectors.Sqlite
{
	public class SqliteFormatter : ISqlFormatter
	{
		public static SqliteFormatter Instance { get; } = new();

		private SqliteFormatter() { }

		public string QuoteName(string name)
		{
			return '"' + name + '"';
		}
	}
}
