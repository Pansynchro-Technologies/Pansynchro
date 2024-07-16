using Pansynchro.SQL;

namespace Pansynchro.Connectors.Snowflake
{
	public class SnowflakeSqlFormatter : ISqlFormatter
	{
		public static SnowflakeSqlFormatter Instance { get; } = new();

		private SnowflakeSqlFormatter() { }

		public string QuoteName(string name) => '"' + name + '"';
	}
}