using Pansynchro.SQL;

namespace Pansynchro.Connectors.MySQL
{
	public class MySqlFormatter : ISqlFormatter
	{
		public static MySqlFormatter Instance { get; } = new();

		private MySqlFormatter() { }

		public string QuoteName(string name)
		{
			return '`' + name + '`';
		}
	}
}
