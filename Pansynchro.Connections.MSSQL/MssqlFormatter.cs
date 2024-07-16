using Pansynchro.Core.Helpers;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.MSSQL
{
	public class MssqlFormatter : ISqlFormatter
	{
		public static MssqlFormatter Instance { get; } = new();

		private MssqlFormatter() { }

		public string QuoteName(string name)
		{
			return '[' + name + ']';
		}

		public string LimitRows(string query, int limit)
			=> query.ReplaceFirst("select ", $"select top {limit} ");
	}
}
