using Pansynchro.Core.Helpers;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.Firebird
{
	public class FirebirdFormatter : ISqlFormatter
	{
		public static FirebirdFormatter Instance { get; } = new();

		private FirebirdFormatter() { }

		public string QuoteName(string name)
		{
			return '"' + name + '"';
		}

		public string LimitRows(string query, int limit)
			=> query.ReplaceFirst("select ", $"select first {limit} ");
	}
}
