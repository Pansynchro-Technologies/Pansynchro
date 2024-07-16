using Pansynchro.Core;

namespace Pansynchro.SQL
{
	public interface ISqlFormatter
	{
		string QuoteName(string name);

		string QuoteName(string? ns, string name)
		{
			var result = QuoteName(name);
			if (!string.IsNullOrEmpty(ns)) {
				result = $"{QuoteName(ns)}.{result}";
			}
			return result;
		}

		string QuoteName(StreamDescription stream) => QuoteName(stream.Namespace, stream.Name);

		string LimitRows(string query, int limit) => $"{query} LIMIT {limit}";
	}
}
