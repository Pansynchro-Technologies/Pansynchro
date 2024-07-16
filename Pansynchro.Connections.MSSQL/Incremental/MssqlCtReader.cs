using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;
using Pansynchro.Core;
using Pansynchro.Core.DataDict;

using Pansynchro.Core.Incremental;
using Pansynchro.SQL;

namespace Pansynchro.Connectors.MSSQL.Incremental
{
	internal class MssqlCtReader : IIncrementalStreamReader
	{
		private readonly SqlConnection _conn;
		private readonly int _bookmarkLength;
		private long _startingPoint;
		private readonly SqlTransaction _tran;

		public MssqlCtReader(SqlConnection conn, int bookmarkLength, SqlTransaction tran)
		{
			_conn = conn;
			_bookmarkLength = bookmarkLength;
			_tran = tran;
		}

		public IncrementalStrategy Strategy => IncrementalStrategy.ChangeTracking;

		private const string CT_QUERY = @"
SELECT
    CH.SYS_CHANGE_VERSION, CH.SYS_CHANGE_OPERATION, {0}
FROM CHANGETABLE(CHANGES {1}, @version) as CH
LEFT OUTER JOIN {1} as T ON {2}";

		public async Task<IDataReader> ReadStreamAsync(StreamDefinition stream)
		{
			var name = ((ISqlFormatter)MssqlFormatter.Instance).QuoteName(stream.Name);
			var names = string.Join(", ", stream.NameList.OrderBy(n => n).Select(s => $"T.[{s}]"));
			var joinCriteria = string.Join(" AND ", stream.Identity.Select(s => $"CH.[{s}] = T.[{s}]"));
			var query = string.Format(CT_QUERY, names, name, joinCriteria);
			using var cmd = new SqlCommand(query, _conn, _tran);
			await cmd.PrepareAsync();
			cmd.Parameters.AddWithValue("version", _startingPoint);
			var reader = await cmd.ExecuteReaderAsync();
			return new MssqlCtDataReader(reader, _bookmarkLength);
		}

		public void StartFrom(string? bookmark)
			=> _startingPoint = bookmark != null ? long.Parse(bookmark) : 0;

		public string CurrentPoint(StreamDescription name)
		{
			if (_startingPoint == 0) {
				ReadStartingPoint();
			}
			return _startingPoint.ToString();
		}

		private void ReadStartingPoint()
		{
			using var cmd = new SqlCommand("select CHANGE_TRACKING_CURRENT_VERSION()", _conn, _tran);
			var result = cmd.ExecuteScalar();
			_startingPoint = (long)result;
		}
	}
}