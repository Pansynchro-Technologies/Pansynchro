using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.EventsSystem;
using Pansynchro.Core.Incremental;

namespace Pansynchro.SQL
{
	public abstract class SqlDbReader : IDbReader, IRandomStreamReader, IIncrementalReader
	{
		protected readonly DbConnection _conn;
		public DbConnection Conn => _conn;
		protected DbTransaction? _tran;

		protected ConcurrentDictionary<StreamDescription, IIncrementalStreamReader> _incrementalReaders = new();
		private Dictionary<StreamDescription, string>? _incrementalPlan;

		public SqlDbReader(string connectionString)
		{
			_conn = CreateConnection(connectionString);
		}

		protected abstract DbConnection CreateConnection(string connectionString);

		protected virtual bool SupportsIncrementalReader => false;

		public void SetIncrementalPlan(Dictionary<StreamDescription, string> plan)
		{
			if (SupportsIncrementalReader) {
				_incrementalPlan = plan;
			} else {
				throw new NotImplementedException();
			}
		}

		public Dictionary<StreamDescription, string>? IncrementalPlan => _incrementalPlan;

		protected abstract ISqlFormatter SqlFormatter { get; }

		protected Task<IDataReader> FullSyncReader(StreamDefinition stream)
			=> FullSyncReader(stream, -1);

		protected async Task<IDataReader> FullSyncReader(StreamDefinition stream, int maxRows)
		{
			var rcf = stream.RareChangeFields;
			var columnList = rcf?.Length > 0
				? stream.NameList.Except(rcf).Concat(rcf)
				: stream.NameList;
			var formatter = SqlFormatter;
			var columns = (stream.Fields.Any(f => f.CustomRead != null))
				? GetCustomColumnList(stream, columnList, formatter)
				: string.Join(", ", columnList.Select(formatter.QuoteName));
			var sql = stream.CustomQuery != null
				? $"select {columns} from ({stream.CustomQuery}) cq"
				: $"select {columns} from {formatter.QuoteName(stream.Name)}";
			if (stream.Identity?.Length > 0) {
				sql = $"{sql} order by {string.Join(", ", stream.Identity.Select(formatter.QuoteName))}";
			}
			if (maxRows >= 0) {
				sql = formatter.LimitRows(sql, maxRows);
			}
			using var query = _conn.CreateCommand();
			query.CommandText = sql;
			query.CommandTimeout = 0;
			query.Transaction = _tran;
			return await query.ExecuteReaderAsync();
		}

		private async Task<IDataReader> AuditReader(StreamDefinition stream)
		{
			var formatter = SqlFormatter;
			var columns = (stream.Fields.Any(f => f.CustomRead != null))
				? GetCustomColumnList(stream, stream.NameList, formatter)
				: string.Join(", ", stream.NameList.Select(formatter.QuoteName));
			var sql = stream.CustomQuery != null 
				? $"select {columns} from ({stream.CustomQuery})"
				: $"select {columns} from {formatter.QuoteName(stream.Name)}";
			var bookmark = _incrementalPlan?.TryGetValue(stream.Name, out var av) == true ? av : null;
			if (bookmark != null) {
				var auditColumn = formatter.QuoteName(stream.Fields[stream.AuditFieldIndex!.Value].Name);
				sql = $"{sql} where {auditColumn} > {bookmark}";
			}
			using var query = _conn.CreateCommand();
			query.CommandText = sql;
			query.CommandTimeout = 0;
			query.Transaction = _tran;
			return new SqlAuditReader(await query.ExecuteReaderAsync(), stream.Fields[stream.AuditFieldIndex!.Value].Name);
		}

		public static string GetCustomColumnList(
			StreamDefinition stream, IEnumerable<string> columnList, ISqlFormatter formatter)
		{
			var result = new List<string>();
			foreach (var column in columnList) {
				var field = stream.Fields.First(f => f.Name == column);
				var fcr = field.CustomRead;
				var qn = formatter.QuoteName(column);
				result.Add(fcr != null ? $"({fcr}) AS {qn}" : qn);
			}
			return string.Join(", ", result);
		}

		public async Task<DataStream> ReadStream(DataDictionary source, string name, int maxResults)
		{
			var stream = source.GetStream(name);
			if (_conn.State == ConnectionState.Closed) {
				await _conn.OpenAsync();
			}
			var result = await FullSyncReader(stream, maxResults);
			return new DataStream(stream.Name, StreamSettings.None, result);
		}

		public Task<DataStream> ReadStream(DataDictionary source, string name)
			=> ReadStream(source, name, -1);

		public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
		{
			await _conn.OpenAsync();
			_tran = _conn.BeginTransaction(IsolationLevel.Snapshot);
			try {
				var streams = source.Streams.ToDictionary(s => s.Name);
				foreach (var name in source.DependencyOrder.SelectMany(s => s)) {
					var stream = streams[name];
					yield return await ReadStream(stream);
				}
			} finally {
				_tran.Dispose();
				_tran = null;
				await _conn.CloseAsync();
			}
		}

		private async Task<DataStream> ReadStream(StreamDefinition stream)
		{
			Console.WriteLine($"{DateTime.Now}: Reading table '{stream.Name}'");
			var recordSize = PayloadSizeAnalyzer.AverageSize(_conn, stream, SqlFormatter, _tran);
			Console.WriteLine($"{DateTime.Now}: Average data size: {recordSize}");
			Func<StreamDefinition, Task<IDataReader>> getReader = FullSyncReader;
			if (_incrementalPlan != null) {
				getReader = GetIncrementalStrategy(stream);
				if (getReader != FullSyncReader) {
					if (_incrementalPlan.TryGetValue(stream.Name, out var bookmark)) {
						_incrementalReaders.TryGetValue(stream.Name, out var incr);
						incr?.StartFrom(bookmark);
					} else {
						getReader = FullSyncReader;
					}
				}
			}
			var settings = StreamSettings.None;
			if (getReader == FullSyncReader) {
				if (stream.RareChangeFields?.Length > 0) {
					settings |= StreamSettings.UseRcf;
				}
			}
			return new DataStream(stream.Name, settings, await getReader(stream));
		}

		public void StreamDone(StreamDescription name)
		{
			_incrementalReaders.TryGetValue(name, out var incr);
			if (incr != null) {
				_incrementalPlan![name] = incr.CurrentPoint(name);
			}
		}

		public virtual Func<StreamDefinition, Task<IDataReader>> GetIncrementalStrategy(StreamDefinition stream)
			=> stream.AuditFieldIndex.HasValue
				? this.AuditReader
				: this.FullSyncReader;

		public void Dispose()
		{
			_conn.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
