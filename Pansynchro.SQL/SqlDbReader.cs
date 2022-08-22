using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.SQL;

namespace Pansynchro.SQL
{
    public abstract class SqlDbReader : IDbReader, IRandomStreamReader, IIncrementalReader
    {
        protected readonly DbConnection _conn;
        public DbConnection Conn => _conn;

        protected IIncrementalStreamReader? _incrementalReader;
        protected Func<StreamDefinition, Task<IDataReader>> _getReader;
        private Dictionary<StreamDescription, string>? _incrementalPlan;

        public SqlDbReader(string connectionString)
        {
            _conn = CreateConnection(connectionString);
            _getReader = FullSyncReader;
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
            var sql = $"select {columns} from {formatter.QuoteName(stream.Name)}";
            if (rcf?.Length > 0) {
                var order = stream.Identity?.Length > 0 ? rcf.Concat(stream.Identity.Except(rcf)) : rcf;
                sql = $"{sql} order by {string.Join(", ", order.Select(formatter.QuoteName))}";
            }
            if (maxRows >= 0) {
                sql = QueryWithMaxRows(sql, maxRows);
            }
            var query = _conn.CreateCommand();
            query.CommandText = sql;
            query.CommandTimeout = 0;
            return await query.ExecuteReaderAsync();
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

        protected virtual string QueryWithMaxRows(string sql, int maxRows)
            => $"{sql} LIMIT {maxRows}";

        public async Task<IDataReader> ReadStream(DataDictionary source, string name, int maxResults)
        {
            var stream = source.GetStream(name);
            if (_conn.State == ConnectionState.Closed) {
                await _conn.OpenAsync();
            }
            return await FullSyncReader(stream, maxResults);
        }

        public Task<IDataReader> ReadStream(DataDictionary source, string name)
            => ReadStream(source, name, -1);

        public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            await _conn.OpenAsync();
            try {
                var streams = source.Streams.ToDictionary(s => s.Name);
                foreach (var name in source.DependencyOrder.SelectMany(s => s)) {
                    var stream = streams[name];
                    Console.WriteLine($"{DateTime.Now}: Reading table '{stream.Name}'");
                    var recordSize = PayloadSizeAnalyzer.AverageSize(_conn, stream, SqlFormatter);
                    Console.WriteLine($"{DateTime.Now}: Average data size: {recordSize}");
                    if (_incrementalPlan != null) {
                        var strat = source.IncrementalStrategyFor(name);
                        if (strat == IncrementalStrategy.None) {
                            continue;
                        }
                        SetIncrementalStrategy(strat);
                        _incrementalPlan.TryGetValue(name, out var bookmark);
                        _incrementalReader!.StartFrom(bookmark);
                    }
                    var settings = StreamSettings.None;
                    if (_getReader == FullSyncReader) {
                        if (stream.RareChangeFields?.Length > 0) {
                            settings |= StreamSettings.UseRcf;
                        }
                    }
                    yield return new DataStream(stream.Name, settings, await _getReader(stream));
                }
            } finally {
                await _conn.CloseAsync();
            }
        }

        public virtual bool SetIncrementalStrategy(IncrementalStrategy strategy)
        {
            _getReader = this.FullSyncReader;
            return false;
        }

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
