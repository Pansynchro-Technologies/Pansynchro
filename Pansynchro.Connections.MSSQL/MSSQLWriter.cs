using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.SQL;
using Pansynchro.State;

namespace Pansynchro.Connectors.MSSQL
{
    public class MSSQLWriter : IIncrementalWriter
    {
        private readonly SqlConnection _conn;
        private readonly SqlConnection? _perfConn;

        private DataDictionary? _order;
        private StateManager _stateManager = null!;

        public MSSQLWriter(string connectionString, string? perfConnectionString)
        {
            _conn = new SqlConnection(connectionString);
            _conn.Open();
            if (perfConnectionString != null) {
                _perfConn = new SqlConnection(perfConnectionString);
            }
        }

        private void Setup(DataDictionary dest)
        {
            MetadataHelper.EnsureScratchTables(_conn, dest);
            _order = dest;
        }

        private void Finish()
        {
            foreach (var table in _order!.Streams) {
                Console.WriteLine($"{DateTime.Now}: Merging table '{table.Name}'");
                MetadataHelper.MergeTable(_conn, table.Name);
            }
            Console.WriteLine($"{DateTime.Now}: Truncating");
            foreach (var table in _order.Streams) {
                MetadataHelper.TruncateTable(_conn, table.Name);
            }
        }

        const SqlBulkCopyOptions COPY_OPTIONS = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction;

        public void Dispose()
        {
            _conn.Close();
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            Setup(dest);
            var order = new List<StreamDescription>();
            await foreach (var (name, settings, reader) in streams) {
                order.Add(name);
                if (reader is IncrementalDataReader inc) {
                    var bookmark = IncrementalSync(name, inc);
                    if (bookmark != null) {
                        _stateManager.SaveIncrementalData(name, bookmark);
                    }
                } else {
                    FullStreamSync(name, settings, reader);
                }
                reader.Dispose();
            }
            Finish();
        }

        private string? IncrementalSync(StreamDescription name, IncrementalDataReader inc)
        {
            var writers = BuildIncrementalWriters(name, inc);
            _conn.Execute($"set IDENTITY_INSERT {name} ON");
            var tran = _conn.BeginTransaction(IsolationLevel.Serializable);
            long count = 0;
            string? latestBookmark = null;
            try {
                while (inc.Read()) {
                    WriteIncrementalRow(name, inc, tran, writers);
                    if (++count % inc.BookmarkLength == 0) {
                        tran.Commit();
                        tran = _conn.BeginTransaction(IsolationLevel.Serializable);
                        latestBookmark = inc.Bookmark(count);
                    }
                }
            } catch {
                tran.Rollback();
            }
            tran.Commit();
            _conn.Execute($"set IDENTITY_INSERT {name} OFF");
            latestBookmark = inc.Bookmark(count);
            return latestBookmark;
        }

        private void WriteIncrementalRow(StreamDescription name, IncrementalDataReader inc, SqlTransaction tran, Action<List<string>, SqlParameterCollection>[] writers)
        {
            List<string> names; SqlCommand cmd;
            switch (inc.UpdateType) {
                case UpdateRowType.Insert:
                    var template = $"insert into [{name.Namespace}].[{name.Name}] ({{0}}) values ({{1}})";
                    (names, cmd) = RunIncrementalWritersForInsert(inc, tran, writers);
                    cmd.CommandText = string.Format(template, string.Join(", ", names), string.Join(", ", names.Select(n => '@' + n)));
                    break;
                case UpdateRowType.Update:
                    template = $"update [{name.Namespace}].[{name.Name}] set {{0}} where {{1}}";
                    (names, cmd) = RunIncrementalWritersForUpdate(inc, tran, writers);
                    cmd.CommandText = string.Format(template, string.Join(", ", names.Select(n => $"{n} = @{n}")), BuildWhereClause(name, inc, cmd.Parameters));
                    break;
                case UpdateRowType.Delete:
                    template = $"delete from [{name.Namespace}].[{name.Name}] where {{0}}";
                    cmd = new SqlCommand(null, _conn, tran);
                    cmd.CommandText = string.Format(template, BuildWhereClause(name, inc, cmd.Parameters));
                    break;
                default:
                    throw new Exception($"Invalid update type: {inc.UpdateType}.");
            }
            cmd.ExecuteNonQuery();
        }

        private object BuildWhereClause(StreamDescription name, IncrementalDataReader inc, SqlParameterCollection parameters)
        {
            var schema = _order!.GetStream(name.ToString());
            var id = schema.Identity;
            foreach (var field in id) {
                parameters.AddWithValue($"__{field}_where", inc[field]);
            }
            var result = string.Join(" and ", id.Select(f => $"[{f}] = @__{f}_where"));
            return result;
        }

        private (List<string> names, SqlCommand cmd) RunIncrementalWritersForInsert(IncrementalDataReader inc, SqlTransaction tran, Action<List<string>, SqlParameterCollection>[] writers)
        {
            var names = new List<string>();
            var cmd = new SqlCommand(null, _conn, tran);
            for (int i = 0; i < writers.Length; ++i) {
                writers[i](names, cmd.Parameters);
            }
            return (names, cmd);
        }

        private (List<string> names, SqlCommand cmd) RunIncrementalWritersForUpdate(IncrementalDataReader inc, SqlTransaction tran, Action<List<string>, SqlParameterCollection>[] writers)
        {
            var names = new List<string>();
            var cmd = new SqlCommand(null, _conn, tran);
            foreach (var column in inc.AffectedColumns) {
                writers[column](names, cmd.Parameters);
            }
            return (names, cmd);
        }

        private static Action<List<string>, SqlParameterCollection>[] BuildIncrementalWriters(StreamDescription name, IncrementalDataReader inc)
        {
            var result = new Action<List<string>, SqlParameterCollection>[inc.FieldCount];
            for (int i = 0; i < inc.FieldCount; ++i) {
                result[i] = BuildWriter(i, inc);
            }
            return result;
        }

        private static Action<List<string>, SqlParameterCollection> BuildWriter(int i, IncrementalDataReader inc) =>
            (l, p) => {
                var name = inc.GetName(i);
                l.Add(name);
                p.AddWithValue(name, inc[i]);
            };

        private void FullStreamSync(StreamDescription name, StreamSettings settings, IDataReader reader)
        {
            ulong progress = 0;
            MetadataHelper.TruncateTable(_conn, name);
            using var copy = new SqlBulkCopy(_conn, COPY_OPTIONS, null) {
                BatchSize = BATCH_SIZE,
                DestinationTableName = $"Pansynchro.[{name.Name}]",
                EnableStreaming = true,
                NotifyAfter = BATCH_SIZE,
            };
            BuildColumnMapping(reader, copy.ColumnMappings);
            copy.SqlRowsCopied += (s, e) => progress = (ulong)e.RowsCopied;
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            copy.WriteToServer(reader);
            stopwatch.Stop();
            //LogThroughput(name, progress, averageSize, stopwatch);
        }

        private static void BuildColumnMapping(IDataReader reader, SqlBulkCopyColumnMappingCollection map)
        {
            for (int i = 0; i < reader.FieldCount; ++i) {
                var name = reader.GetName(i);
                var column = new SqlBulkCopyColumnMapping(i, name);
                map.Add(column);
            }
        }

        const int BATCH_SIZE = 100_000;
        const int STRATEGY_VERSION = 6;

        Dictionary<StreamDescription, string> IIncrementalWriter.IncrementalData => _stateManager.IncrementalDataFor();

        void IIncrementalWriter.SetSourceName(string name)
        {
            _stateManager = StateManager.Create(name);
        }

        private void LogThroughput(StreamDescription stream, ulong progress, int recordSize, Stopwatch timer)
        {
            var throughput = progress / timer.Elapsed.TotalSeconds;
            Console.WriteLine($"{DateTime.Now}: Stream '{stream}' complete.  Throughput: {(int)throughput} elements per second, {(int)(throughput * recordSize)} bytes per second.");
            using var cmd = new SqlCommand(
                $"insert into perf (STREAM_NAME, DATA_SIZE, BATCH_SIZE, RUN_SIZE, COMPLETION_TIME_MS, VERSION) values ('{stream}', {recordSize}, {BATCH_SIZE}, {progress}, {Math.Max(1, timer.ElapsedMilliseconds)}, {STRATEGY_VERSION})",
                _perfConn);
            cmd.ExecuteNonQuery();
        }
    }
}
