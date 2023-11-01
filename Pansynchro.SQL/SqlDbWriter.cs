using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.State;

namespace Pansynchro.SQL
{
    public abstract class SqlDbWriter : IIncrementalWriter
    {
        protected readonly DbConnection _conn;
        private StateManager _stateManager = null!;
        private DataDictionary? _dict;

        protected SqlDbWriter(DbConnection conn)
        {
            _conn = conn;
        }

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            Setup(dest);
            await _conn.OpenAsync();
            try {
                await foreach (var (name, settings, reader) in streams) {
                    try { 
                        if (reader is IncrementalDataReader inc) {
                            var bookmark = IncrementalSync(name, inc);
                            if (bookmark != null) {
                                _stateManager.SaveIncrementalData(name, bookmark);
                            }
                        } else {
                            FullStreamSync(name, settings, reader);
                        }
                    } finally {
                        reader.Dispose();
                    }
                }
                await Finish();
            } finally {
                await _conn.CloseAsync();
            }
        }

        private string? IncrementalSync(StreamDescription name, IncrementalDataReader inc)
        {
            var writers = BuildIncrementalWriters(name, inc);
            BeginIncrementalSync(name);
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
                throw;
            }
            tran.Commit();
            FinishIncrementalSync(name);
            latestBookmark = inc.Bookmark(count);
            return latestBookmark;
        }

        protected virtual void BeginIncrementalSync(StreamDescription name) { }

        protected virtual void FinishIncrementalSync(StreamDescription name) { }

        private static Action<List<string>, DbParameterCollection>[] BuildIncrementalWriters(StreamDescription name, IncrementalDataReader inc)
        {
            var result = new Action<List<string>, DbParameterCollection>[inc.FieldCount];
            for (int i = 0; i < inc.FieldCount; ++i) {
                result[i] = BuildWriter(i, inc);
            }
            return result;
        }

        private static Action<List<string>, DbParameterCollection> BuildWriter(int i, IncrementalDataReader inc) =>
            (l, p) => {
                var name = inc.GetName(i);
                l.Add(name);
                var idx = p.Add(inc[i]);
                p[idx].ParameterName = name;
            };

        private void WriteIncrementalRow(
            StreamDescription name, IncrementalDataReader inc, DbTransaction tran, 
            Action<List<string>, DbParameterCollection>[] writers)
        {
            List<string> names; DbCommand cmd;
            switch (inc.UpdateType) {
                case UpdateRowType.Insert:
                    var template = $"insert into {Formatter.QuoteName(name)} ({{0}}) values ({{1}})";
                    (names, cmd) = RunIncrementalWritersForInsert(inc, tran, writers);
                    cmd.CommandText = string.Format(template, string.Join(", ", names), string.Join(", ", names.Select(n => '@' + n)));
                    break;
                case UpdateRowType.Update:
                    template = $"update {Formatter.QuoteName(name)} set {{0}} where {{1}}";
                    (names, cmd) = RunIncrementalWritersForUpdate(inc, tran, writers);
                    cmd.CommandText = string.Format(template, string.Join(", ", names.Select(n => $"{n} = @{n}")), BuildWhereClause(name, inc, cmd.Parameters));
                    break;
                case UpdateRowType.Delete:
                    template = $"delete from {Formatter.QuoteName(name)} where {{0}}";
                    cmd = _conn.CreateCommand();
                    cmd.Transaction = tran;
                    cmd.CommandText = string.Format(template, BuildWhereClause(name, inc, cmd.Parameters));
                    break;
                default:
                    throw new Exception($"Invalid update type: {inc.UpdateType}.");
            }
            cmd.ExecuteNonQuery();
            cmd.Dispose();
        }

        private string BuildWhereClause(
            StreamDescription name, IncrementalDataReader inc, DbParameterCollection parameters)
        {
            var schema = _dict!.GetStream(name.ToString());
            var id = schema.Identity;
            foreach (var field in id) {
                parameters[parameters.Add(inc[field])].ParameterName = $"__{field}_where";
            }
            var result = string.Join(" and ", id.Select(f => $"{Formatter.QuoteName(f)} = @__{f}_where"));
            return result;
        }

        private (List<string> names, DbCommand cmd) RunIncrementalWritersForInsert(
            IncrementalDataReader inc, DbTransaction tran, Action<List<string>, DbParameterCollection>[] writers)
        {
            var names = new List<string>();
            var cmd = _conn.CreateCommand();
            cmd.Transaction = tran;
            for (int i = 0; i < writers.Length; ++i) {
                writers[i](names, cmd.Parameters);
            }
            return (names, cmd);
        }

        private (List<string> names, DbCommand cmd) RunIncrementalWritersForUpdate(
            IncrementalDataReader inc, DbTransaction tran, Action<List<string>, DbParameterCollection>[] writers)
        {
            var names = new List<string>();
            var cmd = _conn.CreateCommand();
            cmd.Transaction = tran;
            foreach (var column in inc.AffectedColumns) {
                writers[column](names, cmd.Parameters);
            }
            return (names, cmd);
        }

        protected abstract ISqlFormatter Formatter { get; }

        protected virtual void Setup(DataDictionary dest)
        {
            _dict = dest;
        }

        protected abstract void FullStreamSync(StreamDescription name, StreamSettings settings, IDataReader reader);

        protected virtual Task Finish() => Task.CompletedTask;

        public Dictionary<StreamDescription, string> IncrementalData => _stateManager.IncrementalDataFor();

        public void SetSourceName(string name)
        {
            _stateManager = StateManager.Create(name);
        }

        void IIncrementalWriter.MergeIncrementalData(Dictionary<StreamDescription, string>? data)
        {
            if (data != null) {
                _stateManager?.MergeIncrementalData(data);
            }
        }

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
