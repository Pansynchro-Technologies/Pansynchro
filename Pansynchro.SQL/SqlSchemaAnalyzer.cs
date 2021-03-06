using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;

namespace Pansynchro.SQL
{
    public abstract class SqlSchemaAnalyzer : ISchemaAnalyzer, IDisposable
    {
        protected readonly DbConnection _conn;

        /// <summary>Query for all columns in all tables.  Should return schema and table name,
        /// column name, and enough info to construct the column's full type.  No computed columns.
        protected abstract string ColumnsQuery { get; }
        protected abstract string PkQuery { get; }
        protected string DatabaseName { get; private set; } = "";

        public DbConnection Conn => _conn;

        protected SqlSchemaAnalyzer(DbConnection conn)
        {
            _conn = conn;
        }

        public async ValueTask<DataDictionary> AnalyzeAsync(string name)
        {
            await _conn.OpenAsync();
            try {
                DatabaseName = name;
                var types = await LoadCustomTypes();
                var tables = await BuildStreamDefinitions();
                var dependencies = await BuildStreamDependencies();
                var incremental = await LoadIncrementalData();
                return new DataDictionary(name, tables, dependencies, types, incremental);
            } finally {
                await _conn.CloseAsync();
            }
        }

        protected virtual Task<Dictionary<StreamDescription, IncrementalStrategy>> LoadIncrementalData()
        {
            return Task.FromResult(new Dictionary<StreamDescription, IncrementalStrategy>());
        }

        protected abstract Task<StreamDescription[][]> BuildStreamDependencies();

        private async Task<StreamDefinition[]> BuildStreamDefinitions()
        {
            var fields = await SqlHelper.ReadValuesAsync(_conn, ColumnsQuery, BuildFieldDefinition)
                .ToLookupAsync(pair => pair.table, pair => pair.column);
            var pks = await SqlHelper.ReadValuesAsync(_conn, PkQuery, BuildPkDefintion)
                .ToLookupAsync(pair => pair.table, pair => pair.column);
            return fields
                .Select(g => new StreamDefinition(g.Key, g.ToArray(), pks.Contains(g.Key) ? pks[g.Key].ToArray() : Array.Empty<string>()))
                .ToArray();
        }

        protected virtual Task<Dictionary<string, FieldType>> LoadCustomTypes() => Task.FromResult(new Dictionary<string, FieldType>());

        protected abstract (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader);

        protected abstract (StreamDescription table, string column) BuildPkDefintion(IDataReader reader);

        protected virtual CollectionType GetCollType(string v)
        {
            return CollectionType.None;
        }

        protected abstract TypeTag GetTagType(string v);

        protected static IEnumerable<StreamDescription[]> OrderDeps(
            List<StreamDescription> names, List<KeyValuePair<StreamDescription, StreamDescription>> deps)
        {
            while (names.Count > 0)
            {
                var uncounted = names.Count;
                var freeList = names.Except(deps.Select(p => p.Value)).ToArray();
                if (freeList.Length == 0)
                {
                    throw new Exception($"Circular references found in {freeList.Length} tables.");
                }
                yield return freeList;
                foreach (var free in freeList)
                {
                    names.Remove(free);
                    deps.RemoveAll(p => p.Key == free);
                }
            }
        }

        async Task<DataDictionary> ISchemaAnalyzer.Optimize(DataDictionary dict, Action<string> report)
        {
            await _conn.OpenAsync();
            try {
                var streams = new List<StreamDefinition>();
                foreach (var stream in dict.Streams) {
                    report?.Invoke(stream.Name.ToString());
                    if (!CountOverThreshold(stream)) {
                        streams.Add(stream);
                        continue;
                    }
                    string[] rcfs = await ExtractRcfFields(stream);
                    var drs = await ExtractDomainShifts(stream);
                    var sid = GetSeqId(stream);
                    streams.Add(stream.WithOptimizations(rcfs, drs, sid));
                }
                return dict with { Streams = streams.ToArray() };
            } finally {
                await _conn.CloseAsync();
            }
        }

        private int GetSeqId(StreamDefinition stream)
        {
            if (stream.Identity.Length != 1)
                return -1;
            var id = stream.Fields.Single(f => f.Name == stream.Identity[0]);
            if (id.Type.Nullable)
                return -1;
            return id.Type.Type is TypeTag.Int or TypeTag.Long ? Array.IndexOf(stream.Fields, id) : -1;
        }

        private async Task<KeyValuePair<string, long>[]> ExtractDomainShifts(StreamDefinition stream) =>
            (await ExtractDomainShiftsDT(stream)).Concat(await ExtractDomainShiftsTZ(stream)).ToArray();

        private async Task<KeyValuePair<string, long>[]> ExtractDomainShiftsDT(StreamDefinition stream)
        {
            var dateFields = stream.Fields
                .Where(f => f.Type.Type is TypeTag.Date or TypeTag.DateTime or TypeTag.SmallDateTime)
                .ToArray();
            if (dateFields.Length == 0) {
                return Array.Empty<KeyValuePair<string, long>>();
            }
            var fieldList = string.Join(", ", dateFields.Select(f => $"min({f.Name}) as {f.Name}"));
            var query = $"select {fieldList} from {stream.Name}";
            var results = await SqlHelper.ReadValuesAsync(_conn, query, DomainShiftSelectorDT).SingleAsync();
            return results;
        }

        private async Task<KeyValuePair<string, long>[]> ExtractDomainShiftsTZ(StreamDefinition stream)
        {
            var dateFields = stream.Fields.Where(f => f.Type.Type == TypeTag.DateTimeTZ).ToArray();
            if (dateFields.Length == 0) {
                return Array.Empty<KeyValuePair<string, long>>();
            }
            var fieldList = string.Join(", ", dateFields.Select(f => $"min({f.Name}) as {f.Name}"));
            var query = $"select {fieldList} from {stream.Name}";
            var results = await SqlHelper.ReadValuesAsync(_conn, query, DomainShiftSelectorTZ).SingleAsync();
            return results;
        }

        private async Task<string[]> ExtractRcfFields(StreamDefinition stream)
        {
            var fieldList = string.Join(", ", stream.Fields.Select(f => $"count(distinct {f.Name}) as {f.Name}"));
            var query = GetDistinctCountQuery(fieldList, stream.Name.ToString(), LARGE_TABLE_THRESHOLD);
            var results = await SqlHelper.ReadValuesAsync(_conn, query, DistinctQuerySelector).SingleAsync();
            var winners = results.Where(kvp => kvp.Value < (LARGE_TABLE_THRESHOLD / 1000))
                .OrderBy(kvp => kvp.Value)
                .Select(kvp => kvp.Key)
                .ToArray();
            return winners;
        }

        private KeyValuePair<string, int>[] DistinctQuerySelector(IDataReader reader)
        {
            var result = new KeyValuePair<string, int>[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; ++i) {
                result[i] = KeyValuePair.Create(reader.GetName(i), (int)reader.GetValue(i));
            }
            return result;
        }

        private KeyValuePair<string, long>[] DomainShiftSelectorDT(IDataReader reader)
        {
            var result = new KeyValuePair<string, long>[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; ++i) {
                result[i] = KeyValuePair.Create(reader.GetName(i), reader.GetDateTime(i).Ticks);
            }
            return result;
        }

        private KeyValuePair<string, long>[] DomainShiftSelectorTZ(IDataReader reader)
        {
            var result = new KeyValuePair<string, long>[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; ++i) {
                result[i] = KeyValuePair.Create(reader.GetName(i), ((DateTimeOffset)reader.GetValue(i)).Ticks);
            }
            return result;
        }

        protected abstract string GetDistinctCountQuery(string fieldList, string tableName, long threshold);

        private const long LARGE_TABLE_THRESHOLD = 100_000L;

        private bool CountOverThreshold(StreamDefinition stream) 
        {
            return SqlHelper.ReadValue<long>(_conn, GetTableRowCount(stream.Name)) >= LARGE_TABLE_THRESHOLD;
        }

        protected virtual string GetTableRowCount(StreamDescription name) => $"select count(*) from {name}";

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
