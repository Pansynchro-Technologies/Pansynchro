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
	public abstract class SqlSchemaAnalyzer : IQueryableSchemaAnalyzer, IDisposable
	{
		protected readonly DbConnection _conn;

		/// <summary>Query for all columns in all tables.  Should return schema and table name,
		/// column name, and enough info to construct the column's full type.  No computed columns.
		protected abstract string ColumnsQuery { get; }
		protected abstract string PkQuery { get; }
		protected abstract ISqlFormatter Formatter { get; }
		protected string DatabaseName => Conn.Database;

		public DbConnection Conn => _conn;

		protected SqlSchemaAnalyzer(DbConnection conn)
		{
			_conn = conn;
		}

		public ValueTask<DataDictionary> AnalyzeAsync(string name) => AnalyzeAsync(name, null);

		public async ValueTask<DataDictionary> AnalyzeAsync(string name, string[]? tableNames)
		{
			await _conn.OpenAsync();
			try {
				var types = await LoadCustomTypes();
				var tables = await BuildStreamDefinitions(tableNames);
				var dependencies = await BuildStreamDependencies();
				if (tableNames != null && tableNames.Length > 0) {
					dependencies = TrimDependencies(dependencies, tableNames);
				}
				return new DataDictionary(name, tables, dependencies, types);
			} finally {
				await _conn.CloseAsync();
			}
		}

		public async ValueTask<DataDictionary> AnalyzeExcludingAsync(string name, string[] excludedNames)
		{
			await _conn.OpenAsync();
			try {
				var types = await LoadCustomTypes();
				var en = excludedNames.ToHashSet(StringComparer.InvariantCultureIgnoreCase);
				var tables = (await BuildStreamDefinitions()).Where(sd => !en.Contains(sd.Name.ToString())).ToArray();
				var initialDependencies = await BuildStreamDependencies();
				var dependencies = TrimDependencies(initialDependencies, tables.Select(t => t.Name.ToString()).ToArray());
				return new DataDictionary(name, tables, dependencies, types);
			} finally {
				await _conn.CloseAsync();
			}
		}

		private static StreamDescription[][] TrimDependencies(StreamDescription[][] dependencies, string[] tableNames)
		{
			var filter = tableNames.ToHashSet(StringComparer.InvariantCultureIgnoreCase);
			return dependencies
				.Select(a => SqlSchemaAnalyzer.TrimDependencies(a, filter))
				.Where(a => a.Length > 0)
				.ToArray();
		}

		private static StreamDescription[] TrimDependencies(StreamDescription[] dependencies, HashSet<string> filter)
			=> dependencies
				.Where(d => filter.Contains(d.ToString().ToUpperInvariant()))
				.ToArray();

		public async ValueTask<DataDictionary> AddCustomTables(DataDictionary input, params (StreamDescription name, string query)[] tables)
		{
			var streams = input.Streams.ToList();
			foreach (var (name, query) in tables) {
				streams.Add(await AnalyzeCustomTable(name, query));
			}
			return input with { Streams = streams.ToArray() };
		}

		private async ValueTask<StreamDefinition> AnalyzeCustomTable(
			StreamDescription name,
			string query)
		{
			await _conn.OpenAsync();
			try {
				var lQuery = Formatter.LimitRows(query, 1);
				using var cmd = _conn.CreateCommand();
				cmd.CommandText = lQuery;
				using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.KeyInfo);
				return AnalyzeCustomTableSchema(name, reader) with { CustomQuery = query };
			} finally {
				await _conn.CloseAsync();
			}
		}

		private StreamDefinition AnalyzeCustomTableSchema(StreamDescription name, IDataReader reader) 
			=> new StreamDefinition(name, AnalyzeCustomTableFields(reader), Array.Empty<string>());

		protected abstract FieldDefinition[] AnalyzeCustomTableFields(IDataReader reader);

		protected abstract Task<StreamDescription[][]> BuildStreamDependencies();

		protected virtual async Task<StreamDefinition[]> BuildStreamDefinitions(string[]? tables)
		{
			var whole = await BuildStreamDefinitions();
			if (tables == null || tables.Length == 0) {
				return whole;
			}
			var filter = tables.Select(t => t.ToUpperInvariant()).ToHashSet();
			return whole
				.Where(t => filter.Contains(t.Name.ToString().ToUpperInvariant()))
				.ToArray();
		}

		protected virtual async Task<StreamDefinition[]> BuildStreamDefinitions()
		{
			var fields = await SqlHelper.ReadValuesAsync(_conn, ColumnsQuery, BuildFieldDefinition)
				.ToLookupAsync(pair => pair.table, pair => pair.column);
			var pks = await GetPKs(fields.Select(g => g.Key));
			return fields
				.Select(g => new StreamDefinition(g.Key, g.ToArray(), pks.Contains(g.Key) ? pks[g.Key].ToArray() : Array.Empty<string>()))
				.ToArray();
		}

		protected virtual async Task<ILookup<StreamDescription, string>> GetPKs(
			IEnumerable<StreamDescription> tables)
		{
			return await SqlHelper.ReadValuesAsync(_conn, PkQuery, BuildPkDefintion)
				.ToLookupAsync(pair => pair.table, pair => pair.column);
		}

		protected virtual Task<Dictionary<string, FieldType>> LoadCustomTypes() => Task.FromResult(new Dictionary<string, FieldType>());

		protected abstract (StreamDescription table, FieldDefinition column) BuildFieldDefinition(IDataReader reader);

		protected abstract (StreamDescription table, string column) BuildPkDefintion(IDataReader reader);

		protected static IEnumerable<StreamDescription[]> OrderDeps(
			List<StreamDescription> names, List<KeyValuePair<StreamDescription, StreamDescription>> deps)
		{
			deps.RemoveAll(p => p.Key == p.Value); //remove any hierarchical tables with FK to self
			while (names.Count > 0) {
				var uncounted = names.Count;
				var freeList = names.Except(deps.Select(p => p.Value)).ToArray();
				if (freeList.Length == 0) {
					throw new Exception($"Circular references found in {names.Count} tables.  Unresolved dependencies: ({string.Join(", ", deps.Select(d => $"{d.Key}->{d.Value}"))})");
				}
				yield return freeList;
				foreach (var free in freeList) {
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

		private static int? GetSeqId(StreamDefinition stream)
		{
			if (stream.Identity.Length != 1)
				return null;
			var id = stream.Fields.Single(f => f.Name == stream.Identity[0]);
			if (id.Type.Nullable)
				return null;
			return id.Type.Type is TypeTag.Int or TypeTag.Long ? Array.IndexOf(stream.Fields, id) : null;
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
				result[i] = KeyValuePair.Create(
					reader.GetName(i),
					reader.IsDBNull(i) ? 0 : reader.GetInt32(i));
			}
			return result;
		}

		private KeyValuePair<string, long>[] DomainShiftSelectorDT(IDataReader reader)
		{
			var result = new KeyValuePair<string, long>[reader.FieldCount];
			for (int i = 0; i < reader.FieldCount; ++i) {
				result[i] = KeyValuePair.Create(
					reader.GetName(i),
					reader.IsDBNull(i) ? 0 : reader.GetDateTime(i).Ticks);
			}
			return result;
		}

		private KeyValuePair<string, long>[] DomainShiftSelectorTZ(IDataReader reader)
		{
			var result = new KeyValuePair<string, long>[reader.FieldCount];
			for (int i = 0; i < reader.FieldCount; ++i) {
				result[i] = KeyValuePair.Create(
					reader.GetName(i), 
					reader.IsDBNull(i) ? 0 : ((DateTimeOffset)reader.GetValue(i)).Ticks);
			}
			return result;
		}

		protected abstract string GetDistinctCountQuery(string fieldList, string tableName, long threshold);

		private const long LARGE_TABLE_THRESHOLD = 100_000L;

		private bool CountOverThreshold(StreamDefinition stream) 
		{
			return SqlHelper.ReadValue<long>(_conn, GetTableRowCount(stream.Name)) >= LARGE_TABLE_THRESHOLD;
		}

		protected virtual string GetTableRowCount(StreamDescription name) => $"select count(*) from {Formatter.QuoteName(name)}";

		public void Dispose()
		{
			_conn.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
