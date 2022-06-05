using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.MSSQL.Core;
using Pansynchro.SQL;

namespace Pansynchro.MSSQL.Reader
{
    public class MSSQLReader : IReader
    {
        private readonly SqlConnection _conn;
        private readonly SqlConnection _perfConn;

        private const int BATCH_SIZE = 10_000;
        private const int REPORT_SIZE = 100_000;
        private const int CAP = 1_000_000;
        private const int STRATEGY_VERSION = 5;

        private IIncrementalReader _incrementalReader;
        private Func<StreamDefinition, Task<IDataReader>> _getReader;
        private Dictionary<StreamDescription, string> _incrementalPlan;

        public MSSQLReader(string connectionString, string perfConnectionString)
        {
            _conn = new SqlConnection(connectionString);
            if (perfConnectionString != null)
            {
                _perfConn = new SqlConnection(perfConnectionString);
            }
            _getReader = FullSyncReader;
        }

        private async Task<IDataReader> FullSyncReader(StreamDefinition stream)
        {
            var columns = string.Join(", ", stream.NameList.Select(s => '[' + s + ']'));
            var query = new SqlCommand($"select {columns} from [{stream.Name.Namespace}].[{stream.Name.Name}]", _conn) { CommandTimeout = 0 };
            return await query.ExecuteReaderAsync();
        }

        public void SetIncrementalPlan(Dictionary<StreamDescription, string> plan)
        {
            _incrementalPlan = plan;
        }

        public async IAsyncEnumerable<LiveStream> ReadFrom(DataDictionary source)
        {
            await _conn.OpenAsync();
            try {
                var streams = source.Streams.ToDictionary(s => s.Name);
                foreach (var name in source.DependencyOrder.SelectMany(s => s)) {
                    var stream = streams[name];
                    Console.WriteLine($"{DateTime.Now}: Reading table '{stream.Name}'");
                    var recordSize = PayloadSizeAnalyzer.AverageSize(_conn, stream, MssqlFormatter.Instance);
                    Console.WriteLine($"{DateTime.Now}: Average data size: {recordSize}");
                    if (_incrementalPlan != null) {
                        var strat = source.IncrementalStrategyFor(name);
                        if (strat == IncrementalStrategy.None) {
                            continue;
                        }
                        SetIncrementalStrategy(strat);
                        _incrementalPlan.TryGetValue(name, out var bookmark);
                        _incrementalReader.StartFrom(bookmark);
                    }
                    yield return new LiveStream(stream.Name, recordSize, await _getReader(stream));
                }
            } finally {
                await _conn.CloseAsync();
            }
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

        /*
        private static (Batch batch, bool done) ReadBatch(StreamDescription stream, SqlDataReader reader, Func<SqlDataReader, Field[]> extractor, ref ulong progress)
        {
            var records = new List<Field[]>(BATCH_SIZE);
            for (int i = 0; i < BATCH_SIZE; ++i)
            {
                if (reader.Read())
                {
                    records.Add(extractor(reader));
                }
                else break;
            }
            progress += (ulong)records.Count;
            var cookie = new Cookie { Name = stream.ToString(), Progress = progress };
            return (new Batch { Cookie = cookie, Data = records }, records.Count < BATCH_SIZE || progress >= CAP);
        }

        private static Func<SqlDataReader, Field[]> BuildExtractor(ReadOnlyCollection<DbColumn> schema)
        {
            var extractors = schema.Select((c, i) => BuildColumnExtractor(c, i)).ToArray();
            return reader => {
                var result = new Field[extractors.Length];
                for (int i = 0; i < extractors.Length; ++i)
                {
                    result[i] = extractors[i](reader);
                }
                return result;
            };
        }

        private static Func<SqlDataReader, Field> BuildColumnExtractor(DbColumn c, int i)
        {
            var nn = BuildNotNullColumnExtractor(c, i);
            return c.AllowDBNull == true ?
                (r => r.IsDBNull(i) ? new Field { which = Field.WHICH.Null } : nn(r)) : 
                nn;
        }

        private static Func<SqlDataReader, Field> BuildNotNullColumnExtractor(DbColumn c, int i)
        {
            if (c.DataType == typeof(int))
            {
                return r => new Field { which = Field.WHICH.Int, Int = r.GetInt32(i) };
            }
            if (c.DataType == typeof(long))
            {
                return r => new Field { which = Field.WHICH.Long, Long = r.GetInt64(i) };
            }
            if (c.DataType == typeof(float))
            {
                return r => new Field { which = Field.WHICH.Single, Single = r.GetFloat(i) };
            }
            if (c.DataType == typeof(double))
            {
                return r => new Field { which = Field.WHICH.Double, Double = r.GetDouble(i) };
            }
            if (c.DataType == typeof(bool))
            {
                return r => new Field { which = Field.WHICH.Bool, Bool = r.GetBoolean(i) };
            }
            if (c.DataType == typeof(string))
            {
                return r => new Field { which = Field.WHICH.String, String = r.GetString(i) };
            }
            if (c.DataType == typeof(byte[]))
            {
                return r => new Field { which = Field.WHICH.Blob, Blob = (byte[])r[i] };
            }
            if (c.DataType == typeof(System.Guid))
            {
                return r => new Field { which = Field.WHICH.Guid, Guid = new CapnpGen.Guid { Value = ((System.Guid)r[i]).ToByteArray() } };
            }
            if (c.DataType == typeof(decimal))
            {
                return r => {
                    var converter = new DecimalConverter(r.GetDecimal(i));
                    return new Field { Decimal = new CapnpGen.Decimal { High = converter.High, Low = converter.Low } };
                };
            }
            if (c.DataType == typeof(short))
            {
                return r => new Field { which = Field.WHICH.Short, Short = r.GetInt16(i) };
            }
            if (c.DataType == typeof(DateTime))
            {
                return r => new Field { which = Field.WHICH.DateTime, DateTime = r.GetDateTime(i).ToBinary() };
            }
            throw new ArgumentException($"Unsupported data type '{c.DataTypeName}'.");
        }
        */

        const string READ_DEPS =
@"select SchemaName as DependencySchema, TableName as Dependency,
         ReferenceSchemaName as DependentSchema, ReferenceTableName as Dependent from (
	SELECT f.name AS ForeignKey, OBJECT_NAME(f.parent_object_id) AS TableName,
		OBJECT_SCHEMA_NAME(f.parent_object_id) AS SchemaName,
		COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
		OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,
		OBJECT_SCHEMA_NAME(f.referenced_object_id) AS ReferenceSchemaName,
		COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName
	FROM sys.foreign_keys AS f
	INNER JOIN sys.foreign_key_columns AS fc
	ON f.OBJECT_ID = fc.constraint_object_id
) t
where t.TableName <> t.ReferenceTableName
order by TableName";

        DbConnectionStringBuilder IReader.Configurator => new SqlConnectionStringBuilder();

        public async Task<StreamDescription[]> ListStreams()
        {
            _conn.Open();
            _perfConn?.Open();
            try
            {
                var names = new List<StreamDescription>();
                var deps = new List<KeyValuePair<StreamDescription, StreamDescription>>();
                await foreach (var name in SqlHelper.ReadValuesAsync(_conn, "select SCHEMA_NAME(schema_id), name from sys.tables",
                    r => new StreamDescription(r.GetString(0), r.GetString(1))))
                {
                    names.Add(name);
                }
                await foreach (var pair in SqlHelper.ReadValuesAsync(_conn, READ_DEPS,
                    r => KeyValuePair.Create(
                        new StreamDescription(r.GetString(0), r.GetString(1)),
                        new StreamDescription(r.GetString(2), r.GetString(3)))))
                {
                    deps.Add(pair);
                }
                return OrderDeps(names, deps).Reverse().ToArray();
            } finally {
                _conn.Close();
            }
        }

        private static IEnumerable<StreamDescription> OrderDeps(
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
                foreach (var free in freeList)
                {
                    yield return free;
                    names.Remove(free);
                    deps.RemoveAll(p => p.Key == free);
                }
            }
        }

        public bool SetIncrementalStrategy(IncrementalStrategy strategy)
        {
            var result = false;
            switch (strategy)
            {
                case IncrementalStrategy.Cdc:
                    _incrementalReader = new MssqlCdcReader(_conn, BATCH_SIZE);
                    result = true;
                    break;
                default:
                    break;
            }
            _getReader = _incrementalReader != null ? _incrementalReader.ReadStreamAsync : this.FullSyncReader;
            return result;
        }

        public void Dispose()
        {
            _conn.Dispose();
            _perfConn?.Dispose();
            GC.SuppressFinalize(this);
        }

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterReader("MSSQL", cs => new MSSQLReader(cs, null));
        }
    }
}
