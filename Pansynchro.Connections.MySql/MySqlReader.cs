using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.SQL;

using MySqlConnector;
using System.Data.Common;

namespace Pansynchro.Connectors.MySQL
{
    public class MySqlReader : IDbReader
    {
        private readonly MySqlConnection _conn;

        public MySqlReader(string connectionString)
        {
            _conn = new(connectionString);
        }

        DbConnection IDbReader.Conn => _conn;

        public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            await _conn.OpenAsync();
            try {
                var streams = source.Streams.ToDictionary(s => s.Name);
                foreach (var name in source.DependencyOrder.SelectMany(s => s)) {
                    var stream = streams[name];
                    Console.WriteLine($"{DateTime.Now}: Reading table '{stream.Name}'");
                    var recordSize = PayloadSizeAnalyzer.AverageSize(_conn, stream, MySqlFormatter.Instance);
                    Console.WriteLine($"{DateTime.Now}: Average data size: {recordSize}");
                    var columns = string.Join(", ", stream.NameList.Select(s => '`' + s + '`'));
                    var query = new MySqlCommand($"select {columns} from `{stream.Name.Name}`", _conn) { CommandTimeout = 0 };
                    yield return new DataStream(stream.Name, StreamSettings.None, await query.ExecuteReaderAsync());
                }
            } finally {
                await _conn.CloseAsync();
            }
        }

        public void SetIncrementalPlan(Dictionary<StreamDescription, string> plan)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
