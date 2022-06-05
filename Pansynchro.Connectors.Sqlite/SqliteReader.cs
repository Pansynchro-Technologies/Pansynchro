using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Sqlite
{
    public class SqliteReader : IDbReader
    {
        private readonly SqliteConnection _conn;

        public SqliteReader(string connectionString)
        {
            _conn = new SqliteConnection(connectionString);
        }

        DbConnection IDbReader.Conn => _conn;

        public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            await _conn.OpenAsync();
            try
            {
                var streams = source.Streams.ToDictionary(s => s.Name);
                foreach (var name in source.DependencyOrder.SelectMany(s => s))
                {
                    var stream = streams[name];
                    Console.WriteLine($"{DateTime.Now}: Reading table '{stream.Name}'");
                    var columns = string.Join(", ", stream.NameList.Select(s => '"' + s + '"'));
                    var query = new SqliteCommand($"select {columns} from \"{stream.Name.Name}\"", _conn) { CommandTimeout = 0 };
                    yield return new DataStream(stream.Name, StreamSettings.None, await query.ExecuteReaderAsync());
                }
            }
            finally
            {
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
