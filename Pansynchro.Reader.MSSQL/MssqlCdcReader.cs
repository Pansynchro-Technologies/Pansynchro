using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Incremental;
using Pansynchro.SQL;

namespace Pansynchro.MSSQL.Reader
{
    class MssqlCdcReader : IIncrementalReader
    {
        private readonly SqlConnection _conn;
        private readonly int _bookmarkLength;
        private byte[] _startingPoint;
        private readonly byte[] _endingPoint;

        public MssqlCdcReader(SqlConnection conn, int bookmarkLength)
        {
            _conn = conn;
            _bookmarkLength = bookmarkLength;
            _endingPoint = SqlHelper.ReadValues(_conn,
                    "select sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE())",
                    ReadBytes)
                .Single();
        }

        private static byte[] ReadBytes(IDataReader r) => (byte[])r.GetValue(0);

        public IncrementalStrategy Strategy => IncrementalStrategy.Cdc;

        public string CurrentBookmark => throw new NotImplementedException();

        private const string CDC_FIELD_MAPPING =
@"SELECT column_name, column_ordinal - 1
  FROM cdc.captured_columns cc
  join cdc.change_tables ct on cc.object_id = ct.object_id
 where ct.capture_instance = @name
 order by column_ordinal";

        async Task<IDataReader> IIncrementalReader.ReadStreamAsync(StreamDefinition stream)
        {
            var name = stream.Name;
            var tableName = SqlHelper.ReadValues(_conn, 
                    $"EXECUTE sys.sp_cdc_help_change_data_capture @source_schema = N'{name.Namespace}', @source_name = N'{name.Name}';",
                    r => r.GetString(2))
                .First();
            var fnName = "cdc.fn_cdc_get_net_changes_" + tableName;
            if (_startingPoint == null)
            {
                _startingPoint = SqlHelper.ReadValues(_conn,
                        $"select sys.fn_cdc_get_min_lsn('{tableName}')",
                        ReadBytes)
                    .Single();
            }
            var fieldMappings = SqlHelper.ReadValues(
                    _conn,
                    CDC_FIELD_MAPPING,
                    r => KeyValuePair.Create(r.GetString(0), r.GetInt32(1)),
                    new KeyValuePair<string, object>("name", tableName))
                .ToArray();
            var fieldMap = BuildFieldMap(fieldMappings);
            var names = string.Join(", ", stream.NameList.OrderBy(n => n).Select(s => '[' + s + ']'));
            var query = $"select __$start_lsn, __$operation, __$update_mask, {names} from {fnName}(@start, @end, 'all with mask')";
            using var cmd = new SqlCommand(query, _conn);
            await cmd.PrepareAsync();
            cmd.Parameters.AddWithValue("start", _startingPoint);
            cmd.Parameters.AddWithValue("end", _endingPoint);
            var reader = await cmd.ExecuteReaderAsync();
            return new MssqlCdcDataReader(reader, _bookmarkLength, fieldMap);
        }

        private static string[] BuildFieldMap(KeyValuePair<string, int>[] fieldMappings)
        {
            var max = fieldMappings.Max(kvp => kvp.Value);
            var result = new string[max + 1];
            foreach (var pair in fieldMappings)
            {
                result[pair.Value] = pair.Key;
            }
            return result;
        }

        private static byte[] HexToBytes(string value)
        {
            var len = value.Length;
            var result = new byte[len / 2];
            for (int i = 0; i < len; i += 2)
                result[i / 2] = Convert.ToByte(value.Substring(i, 2), 16);
            return result;
        }

        public void StartFrom(string bookmark)
        {
            if (bookmark != null)
            { 
                var bytes = HexToBytes(bookmark);
                _startingPoint = SqlHelper.ReadValues(_conn,
                        "select sys.fn_cdc_increment_lsn(@bytes)",
                        ReadBytes,
                        new KeyValuePair<string, object>("bytes", bytes))
                    .Single();
            }
        }
    }
}
