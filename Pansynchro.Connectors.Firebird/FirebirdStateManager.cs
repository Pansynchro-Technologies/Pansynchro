using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using FirebirdSql.Data.FirebirdClient;

using Pansynchro.Core;
using Pansynchro.State;

namespace Pansynchro.Connectors.Firebird
{
    public class FirebirdStateManager : StateManager, IDisposable
    {
        protected readonly FbConnection _conn;
        protected readonly int _connectionID;

        private const string CONN_STRING =
@"User=SYSDBA;Password=masterkey;Database={0};DataSource=localhost;Port=3050;Dialect=3;
ClientLibrary={1};ServerType=1;";
        private static string ClientLib(string name) =>
            Path.Combine(Path.GetDirectoryName(Path.GetFullPath(name))!, "Firebird", "fbclient.dll");

        public FirebirdStateManager(string name)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            _conn = new FbConnection(
                string.Format(CultureInfo.InvariantCulture, CONN_STRING, name, ClientLib(name))
            );
            _conn.Open();
        }

        public FirebirdStateManager(string name, string connectionName) : this(name)
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "select ID from SOURCES where NAME = @name";
            cmd.Prepare();
            cmd.Parameters.AddWithValue("name", connectionName);
            var result = cmd.ExecuteScalar();
            if (result is int id) {
                _connectionID = id;
            }
        }

        private const string SET_INC =
            "update or insert into INCREMENTAL_DATA (SOURCE_ID, STREAM_NAME, BOOKMARK) values (@id, @name, @bookmark)";

        public override void SaveIncrementalData(StreamDescription stream, string bookmark)
        {
            if (bookmark != null && _connectionID != 0) {
                using var cmd = (FbCommand)_conn.CreateCommand();
                cmd.CommandText = SET_INC;
                cmd.Prepare();
                cmd.Parameters.AddWithValue("id", _connectionID);
                cmd.Parameters.AddWithValue("name", stream.ToString());
                cmd.Parameters.AddWithValue("bookmark", bookmark);
                cmd.ExecuteNonQuery();
            }
        }

        public override Dictionary<StreamDescription, string> IncrementalDataFor()
        {
            if (_connectionID == 0) {
                return new();
            }
            using var cmd = (FbCommand)_conn.CreateCommand();
            cmd.CommandText = "select STREAM_NAME, BOOKMARK from INCREMENTAL_DATA where SOURCE_ID = @id";
            cmd.Parameters.AddWithValue("id", _connectionID);
            cmd.Prepare();
            using var results = cmd.ExecuteReader();
            return new(ReadIncrementalData(results));
        }

        public void Dispose()
        {
            _conn.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
