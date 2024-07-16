using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using FirebirdSql.Data.FirebirdClient;
using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;

namespace Pansynchro.Connectors.Firebird
{
	public class FirebirdWriter : IWriter
	{
		private readonly FbConnection _conn;

		private DataDictionary _schema = null!;

		public FirebirdWriter(string connectionString)
		{
			Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
			_conn = new(connectionString);
			_conn.Open();
		}

		private const int BATCH_SIZE = 1000;

		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			_schema = dest;
			EventLog.Instance.AddStartSyncEvent();
			await foreach (var (name, averageSize, reader) in streams) {
				EventLog.Instance.AddStartSyncStreamEvent(name);
				try {
					var lineReader = BuildReader(name);
					ulong progress = 0;
					var finished = false;
					while (!finished) {
						var tran = _conn.BeginTransaction();
						for (int i = 0; i < BATCH_SIZE; ++i) {
							++progress;
							if (!reader.Read()) {
								finished = true;
								break;
							}
							lineReader(reader, tran);
						}
						tran.Commit();
					}
				} catch (Exception ex) {
					EventLog.Instance.AddErrorEvent(ex, name);
					if (!ErrorManager.ContinueOnError)
						throw;
				} finally {
					reader.Dispose();
				}
				EventLog.Instance.AddEndSyncStreamEvent(name);
			}
			EventLog.Instance.AddEndSyncEvent();
		}

		private Action<IDataReader, FbTransaction> BuildReader(StreamDescription name)
		{
			var schema = _schema.GetStream(name.ToString());
			var fieldNames = schema.NameList;
			var fieldList = string.Join(", ", fieldNames.Select(FirebirdFormatter.Instance.QuoteName));
			var argsList = string.Join(", ", fieldNames.Select(n => '@' + n));
			var sql = $"insert into {FirebirdFormatter.Instance.QuoteName(schema.Name.Name)} ({fieldList}) values ({argsList})";
			var values = new object[fieldNames.Length];
			return (r, t) => {
				using var command = new FbCommand(sql, _conn, t);
				command.Prepare();
				r.GetValues(values);
				for (int i = 0; i < fieldNames.Length; ++i) {
					command.Parameters.AddWithValue(fieldNames[i], values[i]);
				}
				command.ExecuteNonQuery();
			};
		}

		public void Dispose()
		{
			_conn.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
