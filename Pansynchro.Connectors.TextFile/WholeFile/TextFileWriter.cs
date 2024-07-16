using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;

namespace Pansynchro.Connectors.TextFile.WholeFile
{
	public class TextFileWriter : IWriter, ISinkConnector
	{
		private IDataSink? _sink;

		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			if (_sink == null) {
				throw new DataException("Must call SetDataSink before calling Sync");
			}
			EventLog.Instance.AddStartSyncEvent();
			await foreach (var (name, settings, stream) in streams) {
				EventLog.Instance.AddStartSyncStreamEvent(name);
				try {
					using var writer = await _sink.WriteText(name.ToString());
					while (stream.Read()) {
						writer.Write(stream.GetString(stream.GetOrdinal("Value")));
					}
				} catch (Exception ex) {
					EventLog.Instance.AddErrorEvent(ex, name);
					if (!ErrorManager.ContinueOnError)
						throw;
				} finally {
					stream.Dispose();
				}
				EventLog.Instance.AddEndSyncStreamEvent(name);
			}
			EventLog.Instance.AddEndSyncEvent();
		}

		public void SetDataSink(IDataSink sink) => _sink = sink;

		public void Dispose()
		{
			(_sink as IDisposable)?.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
