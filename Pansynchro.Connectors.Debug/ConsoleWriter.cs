using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Errors;
using Pansynchro.Core.EventsSystem;

namespace Pansynchro.Connectors.Debug
{
	public class ConsoleWriter : IWriter
	{
		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			EventLog.Instance.AddStartSyncEvent();
			Console.OutputEncoding = System.Text.Encoding.UTF8;
			await foreach (var stream in streams) {
				EventLog.Instance.AddStartSyncStreamEvent(stream.Name);
				var buffer = new object[stream.Reader.FieldCount];
				try {
					while (stream.Reader.Read()) {
						stream.Reader.GetValues(buffer);
						Console.WriteLine(buffer.Length > 1 ? string.Join(", ", buffer) : buffer[0]);
					}
				} catch (Exception ex) {
					EventLog.Instance.AddErrorEvent(ex, stream.Name);
					if (!ErrorManager.ContinueOnError)
						throw;
				}
				EventLog.Instance.AddEndSyncStreamEvent(stream.Name);
			}
			EventLog.Instance.AddEndSyncEvent();
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
		}
	}
}