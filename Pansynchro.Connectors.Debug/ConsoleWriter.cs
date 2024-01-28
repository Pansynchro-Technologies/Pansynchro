using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Debug
{
	public class ConsoleWriter : IWriter
	{
		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			Console.OutputEncoding = System.Text.Encoding.UTF8;
			await foreach (var stream in streams) {
				var buffer = new object[stream.Reader.FieldCount];
				while (stream.Reader.Read()) {
					stream.Reader.GetValues(buffer);
					Console.WriteLine(buffer.Length > 1 ? string.Join(", ", buffer) : buffer[0]);
				}
			}
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
		}
	}
}
