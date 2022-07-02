using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.Lines
{
    internal class TextLinesWriter : IWriter, ISinkConnector
    {
        private IDataSink? _sink;

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            if (_sink == null) {
                throw new DataException("Must call SetDataSink before calling Sync");
            }
            await foreach (var (name, settings, stream) in streams) {
                try {
                    using var writer = await _sink.WriteText(name.ToString());
                    while (stream.Read()) {
                        writer.WriteLine(stream.GetString(stream.GetOrdinal("Value")));
                    }
                } finally {
                    stream.Dispose();
                }
            }
        }

        public void SetDataSink(IDataSink sink) => _sink = sink;

        public void Dispose()
        {
            (_sink as IDisposable)?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
