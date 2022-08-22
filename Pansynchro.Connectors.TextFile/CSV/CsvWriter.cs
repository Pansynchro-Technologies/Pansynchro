using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ChoETL;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.CSV
{
    public class CsvWriter : IWriter, ISinkConnector
    {
        private IDataSink? _sink;

        public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
        {
            if (_sink == null) {
                throw new DataException("Must call SetDataSink before calling Sync");
            }
            await foreach (var (name, settings, stream) in streams) {
                try {
                    using var tw = await _sink!.WriteText(name.ToString());
                    using var writer = CreateWriter(tw);
                    ChoCSVWriter.Write(writer, stream);
                } finally {
                    stream.Dispose();
                }
            }
        }

        private static ChoCSVWriter CreateWriter(TextWriter tw)
        {
            var result = (ChoCSVWriter) new ChoCSVWriter(tw)
                .WithFirstLineHeader();
            result.Configuration.EscapeQuoteAndDelimiter = true;
            return result;
        }

        public void SetDataSink(IDataSink sink)
        {
            _sink = sink;
        }

        public void Dispose()
        {
            (_sink as IDisposable)?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
