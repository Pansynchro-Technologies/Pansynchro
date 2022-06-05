using System;
using System.Collections.Generic;
using System.Data;
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
                var writer = CreateWriter(name.ToString());
                try {
                    writer.Write(stream);
                } finally {
                    writer.Dispose();
                    stream.Dispose();
                }
            }
        }

        private ChoCSVWriter CreateWriter(string name)
        {
            return (ChoCSVWriter) new ChoCSVWriter(_sink!.WriteText(name))
                .WithFirstLineHeader()
                .QuoteAllFields(false);
        }

        public void SetDataSink(IDataSink sink)
        {
            _sink = sink;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
