using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;

using ChoETL;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.CSV
{
    public class CsvAnalyzer : ISchemaAnalyzer
    {
        private readonly string _config;
        private IDataSource? _source;

        public CsvAnalyzer(string config)
        {
            _config = config;
        }

        public async ValueTask<DataDictionary> AnalyzeAsync(string name)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
            }
            var defs = new List<StreamDefinition>();
            string? lastName = null;
            await foreach (var (sName, stream) in _source.GetDataAsync()) {
                try {
                    if (lastName != sName) {
                        defs.Add(AnalyzeFile(sName, stream));
                        lastName = sName;
                    }
                } finally {
                    stream.Dispose();
                }
            }
            return new DataDictionary(name, defs.ToArray());
        }

        private static StreamDefinition AnalyzeFile(string name, Stream stream)
        {
            using var csvReader = new ChoCSVReader(stream)
                .WithMaxScanRows(10).WithFirstLineHeader(false).AutoDetectDelimiter(true)
                .QuoteAllFields(true);
            csvReader.Configuration.FileHeaderConfiguration.IgnoreColumnsWithEmptyHeader = true;
            var reader = csvReader.AsDataReader();
            return AnalyzerHelper.Analyze(name, reader);
        }

        public void SetDataSource(IDataSource source)
        {
            _source = source;
        }
    }
}
