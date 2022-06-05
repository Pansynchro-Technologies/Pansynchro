using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

using ChoETL;
using System.Data;

namespace Pansynchro.Connectors.TextFile.CSV
{
    public class CsvReader : IReader, ISourcedConnector
    {
        private readonly string _conf;
        private IDataSource? _source;

        public CsvReader(string configuration)
        {
            _conf = configuration;
        }

        public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling ReadFrom");
            }
            await foreach (var (name, stream) in _source.GetDataAsync()) {
                var csvReader = CreateReader(stream);
                try {
                    yield return new DataStream(new(null, name), StreamSettings.None, csvReader.AsDataReader());
                } finally {
                    csvReader.Dispose();
                }
            }
        }

        private ChoCSVReader<dynamic> CreateReader(Stream stream)
        {
            var result = new ChoCSVReader(stream).WithMaxScanRows(10);
            var configurator = new CsvConfigurator(_conf);
            if (configurator.UsesHeader)
            {
                result = result.WithFirstLineHeader(true);
            }
            if (configurator.AutoDetectDelimiter)
            {
                result = result.AutoDetectDelimiter(true);
            }
            if (configurator.UsesQuotes)
            { 
                result = result.QuoteAllFields(true);
            }
            return result;
        }

        public void SetDataSource(IDataSource source)
        {
            _source = source;
        }

        public void SetIncrementalPlan(Dictionary<StreamDescription, string> plan)
        {
            throw new NotImplementedException();
        }

        async Task<Exception?> IReader.TestConnection()
        {
            if (_source == null) {
                return new Exception("No data source has been set.");
            }
            try {
                await foreach (var (name, stream) in _source.GetDataAsync()) {
                    break;
                }
            } catch (Exception e) {
                return e;
            }
            return null;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
