using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

using ChoETL;
using Pansynchro.Core.Readers;

namespace Pansynchro.Connectors.TextFile.CSV
{
    public class CsvReader : IReader, ISourcedConnector, IRandomStreamReader
    {
        private readonly string _conf;
        private IDataSource? _source;

        public CsvReader(string configuration)
        {
            _conf = configuration;
        }

        public IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling ReadFrom");
            }
            return DataStream.CombineStreamsByName(Impl());

            async IAsyncEnumerable<DataStream> Impl()
            {
                await foreach (var (name, reader) in _source.GetTextAsync()) {
                    var csvReader = CreateReader(reader);
                    try {
                        yield return new DataStream(new(null, name), StreamSettings.None, csvReader.AsDataReader());
                    } finally {
                        csvReader.Dispose();
                    }
                }
            }
        }

        public Task<IDataReader> ReadStream(DataDictionary source, string name)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling ReadStream");
            }
            var readers = _source.GetTextAsync(name).Select(r => CreateReader(r).AsDataReader()).ToEnumerable();
            return Task.FromResult<IDataReader>(new GroupingReader(readers));
        }

        private ChoCSVReader<dynamic> CreateReader(TextReader reader)
        {
            var result = new ChoCSVReader(reader).WithMaxScanRows(10);
            var configurator = new CsvConfigurator(_conf);
            if (configurator.UsesHeader) {
                result = result.WithFirstLineHeader(true);
            }
            if (configurator.AutoDetectDelimiter) {
                result = result.AutoDetectDelimiter(true);
            }
            if (configurator.UsesQuotes) { 
                result = result.QuoteAllFields(true);
            }
            return result;
        }

        public void SetDataSource(IDataSource source)
        {
            _source = source;
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
