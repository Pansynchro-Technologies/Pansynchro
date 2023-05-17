using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using ExcelDataReader;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Excel
{
    public class ExcelReader : IReader, ISourcedConnector
    {
        private readonly string _conf;
        private IDataSource? _source;

        public ExcelReader(string configuration)
        {
            _conf = configuration;
        }

        public async IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling ReadFrom");
            }
            await foreach (var (name, stream) in _source.GetDataAsync()) {
                using var excelReader = ExcelReaderFactory.CreateReader(stream);
                do {
                    yield return new DataStream(new(name, excelReader.CodeName), StreamSettings.None, new ExcelReaderWrapper(excelReader));
                } while (excelReader.NextResult());
            }
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
            }
            catch (Exception e) {
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
