using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.WholeFile
{
    class TextFileReader : IReader, ISourcedConnector
    {
        private IDataSource? _source;
        private readonly string _config;

        public TextFileReader(string config)
        {
            _config = config;
        }

        IAsyncEnumerable<DataStream> IReader.ReadFrom(DataDictionary source)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling ReadFrom");
            }
            return DataStream.CombineStreamsByName(Impl);

            async IAsyncEnumerable<DataStream> Impl() { 
                await foreach (var (name, reader) in _source.GetTextAsync()) {
                    try {
                        yield return new DataStream(new(null, name), StreamSettings.None, new WholeFileReader(name, await reader.ReadToEndAsync()));
                    } finally {
                        reader.Dispose();
                    }
                }
            }
        }

        void ISourcedConnector.SetDataSource(IDataSource source) => _source = source;

        void IReader.SetIncrementalPlan(Dictionary<StreamDescription, string> plan)
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

        void IDisposable.Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }

    internal class WholeFileReader : ArrayReader
    {
        private static readonly string[] NAMES = new[] { "Name", "Value" };

        public WholeFileReader(string name, string value)
        {
            _buffer = new object[2] { name, value };
        }

        private bool _readOnce;

        public override int RecordsAffected => 1;

        public override bool Read()
        {
            var result = !_readOnce;
            _readOnce = true;
            return result;
        }

        public override string GetName(int i) => NAMES[i];

        public override int GetOrdinal(string name) => Array.IndexOf(NAMES, name);

        public override void Dispose()
        { }
    }
}
