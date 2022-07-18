using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

using Pansynchro.Core;

namespace Pansynchro.Sources.Compression
{
    public class GzipCompression: IDataInputProcessor, IDataOutputProcessor
    {
        private IDataSource? _source;

        public void SetDataSource(IDataSource source) => _source = source;

        public async IAsyncEnumerable<(string name, Stream data)> GetDataAsync()
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling GetDataAsync");
            }
            await foreach (var (name, data) in _source.GetDataAsync()) {
                yield return (name, new GZipStream(data, CompressionMode.Decompress, false));
            }
        }

        public async IAsyncEnumerable<Stream> GetDataAsync(string name)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling GetDataAsync");
            }
            await foreach (var data in _source.GetDataAsync(name)) {
                yield return new GZipStream(data, CompressionMode.Decompress, false);
            }
        }

        public async IAsyncEnumerable<(string name, TextReader data)> GetTextAsync()
        {
            await foreach (var (name, stream) in GetDataAsync()) {
                yield return (name, new StreamReader(stream));
            }
        }

        public async IAsyncEnumerable<TextReader> GetTextAsync(string name)
        {
            await foreach (var stream in GetDataAsync(name)) {
                yield return new StreamReader(stream);
            }
        }

        private IDataSink? _sink;

        public void SetDataSink(IDataSink sink) => _sink = sink;

        public async Task<Stream> WriteData(string streamName)
        {
            if (_sink == null) {
                throw new DataException("Must call SetDataSink before calling WriteData");
            }
            var stream = await _sink.WriteData(streamName);
            var result = new GZipStream(stream, CompressionLevel.Optimal, false);
            return result;
        }
    }
}