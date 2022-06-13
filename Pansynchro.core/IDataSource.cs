using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Pansynchro.Core
{
    public interface IDataSource
    {
        IAsyncEnumerable<(string name, Stream data)> GetDataAsync();
        IAsyncEnumerable<(string name, TextReader data)> GetTextAsync();
        IAsyncEnumerable<Stream> GetDataAsync(string name);
        IAsyncEnumerable<TextReader> GetTextAsync(string name);
    }

    public interface ISourcedConnector
    {
        void SetDataSource(IDataSource source);
    }

    public interface IDataSink
    {
        TextWriter WriteText(string streamName);
        Stream WriteBinary(string streamName);
    }

    public interface ISinkConnector
    {
        void SetDataSink(IDataSink sink);
    }
}
