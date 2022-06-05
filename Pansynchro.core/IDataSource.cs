using System.Collections.Generic;
using System.IO;

namespace Pansynchro.Core
{
    public interface IDataSource
    {
        IAsyncEnumerable<(string name, Stream data)> GetDataAsync();
        IAsyncEnumerable<(string name, TextReader data)> GetTextAsync();
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
