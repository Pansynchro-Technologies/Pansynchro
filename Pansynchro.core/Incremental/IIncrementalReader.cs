using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core.Incremental
{
    public interface IIncrementalReader
    {
        Task<IDataReader> ReadStreamAsync(StreamDefinition stream);
        IncrementalStrategy Strategy { get; }
        void StartFrom(string? bookmark);
        string CurrentBookmark { get; }
    }
}
