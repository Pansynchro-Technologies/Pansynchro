using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.Core.Readers;

namespace Pansynchro.Core
{
    [Flags]
    public enum StreamSettings
    {
        None = 0,
        UseRcf = 1,
        Incremental = 2,
    }

    public record DataStream(StreamDescription Name, StreamSettings Settings, IDataReader Reader) : IDisposable
    { 
        public DataStream Transformed(Func<IDataReader, IEnumerable<object[]>> transformer, StreamDefinition stream)
        {
            return this with { Reader = new TransformingReader(Reader, transformer, stream) };
        }

        public static async IAsyncEnumerable<DataStream> CombineStreamsByName(IAsyncEnumerable<DataStream> source)
        {
            await foreach (var group in source.LazyGroupAdjacent(s => s.Name)) {
                yield return (await group.FirstAsync()) with {
                    Reader = new GroupingReader(group.Select(g => g.Reader).ToEnumerable())
                };
            }
        }

        public void Dispose()
        {
            Reader.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
