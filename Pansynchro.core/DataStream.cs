using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Pansynchro.Core.Helpers;

namespace Pansynchro.Core
{
    [Flags]
    public enum StreamSettings
    {
        None = 0,
        UseRcf = 1,
        Incremental = 2,
    }

    public record DataStream(StreamDescription Name, StreamSettings Settings, IDataReader Reader)
    { 
        public DataStream Transformed(Func<IDataReader, IEnumerable<object[]>> transformer)
        {
            return this with { Reader = new TransformingReader(Reader, transformer) };
        }

        public static async IAsyncEnumerable<DataStream> CombineStreamsByName(Func<IAsyncEnumerable<DataStream>> source)
        {
            await foreach (var group in source().LazyGroupAdjacent(s => s.Name)) {
                yield return (await group.FirstAsync())with {
                    Reader = new GroupingReader(group.Select(g => g.Reader).ToEnumerable())
                };
            }
        }
    }
}
