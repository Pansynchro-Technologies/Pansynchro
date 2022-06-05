using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Core
{
    public interface ITransformer
    {
        IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input);
    }

    public abstract class StreamTransformerBase : ITransformer
    {
        protected Dictionary<string, Func<IDataReader, IEnumerable<object[]>>> _streamDict
            = new(System.StringComparer.InvariantCultureIgnoreCase);
        protected Dictionary<StreamDescription, StreamDescription> _nameMap = new();

        public async IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input)
        {
            await foreach (var stream in input) {
                DataStream result;
                Console.WriteLine($"Processing Stream: {stream.Name}");
                if (_streamDict.TryGetValue(stream.Name.ToString(), out var processor)) {
                    Console.WriteLine("Stream found");
                    result = stream.Transformed(processor);
                }
                else {
                    Console.WriteLine("Stream not found");
                    result = stream;
                }
                _nameMap.TryGetValue(stream.Name, out var destName);
                yield return destName != null ? result with { Name = destName } : result;
            }
        }
    }
}
