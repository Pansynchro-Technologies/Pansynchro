using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Pansynchro.Core.Readers;
using Pansynchro.Core.Streams;

namespace Pansynchro.Core.Transformations
{
	public class SizePartitionTransformer : ITransformer
	{
		private readonly Func<StreamDescription, Task<MeteredStream>> _getStream;
		private readonly long _sizeLimit;

		public SizePartitionTransformer(Func<StreamDescription, Task<MeteredStream>> getStream, long sizeLimit)
		{
			_getStream = getStream;
			_sizeLimit = sizeLimit;
		}

		public async IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input)
		{
			await foreach (var (name, settings, reader) in input) {
				var finished = false;
				while (!finished) {
					var spReader = new SizePartitionReader(reader, await _getStream(name), _sizeLimit);
					yield return new DataStream(name, settings, spReader);
					finished = spReader.Finished;
				}
			}
		}
	}
}
