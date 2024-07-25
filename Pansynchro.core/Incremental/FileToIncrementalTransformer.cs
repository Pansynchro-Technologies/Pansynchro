using System.Collections.Generic;

namespace Pansynchro.Core.Incremental
{
	// The inverse of IncrementalToFileTransformer.  Transforms a saved incremental data stream
	// into a normal incremental data stream, for purposes of loading data saved to a file
	// and syncing it to a database.
	public class FileToIncrementalTransformer : ITransformer
	{
		private readonly int _bl;

		public FileToIncrementalTransformer(int bookmarkLength)
		{
			_bl = bookmarkLength;
		}

		public async IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input)
		{
			await foreach (var stream in input) {
				var reader = stream.Reader;
				if (GenericIncrementalDataReader.IsIncrementalData(reader)) {
					yield return stream with { Reader = new GenericIncrementalDataReader(reader, _bl) };
				} else {
					yield return stream;
				}
			}
		}
	}
}
