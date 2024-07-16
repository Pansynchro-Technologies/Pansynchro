using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.State;

namespace Pansynchro.Core.Incremental
{
	// Transforms an incremental data stream into a standard data stream, for purposes of saving
	// the data to a file-based IWriter rather than sending it directly to a database.
	public class IncrementalToFileTransformer : IIncrementalWriter
	{
		private StateManager? _state;
		public Dictionary<StreamDescription, string> IncrementalData
			=> _state?.IncrementalDataFor() ?? throw new DataException("Must call SetSourceName before retrieving IncrementalData");

		private readonly IWriter _writer;
		private readonly HashSet<StreamDescription> _streams;

		public IncrementalToFileTransformer(IWriter writer, IEnumerable<StreamDescription> streams)
		{
			_writer = writer;
			_streams = streams.ToHashSet();
		}

		public async Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest)
		{
			var iDict = IncrementalHelper.IncrementalDictFor(dest, _streams);
			await _writer.Sync(Transform(streams), iDict);
		}

		private async IAsyncEnumerable<DataStream> Transform(IAsyncEnumerable<DataStream> input)
		{
			await foreach (var stream in input) {
				var reader = stream.Reader;
				if (reader is IncrementalDataReader idr) {
					yield return stream with { Reader = new GenericIncrementalDataAdapter(idr) };
				} else {
					yield return stream;
				}
			}
		}

		public void SetSourceName(string name)
		{
			_state = StateManager.Create(name);
		}

		void IIncrementalWriter.MergeIncrementalData(Dictionary<StreamDescription, string>? data)
		{
			if (data != null) {
				_state?.MergeIncrementalData(data);
			}
		}

		public void Dispose()
		{
			_writer.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
