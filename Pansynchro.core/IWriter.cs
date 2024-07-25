using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core
{
	public interface IWriter : IDisposable
	{
		Task Sync(IAsyncEnumerable<DataStream> streams, DataDictionary dest);
	}

	public interface IIncrementalWriter : IWriter
	{
		void SetSourceName(string name);
		void MergeIncrementalData(Dictionary<StreamDescription, string>? data);

		Dictionary<StreamDescription, string> IncrementalData { get; }
	}
}
