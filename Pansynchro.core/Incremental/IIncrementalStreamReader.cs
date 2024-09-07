using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core.Incremental
{
	public interface IIncrementalStreamReader
	{
		Task<IDataReader> ReadStreamAsync(StreamDefinition stream);
		IncrementalStrategy Strategy { get; }
		void StartFrom(string? bookmark);
		string CurrentPoint(StreamDescription name);
	}

	public interface IFullCdcStreamReader : IIncrementalStreamReader
	{
		Task<IDataReader> ReadChangesAsync(IEnumerable<StreamDefinition> streams);
	}
}
