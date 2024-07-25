using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;

namespace Pansynchro.Core
{
	public interface IReader : IDisposable
	{
		IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source);
		Task<Exception?> TestConnection();
	}

	public interface IIncrementalReader
	{
		void SetIncrementalPlan(Dictionary<StreamDescription, string> plan);
	}

	public interface IRandomStreamReader
	{
		Task<IDataReader> ReadStream(DataDictionary source, string name);
		async Task<IDataReader> ReadStream(DataDictionary source, string name, int maxResults)
		{
			var result = await ReadStream(source, name);
			return new MaxSizeReader(result, maxResults);
		}
	}

	public interface IDbReader : IReader
	{
		DbConnection Conn { get; }
		async Task<Exception?> IReader.TestConnection()
		{
			try {
				await Conn.OpenAsync();
				await Conn.CloseAsync();
			} catch (Exception e) {
				return e;
			}
			return null;
		}
	}
}
