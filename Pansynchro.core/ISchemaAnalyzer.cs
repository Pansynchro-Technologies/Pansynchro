using System;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core
{
	public interface ISchemaAnalyzer
	{
		ValueTask<DataDictionary> AnalyzeAsync(string name);

		Task<DataDictionary> Optimize(DataDictionary dict, Action<string> report)
		{
			return Task.FromResult(dict);
		}
	}

	public interface IQueryableSchemaAnalyzer : ISchemaAnalyzer
	{
		ValueTask<DataDictionary> AnalyzeAsync(string name, string[]? streamNames);
		ValueTask<DataDictionary> AnalyzeExcludingAsync(string name, string[] excludedNames);
		ValueTask<DataDictionary> AddCustomTables(DataDictionary input, params (StreamDescription name, string query)[] tables);
	}
}
