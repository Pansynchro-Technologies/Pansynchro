using System;
using System.Collections.Generic;
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
}
