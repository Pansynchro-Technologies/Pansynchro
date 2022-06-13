using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core
{
    public interface IReader : IDisposable
    {
        IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source);
        void SetIncrementalPlan(Dictionary<StreamDescription, string> plan);
        Task<Exception?> TestConnection();
    }

    public interface IRandomStreamReader
    {
        Task<IDataReader> ReadStream(DataDictionary source, string name);
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
