using System;
using System.Collections.Generic;
using System.Data;

using Pansynchro.Core;

namespace Pansynchro.State
{
    public abstract class StateManager
    {
        public StateManager()
        { }

        public abstract Dictionary<StreamDescription, string> IncrementalDataFor();

        public abstract void SaveIncrementalData(StreamDescription stream, string bookmark);

        protected static IEnumerable<KeyValuePair<StreamDescription, string>> ReadIncrementalData(IDataReader arg)
        {
            while (arg.Read()) {
                var name = arg.GetString(0);
                yield return new(StreamDescription.Parse(name), arg.GetString(1));
            }
        }

        public static Func<string, StateManager>? Factory { get; set; }

        public static StateManager Create(string name)
        {
            if (Factory == null) {
                throw new DataException("StateManager.Create can't be called before StateManager.Factory has been set");
            }
            return Factory(name);
        }

        public abstract void MergeIncrementalData(Dictionary<StreamDescription, string> data);
    }
}
