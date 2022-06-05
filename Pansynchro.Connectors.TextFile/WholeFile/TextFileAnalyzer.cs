using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.WholeFile
{
    public class TextFileAnalyzer : ISchemaAnalyzer
    {
        private readonly string _config;
        private IDataSource? _source;

        public TextFileAnalyzer(string config)
        {
            _config = config;
        }

        async ValueTask<DataDictionary> ISchemaAnalyzer.AnalyzeAsync(string name)
        {
            if (_source == null) {
                throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
            }
            var defs = new List<StreamDefinition>();
            string? lastName = null;
            await foreach (var (sName, stream) in _source.GetDataAsync()) {
                try {
                    if (lastName != sName) {
                        defs.Add(AnalyzeFile(sName));
                        lastName = sName;
                    }
                } finally {
                    stream.Dispose();
                }
            }
            return new DataDictionary(name, defs.ToArray());
        }

        private static StreamDefinition AnalyzeFile(string sName)
        {
            var fields = new FieldDefinition[] { 
                new FieldDefinition("Name", new FieldType(TypeTag.Nvarchar, false, CollectionType.None, "255")),
                new FieldDefinition("Value", new FieldType(TypeTag.Ntext, false, CollectionType.None, null)),
            };
            return new StreamDefinition(new StreamDescription(null, sName), fields, Array.Empty<string>());
        }

        public void SetDataSource(IDataSource source)
        {
            _source = source;
        }
    }
}
