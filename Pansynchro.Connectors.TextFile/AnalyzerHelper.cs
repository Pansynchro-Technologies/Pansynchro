using System;
using System.Collections.Generic;
using System.Data;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile
{
    internal static class AnalyzerHelper
    {
        internal static StreamDefinition Analyze(string streamName, IDataReader reader)
        {
            var fields = new List<FieldDefinition>();
            for (int i = 0; i < reader.FieldCount; ++i)
            {
                var typ = reader.GetDataTypeName(i);
                var name = reader.GetName(i);
                fields.Add(MakeField(typ, name));
            }
            return new StreamDefinition(new(null, streamName), fields.ToArray(), Array.Empty<string>());
        }

        private static readonly Dictionary<string, TypeTag> _matches = new()
        {
            { "Int32", TypeTag.Int },
            { "Int64", TypeTag.Long },
            { "String", TypeTag.Nvarchar },
            { "DateTime", TypeTag.DateTime },
            { "Decimal", TypeTag.Decimal },
            { "Guid", TypeTag.Guid },
            { "Double", TypeTag.Double },
            { "Char", TypeTag.Char },
        };

        private static FieldDefinition MakeField(string typ, string name)
        {
            var tag = _matches[typ];
            return new FieldDefinition(name, new FieldType(tag, true, CollectionType.None, null));
        }
    }
}