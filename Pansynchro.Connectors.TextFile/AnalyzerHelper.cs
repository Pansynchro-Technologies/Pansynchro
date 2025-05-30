using System;
using System.Collections.Generic;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;

namespace Pansynchro.Connectors.TextFile
{
	internal static class AnalyzerHelper
	{
		internal static StreamDefinition Analyze(string streamName, CSV.CsvArrayProducer reader)
		{
			var fields = new List<FieldDefinition>();
			for (int i = 0; i < reader.FieldTypes.Length; ++i) {
				var typ = reader.FieldTypes[i];
				var name = reader.Names[i];
				fields.Add(MakeField(typ.type.Name, typ.nullable, name));
			}
			return new StreamDefinition(new(null, streamName), fields.ToArray(), Array.Empty<string>());
		}

		private static readonly Dictionary<string, TypeTag> _matches = new() {
			{ "Int32", TypeTag.Int },
			{ "Int64", TypeTag.Long },
			{ "String", TypeTag.Nvarchar },
			{ "DateTime", TypeTag.DateTime },
			{ "Decimal", TypeTag.Decimal },
			{ "Guid", TypeTag.Guid },
			{ "Double", TypeTag.Double },
			{ "Char", TypeTag.Char },
		};

		private static FieldDefinition MakeField(string typ, bool nullable, string name)
		{
			var tag = _matches[typ];
			return new FieldDefinition(name, new BasicField(tag, nullable, null, false));
		}
	}
}