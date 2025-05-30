using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;

namespace Pansynchro.Connectors.TextFile.Lines
{
	class TextLinesAnalyzer : ISchemaAnalyzer, ISourcedConnector
	{
		private readonly string _config;
		private IDataSource? _source;

		public TextLinesAnalyzer(string config)
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
			await foreach (var (sName, stream) in _source.GetTextAsync()) {
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
				new FieldDefinition("Name", new BasicField(TypeTag.Nvarchar, false, "255", false)),
				new FieldDefinition("Line", new BasicField(TypeTag.Int, false, null, false)),
				new FieldDefinition("Value", new BasicField(TypeTag.Ntext, false, null, false)),
			};
			return new StreamDefinition(new StreamDescription(null, sName), fields, Array.Empty<string>());
		}

		public void SetDataSource(IDataSource source)
		{
			_source = source;
		}
	}
}
