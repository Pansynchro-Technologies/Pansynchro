using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.CSV
{
	public class CsvAnalyzer : ISchemaAnalyzer, ISourcedConnector
	{
		private readonly CsvConfigurator _config;
		private IDataSource? _source;

		public CsvAnalyzer(string config)
		{
			_config = new(config);
		}

		public async ValueTask<DataDictionary> AnalyzeAsync(string name)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling AnalyzeAsync");
			}
			var defs = new List<StreamDefinition>();
			string? lastName = null;
			await foreach (var (sName, stream) in _source.GetTextAsync()) {
				try {
					if (lastName != sName) {
						defs.Add(AnalyzeFile(sName, stream));
						lastName = sName;
					}
				} finally {
					stream.Dispose();
				}
			}
			return new DataDictionary(name, defs.ToArray());
		}

		private StreamDefinition AnalyzeFile(string name, TextReader stream)
		{
			using var csvReader = new CsvArrayProducer(stream, _config);
			return AnalyzerHelper.Analyze(name, csvReader);
		}

		public void SetDataSource(IDataSource source)
		{
			_source = source;
		}
	}
}
