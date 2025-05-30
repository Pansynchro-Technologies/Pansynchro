using Pansynchro.Connectors.TextFile.JSON;
using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Connectors.TextFile.HTML;
internal class HtmlAnalyzer : ISchemaAnalyzer, ISourcedConnector
{
	private HtmlConfigurator _config;

	public HtmlAnalyzer(string config) => _config = new(config);

	public ValueTask<DataDictionary> AnalyzeAsync(string name)
	{
		var streams = _config.Streams.SelectMany(BuildStream);
		return ValueTask.FromResult(new DataDictionary("HTML", streams.ToArray()));
	}

	private IEnumerable<StreamDefinition> BuildStream(HtmlConfigurator.HtmlConf query)
	{
		var ns = query.Name;
		foreach (var stream in query.Streams) {
			yield return BuildStream(ns, stream);
		}
	}

	private StreamDefinition BuildStream(string ns, HtmlConfigurator.HtmlQuery query)
	{
		var name = new StreamDescription(ns, query.Name);
		if (query.Type != HtmlConfigurator.DataType.Expressions) {
			return new StreamDefinition(name, [new("Value", new BasicField(TypeTag.Text, true, null, false))], []);
		}
		var fields = query.Expressions?.Select(BuildField).ToArray();
		if (!(fields?.Length > 0)) {
			throw new ValidationException($"Stream '{query.Name}' is defined as Expressions type, but has no expressions listed.");
		}
		return new StreamDefinition(name, fields, []);
	}

	private FieldDefinition BuildField(HtmlConfigurator.ExpressionQuery query)
		=> new FieldDefinition(query.Name, new BasicField(TypeTag.Text, true, null, false));

	public void SetDataSource(IDataSource source)
	{ }
}
