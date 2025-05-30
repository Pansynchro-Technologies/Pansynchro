using HtmlAgilityPack;
using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;
using Pansynchro.Core.Transformations;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Connectors.TextFile.HTML;
public class HtmlReader : IReader, ISourcedConnector
{
	private IDataSource? _source;
	private string _config;

	public HtmlReader(string config)
	{
		_config = config;
	}

	public IAsyncEnumerable<DataStream> ReadFrom(DataDictionary source)
	{
		if (_source == null) {
			throw new DataException("Must call SetDataSource before calling ReadFrom");
		}
		return DataStream.CombineStreamsByName(Impl());

		async IAsyncEnumerable<DataStream> Impl()
		{
			var conf = new HtmlConfigurator(_config);
			await foreach (var (name, reader) in _source.GetTextAsync()) {
				foreach (var stream in LoadData(conf, source, name, reader)) {
					yield return stream;
				}
			}
		}
	}

	private static IEnumerable<DataStream> LoadData(
			HtmlConfigurator conf, DataDictionary source, string name, TextReader reader
		)
	{
		var strategy = conf.Streams.FirstOrDefault(s => s.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
		if (strategy == null) {
			throw new JSON.MissingConfigException(name);
		}
		var doc = new HtmlDocument();
		doc.Load(reader);
		foreach (var ls in BuildObjectStreams(name, doc, strategy.Streams.ToDictionary(s => s.Name), source)) {
			yield return ls;
		}

	}

	private static IEnumerable<DataStream> BuildObjectStreams(
			string ns, HtmlDocument doc, Dictionary<string, HtmlConfigurator.HtmlQuery> streams, DataDictionary source
		)
	{
		foreach (var (name, query) in streams) {
			var streamDef = source.GetStream($"{ns}.{name}");
			if (streamDef == null) { continue; }

			var nodes = doc.DocumentNode.SelectNodes( query.Path!);
			if (nodes == null || nodes.Count == 0) { continue; }

			IEnumerable<object?[]> data = query.Type switch {
				HtmlConfigurator.DataType.OuterHtml => nodes.Select<HtmlNode, object[]>(n => [(object)n.OuterHtml]),
				HtmlConfigurator.DataType.InnerHtml => nodes.Select<HtmlNode, object[]>(n => [(object)n.InnerHtml]),
				HtmlConfigurator.DataType.InnerText => nodes.Select<HtmlNode, object[]>(n => [(object)n.InnerText]),
				HtmlConfigurator.DataType.Expressions => BuildDataStreams(nodes, query.Expressions!),
				_ => throw new NotImplementedException()
			};
			yield return new DataStream(new StreamDescription(ns, name), StreamSettings.None, new EnumerableArrayReader(data, streamDef));
		}
	}

	private static IEnumerable<object?[]> BuildDataStreams(HtmlNodeCollection nodes, IList<HtmlConfigurator.ExpressionQuery> expressions)
	{
		foreach (var node in nodes) {
			var result = new object?[expressions.Count];
			for (int i = 0; i < expressions.Count; i++) {
				var expr = expressions[i];
				result[i] = node.SelectSingleNode(expr.Path!)?.InnerText;
			}
			yield return result;
		}
	}

	public void SetDataSource(IDataSource source) => _source = source;

	public async Task<Exception?> TestConnection()
	{
		if (_source == null) {
			return new DataException("No data source has been set.");
		}
		try {
			await foreach (var (name, stream) in _source.GetDataAsync()) {
				break;
			}
		} catch (Exception e) {
			return e;
		}
		return null;
	}

	public void Dispose()
	{
		throw new NotImplementedException();
	}
}
