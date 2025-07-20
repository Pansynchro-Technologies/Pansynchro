using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Json.Path;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Readers;

using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Pansynchro.Connectors.TextFile.JSON
{
	public class JsonReader : IReader, ISourcedConnector, IRandomStreamReader
	{
		private readonly string _config;
		private IDataSource? _source;

		public JsonReader(string config)
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
				var conf = new JsonConfigurator(_config);
				await foreach (var (name, reader) in _source.GetTextAsync()) {
					await foreach (var stream in LoadData(conf, source, name, reader)) {
						yield return stream;
					}
				}
			}
		}

		private static async IAsyncEnumerable<DataStream> LoadData(
			JsonConfigurator conf, DataDictionary source, string name, TextReader reader
		)
		{
			var strategy = conf.Streams.FirstOrDefault(s => s.Name == name);
			if (strategy == null) {
				throw new MissingConfigException(name);
			}
			var data = JsonNode.Parse(reader.ReadToEnd());
			Validate(name, data, strategy.ErrorPath, await LoadSchema(strategy.Schema));
			if (strategy.FileStructure == FileType.Array) {
				if (!source.HasStream(name)) {
					throw new MissingDataException(name);
				}
				yield return BuildArrayStream(name, data, source.GetStream(name));
			} else {
				foreach (var ls in BuildObjectStreams(name, data, strategy.Streams.ToDictionary(s => s.Name), source)) {
					yield return ls;
				}
			}
		}

		public Task<DataStream> ReadStream(DataDictionary source, string name)
		{
			if (_source == null) {
				throw new DataException("Must call SetDataSource before calling ReadStream");
			}
			var streamNameParser = StreamDescription.Parse(name);
			var streamName = streamNameParser.Namespace ?? streamNameParser.Name;
			var conf = new JsonConfigurator(_config);
			var readers = _source.GetTextAsync(streamName)
				.SelectMany(reader => JsonReader.LoadData(conf, source, name, reader))
				.Where(ds => ds.Name.Equals(streamNameParser))
				.Select(ds => ds.Reader);
			return Task.FromResult<DataStream>(new(streamNameParser, StreamSettings.None, new GroupingReader(readers.ToEnumerable())));
		}

		private static async ValueTask<string?> LoadSchema(string? schema)
		{
			if (string.IsNullOrEmpty(schema)) {
				return null;
			}
			if (Uri.TryCreate(schema, UriKind.Absolute, out var url) && !url.IsFile) {
				using var client = new HttpClient();
				return await client.GetStringAsync(url);
			}
			return await File.ReadAllTextAsync(schema);
		}

		private static void Validate(string name, JsonNode data, string? errorPath, string? schema)
		{
			if (!string.IsNullOrEmpty(errorPath)) {
				var error = SelectToken(data, errorPath);
				if (error != null) {
					throw new ValidationException(name, error.ToString()!);
				}
			}
			if (schema != null) {
				var js = Json.Schema.JsonSchema.FromText(schema);
				var validation = js.Evaluate(data);
				if (validation.HasErrors) {
					throw new ValidationException(name, string.Join(Environment.NewLine, validation.Errors!.Select(p => $"{p.Key}: {p.Value}")));
				}
			}
		}

		private static DataStream BuildArrayStream(string name, JsonNode data, StreamDefinition stream)
		{
			if (data is JsonArray arr) {
				return new DataStream(new StreamDescription(null, name), StreamSettings.None, new JsonArrayReader(arr, stream.Fields[0].Name));
			} else throw new ValidationException($"Stream {name} is not a JSON array");
		}

		private static IEnumerable<DataStream> BuildObjectStreams(
			string ns, JsonNode data, IDictionary<string, JsonQuery> streams, DataDictionary dict)
		{
			if (data is JsonObject) {
				foreach (var (name, query) in streams) {
					var streamData = string.IsNullOrEmpty(query.Path) ? data : SelectToken(data, query.Path);
					var streamName = new StreamDescription(ns, name);
					if (!dict.HasStream(streamName.ToString())) {
						throw new MissingDataException(name);
					}
					var fieldName = dict.GetStream(streamName.ToString()).Fields[0].Name;
					if (streamData is JsonArray arr) {
						yield return new DataStream(streamName, 0, new JsonArrayReader(arr, fieldName));
					} else if (streamData != null) {
						yield return new DataStream(streamName, 0, new SingleValueReader(fieldName, streamData));
					} else if (query.Required) {
						throw new ValidationException($"The query '{query.Path}' did not return any value on stream '{ns}'");
					}
				}
			} else throw new ValidationException($"Stream {ns} is not a JSON object");
		}

		private static JsonNode? SelectToken(JsonNode value, string path)
		{
			var pathObj = JsonPath.Parse(path);
			var result = pathObj.Evaluate(value);
			var matches = result.Matches;
			return matches.Count switch {
				0 => null,
				1 => matches[0].Value,
				_ => new JsonArray(matches.Select(m => m.Value).ToArray())
			};
		}

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

		void ISourcedConnector.SetDataSource(IDataSource source)
		{
			_source = source;
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
		}
	}

	[Serializable]
	internal class ValidationException : Exception
	{
		public ValidationException(string message) : base(message) { }

		public ValidationException(string name, string message)
			: base($"JSON validation failed for stream {name} with message: {Environment.NewLine}{Environment.NewLine}{message}")
		{ }

		public ValidationException(string name, IList<string> errors)
			: base($"JSON Schema validation failed for stream {name} with the following error(s): {Environment.NewLine}{Environment.NewLine}{string.Join(Environment.NewLine, errors)}")
		{ }
	}

	class MissingConfigException : Exception
	{
		public MissingConfigException(string name)
			: base($"No JSON configuration was found for a stream named {name}.")
		{ }
	}

	class MissingDataException : Exception
	{
		public MissingDataException(string name)
			: base($"No stream named {name} was found in the data dictionary.")
		{ }
	}
}
