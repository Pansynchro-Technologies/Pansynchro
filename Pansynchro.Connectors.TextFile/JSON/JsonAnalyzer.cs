using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.JSON
{
	public class JsonAnalyzer : ISchemaAnalyzer
	{
		private readonly string _config;

		public JsonAnalyzer(string config)
		{
			_config = config;
		}

		public async ValueTask<DataDictionary> AnalyzeAsync(string name)
		{
			var conf = new JsonConfigurator(_config);
			var streams = new List<StreamDefinition>();
			foreach (var details in conf.Streams) {
				var defs = details.FileStructure switch {
					FileType.Array => AnalyzeArrType(name),
					FileType.Obj => AnalyzeObjType(name, details.Streams.ToDictionary(s => s.Name)),
					_ => throw new ArgumentException($"Unknown file structure type: {details.FileStructure}"),
				};
				streams.AddRange(defs);
			}
			await ValueTask.CompletedTask;
			return new DataDictionary(name, streams.ToArray());
		}

		private static IEnumerable<StreamDefinition> AnalyzeObjType(string ns, IDictionary<string, JsonQuery> streams)
		{
			foreach (var name in streams.Keys) {
				var field = new FieldDefinition("Value", new FieldType(TypeTag.Json, false, CollectionType.None, null));
				yield return new StreamDefinition(new(ns, name), new[] { field }, Array.Empty<string>());
			}
		}

		private static IEnumerable<StreamDefinition> AnalyzeArrType(string name)
		{
			var field = new FieldDefinition("Value", new FieldType(TypeTag.Json, false, CollectionType.None, null));
			yield return new StreamDefinition(new(null, name), new[] { field }, Array.Empty<string>());
		}
	}
}
