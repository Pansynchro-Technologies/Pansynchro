using System.Runtime.CompilerServices;
using System.Text;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Compression;
public class ZipfileCompressionFactory : DataProcessorFactoryCore
{
	public override string Name => "Zipfile";

	public override SourceCapabilities Capabilities => SourceCapabilities.ALL;

	public override string SourceConfigSchema => "{}";

	public override string EmptySourceConfig => "";

	public override string SinkConfigSchema => @"{
  ""$schema"": ""https://json-schema.org/draft/2020-12/schema"",
  ""type"": ""object"",
  ""properties"": {
    ""StreamName"": {
      ""type"": ""string""
    }
  },
  ""required"": [""StreamName""],
  ""additionalProperties"": false
}";

	public override string EmptySinkConfig => @"{""StreamName"": ""Pansynchro""}";

	public override IDataOutputProcessor GetSink(string config) => new ZipfileCompression(config);

	public override IDataInputProcessor GetSource(string config) => new ZipfileCompression(config);

	[ModuleInitializer]
	public static void Init() => ConnectorRegistry.RegisterProcessor(new ZipfileCompressionFactory());
}
