using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Compression
{
	public class GzipCompressionFactory : DataProcessorFactoryCore
	{
		public override string Name => "Gzip";

		public override SourceCapabilities Capabilities => SourceCapabilities.ALL;

		public override string SourceConfigSchema => "{}";

		public override string EmptySourceConfig => "";

		public override string SinkConfigSchema => "{}";

		public override string EmptySinkConfig => "";

		public override IDataOutputProcessor GetSink(string config) => new GzipCompression();

		public override IDataInputProcessor GetSource(string config) => new GzipCompression();

		[ModuleInitializer]
		public static void Init() => ConnectorRegistry.RegisterProcessor(new GzipCompressionFactory());
	}
}
