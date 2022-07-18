using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Compression
{
    public class BrotliCompressionFactory : DataProcessorFactoryCore
    {
        public override string Name => "Brotli";

        public override SourceCapabilities Capabilities => SourceCapabilities.ALL;

        public override string SourceConfigSchema => "{}";

        public override string EmptySourceConfig => "";

        public override string SinkConfigSchema => "{}";

        public override string EmptySinkConfig => "";

        public override IDataOutputProcessor GetSink(string config) => new BrotliCompression();

        public override IDataInputProcessor GetSource(string config) => new BrotliCompression();

        [ModuleInitializer]
        public static void Init() => ConnectorRegistry.RegisterProcessor(new BrotliCompressionFactory());
    }
}
