using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Http
{
    public class HttpSourceFactory : DataSourceFactoryCore
    {
        public override string Name => "Http";

        public override SourceCapabilities Capabilities => SourceCapabilities.Source;

        public override IDataSink GetSink(string config) => throw new System.NotImplementedException();

        public override IDataSource GetSource(string config) => new HttpDataSource(config);

        public override string ConfigSchema => @"{
    ""type"": ""object"",
    ""properties"": {
        ""Urls"": {
            ""description"": ""The set of URLs for the Data Source to retrieve"",
            ""type"": ""array"",
            ""minItems"": 1,
            ""items"": {
                ""type"": ""object"",
                ""properties"": {
                    ""Name"": {
                        ""description"": ""The name of the stream"",
                        ""type"": ""string"",
                    },
                    ""Url"": {
                        ""description"": ""The URL to retireve"",
                        ""type"": ""string""
                    }
                },
                ""required"": [""Name"", ""Url""]
            }
        }
    },
    ""required"": [""Urls""]
}";

        public override string EmptyConfig =>
@"{
  ""Urls"": [
    {
      ""Name"": """",
      ""Url"": [""]
    }
  ]
}";

        public override string SinkConfigSchema => throw new System.NotImplementedException();

        public override string EmptySinkConfig => throw new System.NotImplementedException();

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterSource(new HttpSourceFactory());
        }
    }
}
