using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using System.Runtime.CompilerServices;

namespace Pansynchro.Sources.Http
{
    public class RestSourceFactory : DataSourceFactoryCore
    {
        public override string Name => "Rest";

        public override SourceCapabilities Capabilities => SourceCapabilities.Source;

        public override IDataSink GetSink(string config) => throw new System.NotImplementedException();

        public override IDataSource GetSource(string config) => new RestSource(config);

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
        },
        ""Next"": {
            ""description"": ""A JSONPath expression to extract a 'Next' URL from paginated results (optional)"",
            ""type"": ""string""
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
  ],
    ""Next"": null
}";

        public override string SinkConfigSchema => throw new System.NotImplementedException();

        public override string EmptySinkConfig => throw new System.NotImplementedException();

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterSource(new RestSourceFactory());
        }
    }
}
