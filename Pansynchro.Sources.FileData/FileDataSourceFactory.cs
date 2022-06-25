using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Files
{
    public class FileDataSourceFactory : DataSourceFactoryCore
    {
        public override string Name => "Files";

        public override SourceCapabilities Capabilities => SourceCapabilities.Source;

        public override IDataSink GetSink(string config) => throw new System.NotImplementedException();

        public override IDataSource GetSource(string config) => new FileDataSource(config);

        public override string ConfigSchema =>
@"{
    ""type"": ""object"",
    ""properties"": {
        ""Files"": {
            ""description"": ""The set of files for the Data Source to retrieve"",
            ""type"": ""array"",
            ""minItems"": 1,
            ""items"": {
                ""type"": ""object"",
                ""properties"": {
                    ""Name"": {
                        ""description"": ""The name of the stream"",
                        ""type"": ""string"",
                    },
                    ""File"": {
                        ""description"": ""The location of the file(s).  Wildcard patterns (*, ?, and **) are permitted."",
                        ""type"": ""array"",
                        ""minItems"": 1,
                        ""items"": {
                            ""type"": ""string""
                        }
                    }
                },
                ""required"": [""Name"", ""File""]
            }
        }
    },
    ""required"": [""Files""]
}";

        public override string EmptyConfig =>
@"{
  ""Files"": [
    {
      ""Name"": """",
      ""File"": [""""]
    }
  ]
}";

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterSource(new FileDataSourceFactory());
        }
    }
}