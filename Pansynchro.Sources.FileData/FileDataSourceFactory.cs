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

        public override string SinkConfigSchema =>
@"{
  ""type"": ""object"",
  ""additionalProperties"": false,
  ""properties"": {
    ""Files"": {
      ""description"": ""The set of streams for the Data Source to save"",
      ""type"": ""array"",
      ""items"": {
        ""type"": ""object"",
        ""additionalProperties"": false,
        ""properties"": {
          ""StreamName"": {
            ""description"": ""A stream name or pattern to match a stream name"",
            ""type"": ""string""
          },
          ""Filename"": {
            ""description"": ""The name of the file to save the stream to.  A * will insert the stream name into the filename."",
            ""type"": ""string""
          }
        }
      }
    },
    ""MissingFilenameSpec"": {
      ""description"": ""A filename to save unmatched stream names to.  A * will insert the stream name into the filename.  Optional; if this is blank, an unmatched stream name will raise an error."",
      ""type"": ""string""
    },
    ""DuplicateFilenameAction"": {
      ""description"": ""Action to take if the filename already exists."",
      ""type"": ""integer"",
      ""description"": """",
      ""x-enumNames"": [
        ""Append"",
        ""Overwrite"",
        ""SequenceNumber"",
        ""Error""
      ],
      ""enum"": [
        0,
        1,
        2,
        3
      ]
    }
  }
}";

        public override string EmptySinkConfig =>
@"{
  ""Files"": [
    {
      ""StreamName"": ""*"",
      ""Filename"": ""C:\\PansynchroData\\*""
    }
  ],
  ""MissingFilenameSpec"": ""C:\\PansynchroData\\Missing\\*"",
  ""DuplicateFilenameAction"": 0
}";

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterSource(new FileDataSourceFactory());
        }
    }
}