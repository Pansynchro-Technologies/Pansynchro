using System;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.S3
{
    public class S3DataSourceFactory : DataSourceFactoryCore
    {
        public override string Name => "S3";

        public override SourceCapabilities Capabilities => SourceCapabilities.Source;

        public override IDataSource GetSource(string config)
            => new S3DataSource(config);

        public override IDataSink GetSink(string config)
        {
            throw new NotImplementedException();
        }

        public override string ConfigSchema =>
@"{
  ""title"": ""S3 Configuration"",
  ""type"": ""object"",
  ""additionalProperties"": false,
  ""properties"": {
    ""Conn"": {
      ""type"": ""object"",
      ""description"": ""AWS S3 connection info.  Requires an AWS Access Key."",
      ""additionalProperties"": false,
      ""properties"": {
        ""AccessKeyId"": {
          ""description"": ""Access Key ID value."",
          ""type"": ""string""
        },
        ""SecretAccessKey"": {
          ""description"": ""Access Key secret value."",
          ""type"": ""string""
        },
        ""RegionCode"": {
          ""description"": ""S3 region system name. (us-west-1, eu-north-1, etc.)"",
          ""type"": [""null"", ""string""]
        }
      },
      ""required"": [""AccessKeyId"", ""SecretAccessKey"", ""RegionCode""]
    },
    ""Bucket"": {
        ""description"": ""S3 bucket name"",
        ""type"": ""string""
    },
    ""Files"": {
      ""type"": ""array"",
      ""description"": ""Specifications of files to retrieve"",
      ""items"": {
        ""type"": ""object"",
        ""additionalProperties"": false,
        ""properties"": {
          ""Pattern"": {
            ""description"": ""Filename or file glob pattern"",
            ""type"": ""string""
          },
          ""StreamName"": {
            ""description"": ""Pansynchro stream name to return the file(s) under"",
            ""type"": ""string""
          }
        },
        ""required"": [""Pattern"", ""StreamName""]
      }
    }
  },
  ""required"": [""Conn"", ""Bucket"", ""Files""]
}";

        public override string EmptyConfig =>
@"{
    ""Conn"": {
        ""AccessKeyId"": ""0123456789abcdef"",
        ""SecretAccessKey"": ""SVQnUyBBIFNFQ1JFVCBUTyBFVkVSWUJPRFku"",
        ""RegionCode"": null
    },
    ""Files"": [
        {
            ""Bucket"": ""BucketName"",
            ""Pattern"": ""*.csv"",
            ""StreamName"": ""S3Data""
        }
    ]
}";

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterSource(new S3DataSourceFactory());
        }
    }
}
