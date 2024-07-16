using System.Runtime.CompilerServices;

using Pansynchro.Core.Connectors;
using Pansynchro.Core;

namespace Pansynchro.Sources.GoogleCloudStorage
{
	public class GcsDataSourceFactory : DataSourceFactoryCore
	{
		public override string Name => "Google Cloud Storage";

		public override SourceCapabilities Capabilities => SourceCapabilities.ALL;

		public override IDataSource GetSource(string config) => new GcsDataSource(config);

		public override IDataSink GetSink(string config) => new GcsDataSink(config);

		public override string SourceConfigSchema =>
@"{
  ""title"": ""GCS Configuration"",
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

		public override string EmptySourceConfig =>
@"{
    ""Conn"": {
        ""AccessKeyId"": ""0123456789abcdef"",
        ""SecretAccessKey"": ""SVQnUyBBIFNFQ1JFVCBUTyBFVkVSWUJPRFku"",
        ""RegionCode"": ""us-west-1""
    },
    ""Bucket"": ""BucketName"",
    ""Files"": [
        {
            ""Pattern"": ""*.csv"",
            ""StreamName"": ""S3Data""
        }
    ]
}";

		public override string SinkConfigSchema =>
@"{
  ""title"": ""S3WriteConfig"",
  ""definitions"": {
    ""S3Config"": {
      ""type"": ""object"",
      ""additionalProperties"": false,
      ""properties"": {
        ""Conn"": {
          ""type"": ""object"",
          ""additionalProperties"": false,
          ""properties"": {
            ""AccessKeyId"": {
              ""type"": ""string""
            },
            ""SecretAccessKey"": {
              ""type"": ""string""
            },
            ""RegionCode"": {
              ""type"": ""string""
            }
          }
        },
        ""Bucket"": {
          ""type"": ""string""
        },
        ""Files"": {
          ""type"": ""array"",
          ""items"": {
            ""type"": ""object"",
            ""additionalProperties"": false,
            ""properties"": {
              ""Pattern"": {
                ""type"": ""string""
              },
              ""StreamName"": {
                ""type"": ""string""
              }
            }
          }
        },
        ""MissingFilenameSpec"": {
          ""type"": ""string""
        },
        ""UploadPartSize"": {
          ""type"": ""integer""
        }
      }
    },
  }
}";

		public override string EmptySinkConfig =>
@"{
    ""Conn"": {
        ""AccessKeyId"": ""0123456789abcdef"",
        ""SecretAccessKey"": ""SVQnUyBBIFNFQ1JFVCBUTyBFVkVSWUJPRFku"",
        ""RegionCode"": ""us-west-1""
    },
    ""Bucket"": ""BucketName"",
    ""Files"": [
        {
            ""Pattern"": ""*.csv"",
            ""StreamName"": ""S3Data""
        }
    ],
    ""MissingFilenameSpec"": ""PansynchroData/Missing/*"",
    ""UploadPartSize"": 5
}";

		[ModuleInitializer]
		public static void Register()
		{
			ConnectorRegistry.RegisterSource(new GcsDataSourceFactory());
		}
	}
}
