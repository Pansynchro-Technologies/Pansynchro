using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;

namespace Pansynchro.Sources.Azure
{
	public class AdlsDataSourceFactory : DataSourceFactoryCore
	{
		public override string Name => "ADLS";

		public override SourceCapabilities Capabilities => SourceCapabilities.ALL;

		public override IDataSource GetSource(string config) => new AdlsDataSource(config);

		public override IDataSink GetSink(string config) => new AdlsDataSink(config);

		public override string SourceConfigSchema =>
@"{
  ""title"": ""ADLS Configuration"",
  ""type"": ""object"",
  ""additionalProperties"": false,
  ""properties"": {
	""Conn"": {
	  ""type"": ""object"",
	  ""description"": ""Azure Data Lake Storage connection info.  Requires a shared access signature (SAS) token."",
	  ""additionalProperties"": false,
	  ""properties"": {
		""AccountName"": {
		  ""description"": ""The name of the ADLS account.  The client will connect to https://{AccountName}.dfs.core.windows.net"",
		  ""type"": ""string""
		},
		""SasToken"": {
		  ""description"": ""Shared Access Signature (SAS) token value."",
		  ""type"": ""string""
		},
	  },
	  ""required"": [""AccountName"", ""SasToken""]
	},
	""FileSystem"": {
		""description"": ""ADLS filesystem name"",
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
  ""required"": [""Conn"", ""FileSystem"", ""Files""]
}";

		public override string EmptySourceConfig =>
@"{
	""Conn"": {
		""AccountName"": ""Pansynchro"",
		""SecretAccessKey"": ""SVQnUyBBIFNFQ1JFVCBUTyBFVkVSWUJPRFku""
	},
	""FileSystem"": ""FileSystemName"",
	""Files"": [
		{
			""Pattern"": ""*.csv"",
			""StreamName"": ""AdlsData""
		}
	]
}";

		public override string SinkConfigSchema =>
@"{
  ""title"": ""S3WriteConfig"",
  ""definitions"": {
	""AdlsConfig"": {
	  ""type"": ""object"",
	  ""additionalProperties"": false,
	  ""properties"": {
		""Conn"": {
		  ""type"": ""object"",
		  ""description"": ""Azure Data Lake Storage connection info.  Requires a shared access signature (SAS) token."",
		  ""additionalProperties"": false,
		  ""properties"": {
			""AccountName"": {
			  ""description"": ""The name of the ADLS account.  The client will connect to https://{AccountName}.dfs.core.windows.net"",
			  ""type"": ""string""
			},
			""SasToken"": {
			  ""description"": ""Shared Access Signature (SAS) token value."",
			  ""type"": ""string""
			},
		  },
		  ""required"": [""AccountName"", ""SasToken""]
		},
		""FileSystem"": {
			""description"": ""ADLS filesystem name"",
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
		}
	  }
	},
  }
}";

		public override string EmptySinkConfig =>
@"{
	""Conn"": {
		""AccountName"": ""Pansynchro"",
		""SecretAccessKey"": ""SVQnUyBBIFNFQ1JFVCBUTyBFVkVSWUJPRFku""
	},
	""FileSystem"": ""FileSystemName"",
	""Files"": [
		{
			""Pattern"": ""*.csv"",
			""StreamName"": ""AdlsData""
		}
	],
	""MissingFilenameSpec"": ""PansynchroData/Missing/*"",
}";

		[ModuleInitializer]
		public static void Register()
		{
			ConnectorRegistry.RegisterSource(new AdlsDataSourceFactory());
		}
	}
}
