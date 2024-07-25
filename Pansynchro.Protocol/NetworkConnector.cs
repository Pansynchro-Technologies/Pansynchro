using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Protocol
{
	class NetworkConnector : ConnectorCore
	{
		public override string Name => "Network";

		public override Capabilities Capabilities => Capabilities.Reader | Capabilities.Writer;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config)
		{
			throw new NotImplementedException();
		}

		public override DbConnectionStringBuilder GetConfig()
		{
			throw new NotImplementedException();
		}

		public override IReader GetReader(string config) => BinaryDecoder.Connect(config);

		public override IWriter GetWriter(string config) => BinaryEncoder.Connect(config);


		[ModuleInitializer]
		public static void Register() => ConnectorRegistry.RegisterConnector(new NetworkConnector());
	}
}
