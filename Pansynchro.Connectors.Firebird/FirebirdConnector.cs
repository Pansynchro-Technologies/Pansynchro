using System.Data.Common;
using System.Runtime.CompilerServices;

using FirebirdSql.Data.FirebirdClient;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Firebird
{
	public class FirebirdConnector : ConnectorCore
	{
		public override string Name => "Firebird";

		public override Capabilities Capabilities => Capabilities.ALL;

		public override NameStrategyType Strategy => NameStrategyType.NameOnly;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new FirebirdSchemaAnalyzer(config);

		public override DbConnectionStringBuilder GetConfig() => new FbConnectionStringBuilder();

		public override IReader GetReader(string config) => new FirebirdReader(config);

		public override IWriter GetWriter(string config) => new FirebirdWriter(config);

		[ModuleInitializer]
		public static void Register()
		{
			ConnectorRegistry.RegisterConnector(new FirebirdConnector());
		}
	}
}
