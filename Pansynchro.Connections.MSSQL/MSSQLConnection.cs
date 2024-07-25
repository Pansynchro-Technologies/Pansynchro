using System.Data.Common;
using System.Runtime.CompilerServices;

using Microsoft.Data.SqlClient;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.MSSQL
{
	public class MSSQLConnector : ConnectorCore
	{
		public override string Name => "MSSQL";

		public override Capabilities Capabilities => Capabilities.ALL;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new MssqlSchemaAnalyzer(config);

		public override DbConnectionStringBuilder GetConfig() => new SqlConnectionStringBuilder();

		public override IReader GetReader(string config) => new MSSQLReader(config, null);

		public override IWriter GetWriter(string config) => new MSSQLWriter(config, null);

		public override SimpleConnectionStringBuilder GetSimpleConfig()
		{
			var result = GetConfig();
			return new SimpleConnectionStringBuilder(result, "UserID", "Password", "DataSource", "InitialCatalog");
		}

		[ModuleInitializer]
		public static void Register()
		{
			ConnectorRegistry.RegisterConnector(new MSSQLConnector());
		}
	}
}
