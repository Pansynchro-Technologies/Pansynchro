using System.Data.Common;
using System.Runtime.CompilerServices;

using Microsoft.Data.Sqlite;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Sqlite
{
	public class SqliteConnector : ConnectorCore
	{
		public override string Name => "Sqlite";

		public override Capabilities Capabilities => Capabilities.ALL;

		public override NameStrategyType Strategy => NameStrategyType.NameOnly;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new SqliteSchemaAnalyzer(config);

		public override DbConnectionStringBuilder GetConfig() => new SqliteConnectionStringBuilder();

		public override IReader GetReader(string config) => new SqliteReader(config);

		public override IWriter GetWriter(string config) => new SqliteWriter(config);

		[ModuleInitializer]
		public static void Register()
		{
			ConnectorRegistry.RegisterConnector(new SqliteConnector());
		}
	}
}
