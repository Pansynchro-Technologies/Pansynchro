using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.CSV
{
	public class CsvConnector : ConnectorCore
	{
		public override string Name => "CSV";

		public override Capabilities Capabilities => Capabilities.ALL;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new CsvAnalyzer(config);

		public override DbConnectionStringBuilder GetConfig() => new CsvConfigurator();

		public override IReader GetReader(string config) => new CsvReader(config);

		public override IWriter GetWriter(string config) => new CsvWriter(config);

		[ModuleInitializer]
		public static void Run()
		{
			ConnectorRegistry.RegisterConnector(new CsvConnector());
		}
	}
}
