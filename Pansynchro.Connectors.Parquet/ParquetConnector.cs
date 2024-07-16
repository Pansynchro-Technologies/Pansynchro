using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Parquet
{
	public class ParquetConnector : ConnectorCore
	{
		public override string Name => "Parquet";

		public override Capabilities Capabilities => Capabilities.Reader | Capabilities.Writer | Capabilities.Analyzer | Capabilities.Configurator | Capabilities.RandomAccessReader;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new ParquetAnalyzer(config);

		public override DbConnectionStringBuilder GetConfig() => new DbConnectionStringBuilder();

		public override IReader GetReader(string config) => new ParquetReader(config);

		public override IWriter GetWriter(string config) => new ParquetWriter();

		[ModuleInitializer]
		public static void Run()
		{
			ConnectorRegistry.RegisterConnector(new ParquetConnector());
		}
	}
}
