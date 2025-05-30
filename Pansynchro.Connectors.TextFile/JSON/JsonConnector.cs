using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.JSON
{
	public class JsonConnector : ConnectorCore
	{
		public override string Name => "JSON";

		public override Capabilities Capabilities
			=> Capabilities.Reader | Capabilities.Writer | Capabilities.Analyzer | Capabilities.Configurator | Capabilities.RandomAccessReader;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new JsonAnalyzer(config);

		public override DbConnectionStringBuilder GetConfig() => new JsonConfigurator();

		public override IReader GetReader(string config) => new JsonReader(config);

		public override IWriter GetWriter(string config) => new JsonWriter(config);

		[ModuleInitializer]
		public static void Run()
		{
			ConnectorRegistry.RegisterConnector(new JsonConnector());
		}
	}
}
