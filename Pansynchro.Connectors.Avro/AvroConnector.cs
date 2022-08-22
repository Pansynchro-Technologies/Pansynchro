using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Avro
{
	public class AvroConnector : ConnectorCore
	{
		public override string Name => "Avro";

		public override Capabilities Capabilities => Capabilities.Reader | Capabilities.Writer | Capabilities.Analyzer | Capabilities.Configurator | Capabilities.RandomAccessReader;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new AvroAnalyzer();

		public override DbConnectionStringBuilder GetConfig() => new DbConnectionStringBuilder();

		public override IReader GetReader(string config) => new AvroReader();

		public override IWriter GetWriter(string config) => new AvroWriter();

		[ModuleInitializer]
		public static void Run()
		{
			ConnectorRegistry.RegisterConnector(new AvroConnector());
		}
	}
}
