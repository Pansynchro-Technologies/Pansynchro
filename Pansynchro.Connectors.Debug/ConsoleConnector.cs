using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Debug
{
	public class ConsoleConnector : ConnectorCore
	{
		public override string Name => "Console";

		public override Capabilities Capabilities => Capabilities.Writer;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config) => throw new NotImplementedException();

		public override DbConnectionStringBuilder GetConfig() => throw new NotImplementedException();

		public override IReader GetReader(string config) => throw new NotImplementedException();

		public override IWriter GetWriter(string config) => new ConsoleWriter();

		[ModuleInitializer]
		public static void Register()
		{
			ConnectorRegistry.RegisterConnector(new ConsoleConnector());
		}

	}
}
