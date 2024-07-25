using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.TextFile.Lines
{
	public class TextLinesConnector : ConnectorCore
	{
		public override string Name => "Text File (lines)";

		public override Capabilities Capabilities => Capabilities.Reader | Capabilities.Writer | Capabilities.Analyzer | Capabilities.RandomAccessReader;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config) => new TextLinesAnalyzer(config);

		public override DbConnectionStringBuilder GetConfig()
		{
			throw new NotImplementedException();
		}

		public override IReader GetReader(string config) => new TextLinesReader(config);

		public override IWriter GetWriter(string config) => new TextLinesWriter();

		[ModuleInitializer]
		public static void Run()
		{
			ConnectorRegistry.RegisterConnector(new TextLinesConnector());
		}
	}
}