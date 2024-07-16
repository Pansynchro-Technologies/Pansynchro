using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Excel
{
	public class ExcelConnector : ConnectorCore
	{
		public override string Name => "Excel";

		public override Capabilities Capabilities => Capabilities.Reader | Capabilities.Analyzer;

		public override NameStrategyType Strategy => NameStrategyType.Identity;

		public override ISchemaAnalyzer GetAnalyzer(string config)
		{
			return new ExcelAnalyzer();
		}

		public override DbConnectionStringBuilder GetConfig() => new CustomConfiguratorBase();

		public override IReader GetReader(string config) => new ExcelReader(config);

		public override IWriter GetWriter(string config)
		{
			throw new NotImplementedException();
		}

		[ModuleInitializer]
		public static void Run()
		{
			ConnectorRegistry.RegisterConnector(new ExcelConnector());
			System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
		}
	}
}
