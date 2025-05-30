using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Pansynchro.Connectors.TextFile.HTML;
internal class HtmlConnector : ConnectorCore
{
	public override string Name => "HTML";

	public override Capabilities Capabilities
		=> Capabilities.Reader | Capabilities.Analyzer | Capabilities.Configurator;

	public override NameStrategyType Strategy => NameStrategyType.Identity;

	public override ISchemaAnalyzer GetAnalyzer(string config) => new HtmlAnalyzer(config);

	public override DbConnectionStringBuilder GetConfig() => new HtmlConfigurator();

	public override IReader GetReader(string config) => new HtmlReader(config);

	public override IWriter GetWriter(string config)
	{
		throw new NotImplementedException();
	}

	[ModuleInitializer]
	public static void Run()
	{
		ConnectorRegistry.RegisterConnector(new HtmlConnector());
	}
}
