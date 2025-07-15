using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.FileSystem;
internal class FileSystemConnector : ConnectorCore
{
	public override string Name => "FileSystem";

	public override Capabilities Capabilities => Capabilities.Reader | Capabilities.Analyzer | Capabilities.Configurator | Capabilities.RandomAccessReader;

	public override NameStrategyType Strategy => NameStrategyType.Identity;

	public override ISchemaAnalyzer GetAnalyzer(string config) => new FileSystemAnalyzer();

	public override DbConnectionStringBuilder GetConfig() => new FileSystemConfigurator();

	public override IReader GetReader(string config) => new FileSystemReader(config);

	public override IWriter GetWriter(string config) => throw new NotImplementedException();

	[ModuleInitializer]
	public static void Run()
	{
		ConnectorRegistry.RegisterConnector(new FileSystemConnector());
	}
}
