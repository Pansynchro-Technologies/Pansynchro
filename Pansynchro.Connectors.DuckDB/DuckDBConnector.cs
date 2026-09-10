using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

using DuckDBConnectionStringBuilder = DuckDB.NET.Data.DuckDBConnectionStringBuilder;

namespace Pansynchro.Connectors.DuckDB;

public class DuckDBConnector : ConnectorCore
{
	public override string Name => "DuckDB";

	public override Capabilities Capabilities => Capabilities.ALL;

	public override NameStrategyType Strategy => NameStrategyType.LowerCase;

	public override ISchemaAnalyzer GetAnalyzer(string config) => new DuckDBSchemaAnalyzer(config);

	public override DbConnectionStringBuilder GetConfig() => new DuckDBConnectionStringBuilder();

	public override IReader GetReader(string config) => new DuckDBReader(config);

	public override IWriter GetWriter(string config) => new DuckDBWriter(config);

	[ModuleInitializer]
	public static void Register() => ConnectorRegistry.RegisterConnector(new DuckDBConnector());
}
