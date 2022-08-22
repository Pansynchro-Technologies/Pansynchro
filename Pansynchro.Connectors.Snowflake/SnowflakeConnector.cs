using System.Data.Common;
using System.Runtime.CompilerServices;

using Tortuga.Data.Snowflake;

using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Snowflake
{
    public class SnowflakeConnector : ConnectorCore
    {
        public override string Name => "Snowflake";

        public override Capabilities Capabilities => Capabilities.ALL;

        public override NameStrategyType Strategy => NameStrategyType.Identity;

        public override ISchemaAnalyzer GetAnalyzer(string config) => new SnowflakeSchemaAnalyzer(config);

        public override DbConnectionStringBuilder GetConfig() => new SnowflakeDbConnectionStringBuilder();

        public override IReader GetReader(string config) => new SnowflakeReader(config);

        public override IWriter GetWriter(string config) => new SnowflakeWriter(config);

        [ModuleInitializer]
        public static void Init()
        {
            ConnectorRegistry.RegisterConnector(new SnowflakeConnector());
        }
    }
}
