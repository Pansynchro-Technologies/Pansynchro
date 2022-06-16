using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Pansynchro.Core;
using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;

namespace Pansynchro.Connectors.Oracle
{
    internal class OracleConnector : ConnectorCore
    {
        public override string Name => "Oracle";

        public override Capabilities Capabilities => Capabilities.ALL;

        public override NameStrategyType Strategy => NameStrategyType.LowerCase;

        public override ISchemaAnalyzer GetAnalyzer(string config) => new OracleSchemaAnalyzer(config);

        public override DbConnectionStringBuilder GetConfig() => new OracleConnectionStringBuilder();

        public override IReader GetReader(string config) => new OracleReader(config);

        public override IWriter GetWriter(string config) => new OracleWriter(config);

        [ModuleInitializer]
        public static void Register()
        {
            ConnectorRegistry.RegisterConnector(new OracleConnector());
        }
    }
}
