using Pansynchro.Core.DataDict;
using System;
using System.Data.Common;

namespace Pansynchro.Core.Connectors
{
    public record ConnectorDescription(string Name, string Assembly, Capabilities Capabilities, bool RequiresDataSource)
    {
        public bool HasReader => Capabilities.HasFlag(Capabilities.Reader);
        public bool HasWriter => Capabilities.HasFlag(Capabilities.Writer);
        public bool HasAnalyzer => Capabilities.HasFlag(Capabilities.Analyzer);
        public bool HasConfig => Capabilities.HasFlag(Capabilities.Configurator);
    }

    public record SourceDescription(string Name, string Assembly);

    [Flags]
    public enum Capabilities
    {
        None = 0,
        Reader = 1 << 0,
        Writer = 1 << 1,
        Analyzer = 1 << 2,
        Configurator = 1 << 3,

        ALL = Reader | Writer | Analyzer | Configurator
    }

    public abstract class ConnectorCore
    {
        public abstract string Name { get; }
        public abstract Capabilities Capabilities { get; }
        public abstract IReader GetReader(string config);
        public abstract IWriter GetWriter(string config);
        public abstract ISchemaAnalyzer GetAnalyzer(string config);
        public abstract DbConnectionStringBuilder GetConfig();
        public virtual SimpleConnectionStringBuilder? GetSimpleConfig() => null;
        public abstract NameStrategyType Strategy { get; }

        public bool HasReader => Capabilities.HasFlag(Capabilities.Reader);
        public bool HasWriter => Capabilities.HasFlag(Capabilities.Writer);
        public bool HasAnalyzer => Capabilities.HasFlag(Capabilities.Analyzer);
        public bool HasConfig => Capabilities.HasFlag(Capabilities.Configurator);
    }
}
