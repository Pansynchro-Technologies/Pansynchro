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

    public record SourceDescription(string Name, string Assembly, SourceCapabilities Capabilities)
    {
        public bool HasSource => Capabilities.HasFlag(SourceCapabilities.Source);
        public bool HasSink => Capabilities.HasFlag(SourceCapabilities.Sink);
    }

    [Flags]
    public enum Capabilities
    {
        None = 0,
        Reader = 1 << 0,
        Writer = 1 << 1,
        Analyzer = 1 << 2,
        Configurator = 1 << 3,
        RandomAccessReader = 1 << 4,

        ALL = Reader | Writer | Analyzer | Configurator | RandomAccessReader
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
        public bool HasRandomAccessReader => Capabilities.HasFlag(Capabilities.RandomAccessReader);
    }

    [Flags]
    public enum SourceCapabilities
    {
        None = 0,
        Source = 1 << 0,
        Sink = 1 << 1,

        ALL = Source | Sink
    }

    public abstract class DataSourceFactoryCore
    {
        public abstract string Name { get; }
        public abstract SourceCapabilities Capabilities { get; }
        public abstract IDataSource GetSource(string config);
        public abstract IDataSink GetSink(string config);
        public abstract string ConfigSchema { get; }
        public abstract string EmptyConfig { get; }

        public bool HasSource => Capabilities.HasFlag(SourceCapabilities.Source);
        public bool HasSink => Capabilities.HasFlag(SourceCapabilities.Sink);
    }
}
