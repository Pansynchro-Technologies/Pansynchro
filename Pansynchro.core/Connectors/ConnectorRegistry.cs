using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;

using Pansynchro.Core.DataDict;

namespace Pansynchro.Core.Connectors
{
    public static class ConnectorRegistry
    {
        public const string CONNECTOR_FILE = "connectors.pansync";

        private static readonly Dictionary<string, ConnectorCore> _factories = new();
        private static readonly Dictionary<string, DataSourceFactoryCore> _sources = new();
        private static readonly Dictionary<string, DataProcessorFactoryCore> _procesors = new();
        private static readonly Dictionary<string, ConnectorDescription> _connectors = new();
        private static readonly Dictionary<string, SourceDescription> _sourceLoaders = new();
        private static readonly Dictionary<string, SourceDescription> _procLoaders = new();

        static ConnectorRegistry()
        {
            AssemblyLoadContext.Default.Resolving += ResolveLocal;
        }

        private static Assembly? ResolveLocal(AssemblyLoadContext context, AssemblyName name)
        {
            var filename = Path.Combine(Environment.CurrentDirectory, name.Name + ".dll");
            return File.Exists(filename) ? AssemblyLoadContext.Default.LoadFromAssemblyPath(filename) : null;
        }

        public static IEnumerable<string> ReaderTypes => _connectors.Where(kv => kv.Value.HasReader).Select(kv => kv.Key);

        public static IEnumerable<string> WriterTypes => _connectors.Where(kv => kv.Value.HasWriter).Select(kv => kv.Key);

        public static IEnumerable<string> AnalyzerTypes => _connectors.Where(kv => kv.Value.HasAnalyzer).Select(kv => kv.Key);

        public static IEnumerable<string> ConfigurableTypes => _connectors.Where(kv => kv.Value.HasConfig).Select(kv => kv.Key);

        public static IEnumerable<string> DataSourceTypes => _sourceLoaders.Keys;

        public static void LoadConnectors(IEnumerable<ConnectorDescription> connectors)
        {
            foreach (var conn in connectors) {
                _connectors.Add(conn.Name, conn);
            }
        }

        public static void LoadDataSources(IEnumerable<SourceDescription> sources)
        {
            foreach (var src in sources) {
                _sourceLoaders.Add(src.Name, src);
            }
        }

        public static void LoadDataProcessors(IEnumerable<SourceDescription> sources)
        {
            foreach (var src in sources) {
                _procLoaders.Add(src.Name, src);
            }
        }

        public static void RegisterSource(DataSourceFactoryCore factory)
            => _sources.Add(factory.Name, factory);

        public static void RegisterConnector(ConnectorCore connector)
            => _factories.Add(connector.Name, connector);

        public static void RegisterProcessor(DataProcessorFactoryCore factory)
            => _procesors.Add(factory.Name, factory);

        private static ConnectorCore GetFactory(string name)
        {
            if (!_factories.TryGetValue(name, out var factory)) {
                if (_connectors.TryGetValue(name, out var conn)) {
                    var laResult = LoadAssembly(conn.Assembly);
                    HandleResult(name, laResult);
                    _factories.TryGetValue(name, out factory);
                }
            }
            return factory ?? throw new ArgumentException($"No connector named '{name}' is registered.");
        }

        public static string GetLocation(string name)
        {
            _connectors.TryGetValue(name, out var conn);
            return conn?.Assembly ?? throw new ArgumentException($"No connector named '{name}' is registered."); ;
        }

        public static IReader GetReader(string name, string connectionString)
        {
            var factory = GetFactory(name);
            if (factory.HasReader) {
                return factory.GetReader(Process(connectionString, factory));
            }
            throw new ArgumentException($"Connector '{name}' does not define a reader");
        }

        public static IWriter GetWriter(string name, string connectionString)
        {
            var factory = GetFactory(name);
            if (factory.HasWriter) {
                return factory.GetWriter(Process(connectionString, factory));
            }
            throw new ArgumentException($"Connector '{name}' does not define a writer");
        }

        public static ISchemaAnalyzer GetAnalyzer(string name, string connectionString)
        {
            var factory = GetFactory(name);
            if (factory.HasAnalyzer) {
                return factory.GetAnalyzer(Process(connectionString, factory));
            }
            throw new ArgumentException($"Connector '{name}' does not define an analyzer");
        }

        public static DbConnectionStringBuilder GetConfigurator(string name)
        {
            var factory = GetFactory(name);
            if (factory.HasConfig) {
                return factory.GetConfig();
            }
            throw new ArgumentException($"Connector '{name}' does not define a configurator");
        }

        public static NameStrategyType GetStrategy(string name)
        {
            var factory = GetFactory(name);
            return factory.Strategy;
        }

        public static DataSourceFactoryCore GetSourceFactory(string name)
        {
            if (_sources.TryGetValue(name, out var factory)) {
                return factory;
            }
            if (_sourceLoaders.TryGetValue(name, out var desc)) {
                if (desc.Assembly == null) {
                    throw new ArgumentException($"No assembly is defined for '{name}'.");
                }
                var laResult = LoadAssembly(desc.Assembly);
                HandleResult(name, laResult);
                if (_sources.TryGetValue(name, out factory)) {
                    return factory;
                }
            }
            throw new ArgumentException($"No data source named '{name}' is registered.");

        }

        public static IDataSource GetSource(string name, string connectionString)
            => GetSourceFactory(name).GetSource(connectionString);

        public static DataProcessorFactoryCore GetProcessorFactory(string name)
        {
            if (_procesors.TryGetValue(name, out var factory)) {
                return factory;
            }
            if (_procLoaders.TryGetValue(name, out var desc)) {
                if (desc.Assembly == null) {
                    throw new ArgumentException($"No assembly is defined for '{name}'.");
                }
                var laResult = LoadAssembly(desc.Assembly);
                HandleResult(name, laResult);
                if (_procesors.TryGetValue(name, out factory)) {
                    return factory;
                }
            }
            throw new ArgumentException($"No data source named '{name}' is registered.");

        }

        public static IDataInputProcessor GetInputProcessor(string name, string config)
        {
            var factory = GetProcessorFactory(name);
            return factory.GetSource(config);
        }

        public static IDataOutputProcessor GetOutputProcessor(string name, string config)
        {
            var factory = GetProcessorFactory(name);
            return factory.GetSink(config);
        }

        private static void HandleResult(string name, LoadAssemblyResult laResult)
        {
            switch (laResult) {
                case LoadAssemblyResult.Success:
                    return;
                case LoadAssemblyResult.Fail:
                    throw new ArgumentException($"Unable to locate the assembly for the {name} connector.");
                case LoadAssemblyResult.Dupe:
                    throw new ArgumentException($"The assembly for the {name} connector is already loaded, but no connector is registered.");
                default:
                    throw new ArgumentException($"Unknown LoadAssemblyResult value: {laResult}");
            }
        }

        private static LoadAssemblyResult LoadAssembly(string name)
        {
            if (AppDomain.CurrentDomain.GetAssemblies().Any(a => a.GetName().Name == name)) {
                return LoadAssemblyResult.Dupe;
            }
            try {
                var asm = AssemblyLoadContext.Default.LoadFromAssemblyPath(Path.Combine(Environment.CurrentDirectory, name + ".dll"));
                foreach (var module in asm.Modules) {
                    RuntimeHelpers.RunModuleConstructor(module.ModuleHandle);
                }
                return LoadAssemblyResult.Success;
            }
            catch (FileNotFoundException) {
                return LoadAssemblyResult.Fail;
            }
        }

        private static string Process(string connectionString, ConnectorCore factory)
        {
            if (!factory.HasConfig || !connectionString.Contains("=(", StringComparison.Ordinal)) {
                return connectionString;
            }
            var config = factory.GetConfig();
            config.ConnectionString = connectionString;
            var keys = config.Keys.Cast<string>().ToArray();
            foreach (var key in keys) {
                var value = config[key] as string;
                if (value == null) {
                    continue;
                }
                if (ProcessStringValue(ref value)) {
                    config[key] = value;
                }
            }
            return config.ConnectionString;
        }

        private static bool ProcessStringValue(ref string value)
        {
            if (!(value.StartsWith("=(") && value.EndsWith(')') && value.Contains(':'))) { 
                return false;
            }

            return ConnectionStringProcessor.Process(ref value);
        }

        private enum LoadAssemblyResult
        {
            Success,
            Fail,
            Dupe
        }
    }
}
