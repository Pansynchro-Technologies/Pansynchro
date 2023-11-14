using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using Pansynchro.Core.Helpers;
using Pansynchro.Core.Pansync;

namespace Pansynchro.Core.Connectors
{
	static class ConnectorLoader
	{
		[ModuleInitializer]
		internal static void Load()
		{
			if (!File.Exists(ConnectorRegistry.CONNECTOR_FILE)) return;
			var connectors = File.ReadAllText(ConnectorRegistry.CONNECTOR_FILE);
			var parsed = PansyncData.Parse(connectors);
			var ast = parsed.Body;
			var connDescs = ast.OfType<Command>().First(ms => ms.Name == "Connectors")
				.Body!
				.OfType<Command>()
				.Select(ParseConnector).ToArray();
			ConnectorRegistry.LoadConnectors(connDescs);
			var sourceDescs = ast.OfType<Command>().First(ms => ms.Name == "DataSources")
				.Body!
				.OfType<Command>()
				.Select(ParseDataSource).ToArray();
			ConnectorRegistry.LoadDataSources(sourceDescs);
			var procDescs = ast.OfType<Command>().First(ms => ms.Name == "DataProcessors")
				.Body!
				.OfType<Command>()
				.Select(ParseDataProcessor).ToArray();
			ConnectorRegistry.LoadDataProcessors(procDescs);
		}

		private static readonly NameNode REQ_DATA_SOURCE = new("DataSource");

		private static ConnectorDescription ParseConnector(Command conn)
		{
			if (conn.Name != "Connector" || conn.Arguments.Length != 1) {
				throw new InvalidDataException($"Unknown connector statement: {conn.Name} {string.Join<Expression>(", ", conn.Arguments)}");
			}
			var name = GetName(conn.Arguments[0]);
			var items = conn.Body;
			if (items == null) {
				throw new InvalidDataException($"Missing connector description for {name}");
			}
			var supports = items.OfType<Command>().SingleOrDefault(m => m.Name == "supports");
			if (supports == null) {
				throw new InvalidDataException($"Missing 'supports' statement for {name}");
			}
			var requires = items.OfType<Command>().SingleOrDefault(m => m.Name == "requires");
			var requiresDS = requires?.Arguments.Length == 1 && requires.Arguments[0].Matches(REQ_DATA_SOURCE);
			var support = ParseSupports(supports);
			var assembly = items.OfType<Command>().Single(m => m.Name == "assembly");
			if (assembly.Arguments.Length != 1 || !(assembly.Arguments[0] is NameNode)) {
				throw new InvalidDataException($"Unknown assembly directive: {assembly}");
			}
			return new ConnectorDescription(name, assembly.Arguments[0].ToString(), support, requiresDS);
		}

		private static string GetName(Expression expr)
		{
			if (expr is StringNode sl) {
				return sl.Value;
			}
			if (expr is NameNode r) {
				return r.Name;
			}
			throw new Exception("This should not happen");
		}

		private static SourceDescription ParseDataSource(Command source)
		{
			if (source.Name != "Source" || source.Arguments.Length != 1 || !(source.Arguments[0] is NameNode)) {
				throw new InvalidDataException($"Unknown data source statement: {source.Name} {string.Join<Expression>(", ", source.Arguments)}");
			}
			var name = source.Arguments[0].ToString();
			var asm = source.Body?.OfType<Command>().SingleOrDefault(m => m.Name == "Assembly");
			if (asm == null) {
				throw new InvalidDataException($"Missing Assembly declaration for {name}");
			}
			if (asm.Arguments?.Length != 1 || !(asm.Arguments[0] is NameNode)) {
				throw new InvalidDataException($"Invalid Assembly declaration for {name}");
			}
			var expr = asm.Arguments[0];
			var supports = source.Body!.OfType<Command>().SingleOrDefault(m => m.Name == "Supports");
			if (supports == null) {
				throw new InvalidDataException($"Missing 'supports' statement for {name}");
			}
			var support = ParseSourceSupports(supports);
			return new SourceDescription(name, expr.ToString(), support);
		}

		private static SourceDescription ParseDataProcessor(Command proc)
		{
			if (proc.Name != "Source" || proc.Arguments.Length != 1 || !(proc.Arguments[0] is NameNode)) {
				throw new InvalidDataException($"Unknown data source statement: {proc.Name} {string.Join<Expression>(", ", proc.Arguments)}");
			}
			var name = proc.Arguments[0].ToString();
			var asm = proc.Body?.OfType<Command>().SingleOrDefault(m => m.Name == "Assembly");
			if (asm == null) {
				throw new InvalidDataException($"Missing Assembly declaration for {name}");
			}
			if (asm.Arguments?.Length != 1 || !(asm.Arguments[0] is NameNode)) {
				throw new InvalidDataException($"Invalid Assembly declaration for {name}");
			}
			var expr = asm.Arguments[0];
			return new SourceDescription(name, expr.ToString(), SourceCapabilities.Source | SourceCapabilities.Sink);
		}

		private static Capabilities ParseSupports(Command stmt)
		{
			var args = stmt.Arguments;
			if (!args.All(e => e is NameNode)) {
				throw new InvalidDataException($"Unknown supports statement: {stmt}");
			}
			var result = Capabilities.None;
			foreach (var item in args.Cast<NameNode>()) {
				result |= Enum.Parse<Capabilities>(item.Name);
			}
			return result;
		}

		private static SourceCapabilities ParseSourceSupports(Command stmt)
		{
			var args = stmt.Arguments;
			if (!args.All(e => e is NameNode)) {
				throw new InvalidDataException($"Unknown supports statement: {stmt}");
			}
			var result = SourceCapabilities.None;
			foreach (var item in args.Cast<NameNode>()) {
				result |= Enum.Parse<SourceCapabilities>(item.Name);
			}
			return result;
		}

		public static string WriteConnectors(string[] connNames, string[]? sourceNames = null, string[]? procNames = null)
		{
			var connectors = connNames.Select(ConnectorRegistry.GetConnectorDescription).WhereNotNull().ToArray();
			var sources = sourceNames?.Select(ConnectorRegistry.GetDataSourceDescription)?.WhereNotNull()?.ToArray();
			var processors = procNames?.Select(ConnectorRegistry.GetDataProcessorDescription)?.WhereNotNull()?.ToArray();
			var lines = new List<Statement>();
			lines.Add(new Command("Connectors", body: connectors.Select(WriteConnector).ToArray()));
			if (sources != null) {
				lines.Add(new Command("DataSources", body: sources.Select(WriteSource).ToArray()));
			}
			if (processors != null) {
				lines.Add(new Command("DataProcessors", body: processors.Select(WriteProcessor).ToArray()));
			}
			var file = new PansyncFile(lines.ToArray());
			return file.ToString();
		}

		private static Statement WriteConnector(ConnectorDescription desc)
		{
			var body = new List<Statement>();
			body.Add(WriteSupports(desc.Capabilities));
			if (desc.RequiresDataSource) {
				body.Add(new Command("requires", new[] { REQ_DATA_SOURCE }));
			}
			body.Add(new Command("assembly", new[] { new NameNode(desc.Assembly) }));
			var conn = new Command("Connector", new[] { new NameNode(desc.Name) }, body: body.ToArray());
			return conn;
		}

		private static Statement WriteSupports(Capabilities capabilities)
		{
			var exprs = new List<Expression>();
			if (capabilities.HasFlag(Capabilities.Analyzer)) {
				exprs.Add(new NameNode("Analyzer"));
			}
			if (capabilities.HasFlag(Capabilities.Reader)) {
				exprs.Add(new NameNode("Reader"));
			}
			if (capabilities.HasFlag(Capabilities.Writer)) {
				exprs.Add(new NameNode("Writer"));
			}
			if (capabilities.HasFlag(Capabilities.Configurator)) {
				exprs.Add(new NameNode("Configurator"));
			}
			if (capabilities.HasFlag(Capabilities.Queryable)) {
				exprs.Add(new NameNode("Queryable"));
			}
			return new Command("supports", exprs.ToArray());
		}

		private static Statement WriteSource(SourceDescription description)
		{
			var body = new List<Statement>() {
				new Command("Assembly", new[]{new NameNode(description.Assembly) }),
				WriteSourceSupports(description.Capabilities)
			};
			return new Command("Source", new[] { new NameNode(description.Name) }, body: body.ToArray());
		}

		private static Statement WriteSourceSupports(SourceCapabilities capabilities)
		{
			var exprs = new List<Expression>();
			if (capabilities.HasFlag(SourceCapabilities.Source)) {
				exprs.Add(new NameNode("Source"));
			}
			if (capabilities.HasFlag(SourceCapabilities.Sink)) {
				exprs.Add(new NameNode("Sink"));
			}
			return new Command("Supports", exprs.ToArray());
		}

		private static Statement WriteProcessor(SourceDescription description, int arg2)
		{
			var body = new List<Statement>() {
				new Command("Assembly", new[]{new NameNode(description.Assembly) })
			};
			return new Command("Source", new[] { new NameNode(description.Name) }, body: body.ToArray());
		}
	}
}
