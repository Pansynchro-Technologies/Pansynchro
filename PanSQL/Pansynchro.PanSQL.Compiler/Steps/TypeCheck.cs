using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.Core.Connectors;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	using LiteralExpression = DataModels.LiteralExpression;

	internal class TypeCheck : VisitorCompileStep
	{
		private readonly List<string> _readers = [];
		private readonly List<string> _writers = [];
		private readonly List<string> _analyzers = [];
		private readonly List<string> _sources = [];
		private readonly List<string> _sinks = [];

		public override void Execute(PanSqlFile f)
		{
			_readers.AddRange(ConnectorRegistry.ReaderTypes);
			_writers.AddRange(ConnectorRegistry.WriterTypes);
			_analyzers.AddRange(ConnectorRegistry.AnalyzerTypes);
			_sources.AddRange(ConnectorRegistry.DataSourceTypes);
			_sinks.AddRange(ConnectorRegistry.DataSinkTypes);
			base.Execute(f);
		}

		public override void OnSqlStatement(SqlTransformStatement node)
		{
			var validation = node.DataModel.Validate();
			if (validation != null) {
				throw new CompilerError(validation, node);
			}
			var output = GetStream(((VarDeclaration)node.Output.Declaration)).Fields;
			var model = node.DataModel.Model;
			var fields = model.Outputs;
			node.DataModel.Model = model with { Outputs = DoTypeCheck(fields, output, node) };
			DoTypeCheck(model.Filter, node);
			DoTypeCheck(model.AggFilter, node);
			DoTypeCheck(model.Joins, node);
			if (node.DataModel.Model.AggOutputs?.Length > 0) {
				ValidateGrouping(node.DataModel.Model, node);
			}
		}

		private static StreamDefinition GetStream(VarDeclaration decl) => decl.Stream!;

		private static void ValidateGrouping(DataModel model, SqlTransformStatement node)
		{
			var outputs = new HashSet<string>();
			foreach (var o in model.Outputs) {
				GetField(o, outputs);
			}
			if (model.GroupKey != null) { 
				foreach (var field in model.GroupKey) {
					outputs.Remove(field.ToString());
				}
			}
			var used = new HashSet<string>();
			foreach (var agg in model.AggOutputs) {
				GetField(agg.Args[0], used);
			}
			outputs = outputs.Except(used).ToHashSet();
			if (outputs.Count > 0) {
				throw new CompilerError($"The following field(s) must be contained in either an aggregate expression or the GROUP BY clause: {string.Join(", ", outputs)}", node);
			}
		}

		private static DbExpression[] DoTypeCheck(DbExpression[] input, FieldDefinition[] output, SqlTransformStatement node)
		{
			var required = output.Where(f => !f.Type.Nullable).ToDictionary(n => n.Name, StringComparer.InvariantCultureIgnoreCase);
			var available = output.Select((f, i) => KeyValuePair.Create(f, i)).ToDictionary(n => n.Key.Name, StringComparer.InvariantCultureIgnoreCase);
			var used = new HashSet<string>();
			var result = new DbExpression[output.Length];
			for (int i = 0; i < input.Length; ++i) {
				var value = input[i];
				var name = value is LiteralExpression ? output[i].Name : GetName(value) ?? output[i].Name;
				if (available.TryGetValue(name, out var field)) {
					used.Add(name);
					available.Remove(name);
					required.Remove(name);
					DoTypeCheck(value, field.Key, node);
					if (result[field.Value] == null) {
						result[field.Value] = value;
					} else {
						throw new CompilerError($"Output field '{name}' cannot be assigned more than one value.", node);
					}
				} else if (used.Contains(name)) {
					throw new CompilerError($"Output field '{name}' cannot be assigned more than one value.", node);
				} else {
					throw new CompilerError($"'{node.Output.Name}' does not contain a field named '{name}'.", node);
				}
			}
			if (required.Count > 0) {
				throw new CompilerError($"The following field(s) on {node.Output.Name} are not nullable, but are not assigned a value: {string.Join(", ", required.Keys)}", node);
			}
			for (int i = 0; i < result.Length; ++i) {
				if (result[i] == null) {
					result[i] = new NullLiteralExpression();
				}
			}
			return result;
		}

		private static void DoTypeCheck(DbExpression value, FieldDefinition field, SqlTransformStatement node)
		{
			var inType = value.Type ?? throw new CompilerError($"No type has been bound for '{value}'.", node);
			var outType = field.Type;
			if (inType == TypesHelper.NullType) {
				if (outType.Nullable) {
					return;
				}
				throw new CompilerError($"Unable to insert NULL literal into non-nullable column '{field.Name}'", node);
			}
			var typeResult = DataDictionaryComparer.TypeCheckField(inType, outType, value.ToString()!, false);
			if (typeResult is null or PromotionLine) {
				return;
			}
			if (typeResult is ComparisonError ce) {
				throw new CompilerError(ce.Message, node);
			}
			throw new NotImplementedException();
		}

		private void DoTypeCheck(DbExpression? filter, SqlTransformStatement node)
		{
			if (filter is BooleanExpression b) {
				DoTypeCheck(b.Left, node);
				DoTypeCheck(b.Right, node);
				var typeResult = DataDictionaryComparer.TypeCheckField(b.Left.Type!, b.Right.Type!, b.ToString()!, false);
				if (typeResult is null or PromotionLine) {
					return;
				}
				if ((typeResult is ComparisonError) && (DataDictionaryComparer.TypeCheckField(b.Right.Type!, b.Left.Type!, b.ToString()!, false) is ComparisonError)) {
					throw new CompilerError($"Incompatible types in expression '{b}':  '{b.Left.Type}' and '{b.Right.Type}'", node);
				}
			}
		}

		private void DoTypeCheck(JoinSpec[] joins, SqlTransformStatement node)
		{
			foreach (var join in joins) {
				DoTypeCheck(join.Condition, node);
			}
		}

		private static string? GetName(DbExpression field) => field switch {
			ReferenceExpression re => re.Name,
			AliasedExpression a => a.Alias,
			AggregateExpression ag => GetName(ag.Args[0]),
			CountExpression => "Count",
			CallExpression or BinaryExpression => null,
			_ => throw new NotImplementedException()
		};

		private static void GetField(DbExpression field, ICollection<string> fields)
		{
			switch (field) {
				case ReferenceExpression re: fields.Add(re.ToString()); break;
				case AliasedExpression a: GetField(a.Expr, fields); break;
				case AggregateExpression ag: GetField(ag.Args[0], fields); break;
				case CountExpression: fields.Add("__Count__"); break;
				case BinaryExpression bin: GetField(bin.Left, fields); GetField(bin.Right, fields); break;
				case BooleanExpression bo: GetField(bo.Left, fields); GetField(bo.Right, fields); break;
				case CallExpression call:
					foreach (var arg in call.Args) {
						GetField(arg, fields);
					}
					break;
				case LiteralExpression: break;
				default: throw new NotImplementedException();
			}
		}

		public override void OnOpenStatement(OpenStatement node)
		{
			var list = node.Type switch {
				OpenType.Read => _readers,
				OpenType.Write => _writers,
				OpenType.Analyze => _analyzers,
				OpenType.Source => _sources,
				OpenType.Sink => _sinks,
				_ => throw new NotImplementedException(),
			};
			var idx = list.FindIndex(n => n.Equals(node.Connector, StringComparison.InvariantCultureIgnoreCase));
			if (idx == -1) {
				throw new CompilerError($"No connector named '{node.Connector}' with {node.Type} ability is registered in connectors.pansync", node);
			}
			var conn = list[idx];
			if (conn != node.Connector) {
				node.Connector = conn;
			}
			if (node.Source != null) {
				TypeCheckSource(node);
			} else {
				var desc = ConnectorRegistry.GetConnectorDescription(node.Connector); 
				if (desc?.RequiresDataSource == true) {
					throw new CompilerError($"The '{node.Connector}' connector requires a data source.", node);
				}
			}
			Visit(node.Creds);
		}

		private void TypeCheckSource(OpenStatement node)
		{
			if (node.Type is not (OpenType.Read or OpenType.Write or OpenType.Analyze)) {
				throw new CompilerError($"The connector '{node.Name}' is a {node.Type} and cannot be connected to a Source or Sink.", node);
			}
			var source = _file.Vars[node.Source!.Name];
			var expectedType = node.Type switch {
				OpenType.Read or OpenType.Analyze => "Source",
				OpenType.Write => "Sink",
				_ => throw new NotImplementedException()
			};
			if (source.Type != expectedType) {
				throw new CompilerError($"The connector '{node.Name}' requires a {expectedType}, but '{node.Source}' is a {source.Type}.", node);
			}
			var desc = ConnectorRegistry.GetConnectorDescription(node.Connector);
			if (desc?.RequiresDataSource != true) {
				throw new CompilerError($"The '{node.Connector}' connector does not use a data source.", node);
			}
			var sourceName = ((OpenStatement)source.Declaration).Connector;
			var sDesc = ConnectorRegistry.GetDataSourceDescription(sourceName);
			if (expectedType == "Source" && !sDesc!.Capabilities.HasFlag(SourceCapabilities.Source)) {
				throw new CompilerError($"'{sourceName}' does not provide a data source.", node);
			} else if (expectedType == "Sink" && !sDesc!.Capabilities.HasFlag(SourceCapabilities.Sink)) {
				throw new CompilerError($"'{sourceName}' does not provide a data sink.", node);
			}
		}

		public override void OnCredentialExpression(CredentialExpression node)
		{
			base.OnCredentialExpression(node);
			if (TypesHelper.FieldTypeToCSharpType(node.Value.ExpressionType) != "string") {
				throw new CompilerError($"'{node.Value}' is not a string type", node);
			}
		}

		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies() => [Dependency<BindTypes>()];
	}
}
