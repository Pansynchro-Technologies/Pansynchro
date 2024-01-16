using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal class AggregateStreamGeneratorModel(DataModel model) : MemorySqlModel(model)
	{
		public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes)
		{
			var methodName = cb.NewNameReference("Transformer");
			List<CSharpStatement> methodBody = [.. InvokeCtes(ctes), new VarDecl("result", new CSharpStringExpression($"new object[{Model.Outputs.Length}]")) ];
			var aggs = new Dictionary<AggregateExpression, string>();
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (Model.Outputs[i].IsLiteral) {
					methodBody.Add(new ExpressionStatement(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}")));
				}
			}
			var query = BuildLinqExpression(Model);
			methodBody.Add(new CSharpStringExpression($"var __preAgg = {query}"));
			foreach (var agg in Model.AggOutputs) {
				methodBody.Add(BuildAggregateProcessor(cb, agg, Model.GroupKey!, aggs));
			}
			var forBody = new List<CSharpStatement>();
			foreach (var agg in Model.AggOutputs) {
				if (agg.Name == "Count") {
					forBody.Add(new CSharpStringExpression($"{aggs[agg]}.Add({GetFieldSet(Model.GroupKey!)})"));
				} else {
					forBody.Add(new CSharpStringExpression($"{aggs[agg]}.Add({GetFieldSet(Model.GroupKey!)}, {GetInput(agg.Args[0])})"));
				}
			}
			methodBody.Add(new ForeachLoop("__item", "__preAgg", forBody));
			forBody = [];
			if (Model.AggFilter != null) {
				forBody.Add(WriteHavingClause(Model.AggFilter));
			}
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (!Model.Outputs[i].IsLiteral) {
					forBody.Add(new ExpressionStatement(new CSharpStringExpression($"result[{i}] = {GetAggInput(Model.Outputs[i])}")));
				}
			}
			forBody.Add(new YieldReturn(new ReferenceExpression("result")));
			methodBody.Add(new ForeachLoop("pair", GetAggIterator(Model.AggOutputs, aggs), forBody));
			imports.Add("System.Linq");
			var input = ctes.Count > 0 ? "IDataReader r" : null;
			return new Method("private", methodName.Name, "IEnumerable<object?[]>", input, methodBody);
		}

		private static string GetAggIterator(AggregateExpression[] aggOutputs, Dictionary<AggregateExpression, string> aggs)
			=> aggOutputs.Length == 1 ? aggs[aggOutputs[0]] : $"Aggregates.Combine({string.Join(", ", aggOutputs.Select(a => aggs[a]))})";

		public override string? Validate() => null;
	}
}