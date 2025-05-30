using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal class AggregateStreamModel(DataModel model) : StreamedSqlModel(model)
	{
		// Generates an aggregated stream from a stream
		public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes)
		{
			var filters = new List<string>();
			var methodName = cb.NewNameReference("Transformer");
			List<CSharpStatement> methodBody = [.. InvokeCtes(ctes), new VarDecl("result", new CSharpStringExpression($"new object[{Model.Outputs.Length}]")) ];
			HasCtes = ctes.Count > 0;
			if (HasCtes) {
				var query = BuildLinqExpression(Model);
				methodBody.Add(new CSharpStringExpression($"var __preAgg = {query}"));
			}
			var aggs = new Dictionary<AggregateExpression, string>();
			foreach (var agg in Model.AggOutputs) {
				methodBody.Add(BuildAggregateProcessor(cb, agg, Model.GroupKey, aggs));
			}
			var loopBody = new List<CSharpStatement>();
			WriteJoins(indices, filters, loopBody);
			if (Model.Filter != null) {
				filters.Add(GetInput(Model.Filter));
			}
			if (filters.Count > 0) {
				loopBody.Add(new CSharpStringExpression($"if (!(({string.Join(") && (", filters)}))) continue"));
			}
			foreach (var agg in Model.AggOutputs) {
				if (agg.Name == "Count") {
					loopBody.Add(new CSharpStringExpression($"{aggs[agg]}.Add({GetFieldSet(Model.GroupKey!)})"));
				} else {
					loopBody.Add(new CSharpStringExpression($"{aggs[agg]}.Add({GetFieldSet(Model.GroupKey!)}, {GetInput(agg.Args[0])})"));
				}
			}
			if (HasCtes) {
				methodBody.Add(new ForeachLoop("__item", "__preAgg", loopBody));
				imports.Add("System.Linq");
			} else {
				methodBody.Add(new WhileLoop(new CSharpStringExpression("r.Read()"), loopBody));
			}
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (Model.Outputs[i].IsLiteral) {
					methodBody.Add(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}"));
				}
			}
			loopBody = [];
			if (Model.AggFilter != null) {
				loopBody.Add(WriteHavingClause(Model.AggFilter));
			}
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (!Model.Outputs[i].IsLiteral) {
					loopBody.Add(new CSharpStringExpression($"result[{i}] = {GetAggInput(Model.Outputs[i])}"));
				}
			}
			loopBody.Add(new YieldReturn(new ReferenceExpression("result")));
			methodBody.Add(new ForeachLoop("pair", GetAggIterator(Model.AggOutputs, aggs), loopBody));
			return new Method("private", methodName.Name, "IEnumerable<object?[]>", "IDataReader r", methodBody);
		}

		private static string GetAggIterator(AggregateExpression[] aggOutputs, Dictionary<AggregateExpression, string> aggs)
			=> aggOutputs.Length == 1 ? aggs[aggOutputs[0]] : $"Aggregates.Combine({string.Join(", ", aggOutputs.Select(a => aggs[a]))})";
	}
}
