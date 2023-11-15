using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal class AggregateStreamGeneratorModel(DataModel model) : MemorySqlModel(model)
	{
		public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports)
		{
			var methodName = cb.NewNameReference("Transformer");
			var methodBody = new List<CSharpStatement> { new VarDecl("result", new CSharpStringExpression($"new object[{Model.Outputs.Length}]")) };
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
					forBody.Add(new CSharpStringExpression($"{aggs[agg]}.Add({GetFieldSet(Model.GroupKey!)}, {GetInput(agg.Arg)})"));
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
			return new Method("private", methodName.Name, "IEnumerable<object?[]>", null, methodBody);
		}

		private static string GetAggIterator(AggregateExpression[] aggOutputs, Dictionary<AggregateExpression, string> aggs)
			=> aggOutputs.Length == 1 ? aggs[aggOutputs[0]] : $"Aggregates.Combine({string.Join(", ", aggOutputs.Select(a => aggs[a]))})";

		private string GetFieldSet(MemberReferenceExpression[] group)
			=> group.Length == 1 ? GetInput(group[0]) : $"({string.Join(", ", group.Select(GetInput))})";
		private CSharpStatement WriteHavingClause(BooleanExpression having)
		{
			var filter = GetAggInput(having);
			return new CSharpStringExpression($"if (!({filter})) continue");
		}

		private DbExpression GetAggInput(DbExpression expr) => expr switch {
			BooleanExpression be => new BooleanExpression(be.Op, GetAggInput(be.Left), GetAggInput(be.Right)),
			AggregateExpression agg => GetAggInput(agg),
			AliasedExpression ae => GetAggInput(ae.Expr),
			MemberReferenceExpression mre => GetAggInput(mre),
			LiteralExpression => expr,
			_ => throw new NotImplementedException()
		};

		private CSharpStringExpression GetAggInput(MemberReferenceExpression mre)
		{
			for (int i = 0; i < Model.GroupKey!.Length; ++i) {
				if (mre.Match(Model.GroupKey[i])) {
					return new CSharpStringExpression(Model.GroupKey.Length == 1 ? "pair.Key" : $"pair.Key.Item{i + 1}");
				}
			}
			throw new Exception($"Field '{mre}' does not match any field declared in the GROUP BY clause");
		}

		private CSharpStringExpression GetAggInput(AggregateExpression agg)
		{
			for (int i = 0; i < Model.AggOutputs.Length; ++i) {
				if (agg.Match(Model.AggOutputs[i])) {
					return new CSharpStringExpression(Model.AggOutputs.Length == 1 ? "pair.Value" : $"pair.Value.Item{i + 1}");
				}
			}
			throw new Exception($"HAVING aggregate '{agg}' does not match any aggregate declared in the SELECT clause");
		}

		private static VarDecl BuildAggregateProcessor(
			CodeBuilder cb,
			AggregateExpression agg,
			MemberReferenceExpression[] groupKey,
			Dictionary<AggregateExpression, string> aggs)
		{
			var name = cb.NewNameReference("aggregator");
			aggs.Add(agg, name.Name);
			var callName = "Aggregates." + agg.Name;
			var typeArgs = string.Join(", ", groupKey.Select(mre => TypesHelper.FieldTypeToCSharpType(mre.Type!)));
			var typeArg2 = TypesHelper.FieldTypeToCSharpType(agg.Type!);
			var invocation = agg.Name == "Count" ? $"{callName}<{typeArgs}>()" : $"{callName}<{typeArgs}, {typeArg2}>()";
			return new VarDecl(name.Name, new CSharpStringExpression(invocation));
		}

		public override string? Validate() => null;
	}
}