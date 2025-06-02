using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal class StreamToAggregateMemoryModel : StreamedSqlModel
	{
		public StreamToAggregateMemoryModel(DataModel model) : base(model)
		{}

		public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes)
		{
			if (ctes.Count > 0) {
				throw new NotImplementedException();
			}
			var filters = new List<string>();
			var methodName = cb.NewNameReference("Transformer");
			var methodBody = new List<CSharpStatement>();
			var aggs = new Dictionary<AggregateExpression, string>();
			foreach (var agg in Model.AggOutputs) {
				methodBody.Add(BuildAggregateProcessor(cb, agg, Model.GroupKey!, aggs));
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
			methodBody.Add(new WhileLoop(new CSharpStringExpression("r.Read()"), loopBody));
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (Model.Outputs[i].IsLiteral) {
					methodBody.Add(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}"));
				}
			}
			var tableName = Model.OutputTable ?? throw new Exception("No memory table name is bound to this model");
			var arglist = Model.AggOutputs.Length == 1 
				? "pair.Value"
				: string.Join(", ", Enumerable.Range(1, Model.AggOutputs.Length).Select(i => $"pair.Value.Item{i}"));
			var forBody = new List<CSharpStatement>();
			if (Model.AggFilter != null) {
				forBody.Add(WriteHavingClause(Model.AggFilter));
			}
			forBody.Add(new CSharpStringExpression($"__db.{tableName}.Add(new DB.{tableName}_(pair.Key, {arglist}))"));
			methodBody.Add(new ForeachLoop("pair", GetAggIterator(Model.AggOutputs, aggs), forBody));
			return new Method("private", methodName.Name, "void", "IDataReader r", methodBody);
		}

		private static string GetAggIterator(AggregateExpression[] aggOutputs, Dictionary<AggregateExpression, string> aggs)
			=> aggOutputs.Length == 1 ? aggs[aggOutputs[0]] : $"Aggregates.Combine({string.Join(", ", aggOutputs.Select(a => aggs[a]))})";	}
}
