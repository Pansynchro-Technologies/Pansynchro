using System.Collections.Generic;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	class IterateStreamModel(DataModel model) : StreamedSqlModel(model)
	{
		public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports)
		{
			var filters = new List<string>();
			var methodName = cb.NewNameReference("Transformer");
			var methodBody = new List<CSharpStatement> { new VarDecl("result", new CSharpStringExpression($"new object[{Model.Outputs.Length}]")) };
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (Model.Outputs[i].IsLiteral) {
					methodBody.Add(new ExpressionStatement(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}")));
				}
			}
			var whileBody = new List<CSharpStatement>();
			WriteJoins(indices, filters, whileBody);
			if (Model.Filter != null) {
				filters.Add(GetInput(Model.Filter));
			}
			if (filters.Count > 0) {
				whileBody.Add(new ExpressionStatement(new CSharpStringExpression($"if (!(({string.Join(") && (", filters)}))) continue")));
			}
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (!Model.Outputs[i].IsLiteral) {
					whileBody.Add(new ExpressionStatement(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}")));
				}
			}
			whileBody.Add(new YieldReturn(new ReferenceExpression("result")));
			methodBody.Add(new WhileLoop(new CSharpStringExpression("r.Read()"), whileBody));
			return new Method("private", methodName.Name, "IEnumerable<object?[]>", "IDataReader r", methodBody);
		}
	}
}
