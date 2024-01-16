﻿using System;
using System.Collections.Generic;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal class StreamGeneratorModel(DataModel model) : MemorySqlModel(model)
	{
		public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes)
		{
			var methodName = cb.NewNameReference("Transformer");
			List<CSharpStatement> methodBody = [.. InvokeCtes(ctes), new VarDecl("result", new CSharpStringExpression($"new object[{Model.Outputs.Length}]")) ];
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (Model.Outputs[i].IsLiteral) {
					methodBody.Add(new ExpressionStatement(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}")));
				}
			}
			var query = BuildLinqExpression(Model);
			methodBody.Add(new CSharpStringExpression($"var __resultSet = {query}"));
			var forBody = new List<CSharpStatement>();
			for (int i = 0; i < Model.Outputs.Length; ++i) {
				if (!Model.Outputs[i].IsLiteral) {
					forBody.Add(new ExpressionStatement(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}")));
				}
			}
			forBody.Add(new YieldReturn(new ReferenceExpression("result")));
			methodBody.Add(new ForeachLoop("__item", "__resultSet", forBody));
			imports.Add("System.Linq");
			var input = ctes.Count > 0 ? "IDataReader r" : null;
			return new Method("private", methodName.Name, "IEnumerable<object?[]>", input, methodBody);
		}

		public override string? Validate() => null;
	}
}
