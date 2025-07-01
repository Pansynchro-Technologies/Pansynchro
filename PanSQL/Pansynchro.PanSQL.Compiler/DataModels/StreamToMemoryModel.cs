using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels;
internal class StreamToMemoryModel(DataModel model) : StreamedSqlModel(model)
{
	public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes)
	{
		var filters = new List<string>();
		var methodName = cb.NewNameReference("Consumer");
		var methodBody = new List<CSharpStatement>([.. InvokeCtes(ctes)]);
		var loopBody = new List<CSharpStatement>();
		WriteJoins(indices, filters, loopBody);
		var tableName = Model.Inputs[0].Stream.Name.ToTableName();
		if (Model.Filter != null) {
			filters.Add(GetInput(Model.Filter));
		}
		if (filters.Count > 0) {
			loopBody.Add(new CSharpStringExpression($"if (!(({string.Join(") && (", filters)}))) continue"));
		}
		loopBody.Add(new CSharpStringExpression($"__db.{tableName}.Add(new DB.{tableName}_(r))"));
		for (int i = 0; i < Model.Outputs.Length; ++i) {
			if (Model.Outputs[i].IsLiteral) {
				methodBody.Add(new CSharpStringExpression($"result[{i}] = {GetInput(Model.Outputs[i])}"));
			}
		}
		methodBody.Add(new WhileLoop(new CSharpStringExpression("r.Read()"), loopBody));
		return new Method("private", methodName.Name, "void", "IDataReader r", methodBody);
	}

	public override string? Validate() => null;
}
