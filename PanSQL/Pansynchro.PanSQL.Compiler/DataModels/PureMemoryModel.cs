using System.Collections.Generic;
using System.Linq;

namespace Pansynchro.PanSQL.Compiler.DataModels;
internal class PureMemoryModel(DataModel model) : MemorySqlModel(model)
{
	public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes)
	{
		var methodName = cb.NewNameReference("Cte");
		List<CSharpStatement> methodBody = [.. InvokeCtes(ctes)];
		var query = BuildLinqExpression(Model);
		var ctorCall = new CallExpression(new($"new DB.{Model.OutputTable}_"), [.. Model.Outputs.Select(i => new CSharpStringExpression(GetInput(i)))]);
		var addCall = new CallExpression(new MemberReferenceExpression(new("__db." + Model.OutputTable!), "Add"), [ctorCall]);
		var loop = new ForeachLoop("__item", query.ToString()!, new([addCall]));
		methodBody.Add(loop);
		return new Method("private", methodName.Name, "void", "", methodBody);
	}

	public override string? Validate() => null;
}