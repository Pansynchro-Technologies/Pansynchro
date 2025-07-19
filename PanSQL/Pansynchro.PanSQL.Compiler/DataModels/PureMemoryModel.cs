using System.Collections.Generic;
using System.Linq;

namespace Pansynchro.PanSQL.Compiler.DataModels;
internal class PureMemoryModel(DataModel model, bool isExists) : MemorySqlModel(model)
{
	private readonly bool _isExists = isExists;

	public override Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes)
	{
		var methodName = cb.NewNameReference(_isExists ? "Exists" :"Cte");
		List<CSharpStatement> methodBody = [.. InvokeCtes(ctes)];
		var query = BuildLinqExpression(Model);
		imports.Add("System.Linq");
		var ctorCall = new CallExpression(new($"new DB.{Model.OutputTable}_"), [.. Model.Outputs.Select(i => new CSharpStringExpression(GetInput(i)))]) { CallType = CallType.StaticMethod };
		CSharpStatement yieldCall = _isExists ? new ReturnStatement(new BooleanLiteralExpression(true)) : new YieldReturn(ctorCall);
		var loop = new ForeachLoop("__item", query.ToString()!, new([yieldCall]));
		methodBody.Add(loop);
		if (_isExists) {
			methodBody.Add(new ReturnStatement(new BooleanLiteralExpression(false)));
		}
		return new Method(_isExists ? "internal static" : "private", methodName.Name, _isExists ? "bool" : $"IEnumerable< DB.{Model.OutputTable}_>", "", methodBody);
	}

	public override string? Validate() => null;
}