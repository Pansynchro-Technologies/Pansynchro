using System;
using System.Collections.Generic;

using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal class BuildDatabase : VisitorCompileStep
	{
		public override void OnVarDeclaration(VarDeclaration node)
		{
			if (node.Type == VarDeclarationType.Table) {
				var table = TypesHelper.BuildDataClass(node.Stream);
				_file.Database.Add(table);
			}
		}

		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies() => [Dependency<BindTypes>()];
	}
}
