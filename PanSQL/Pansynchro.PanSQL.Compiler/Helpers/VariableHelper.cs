using System;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;

namespace Pansynchro.PanSQL.Compiler.Helpers
{
	internal static class VariableHelper
	{
		public static StreamDefinition GetStream(Variable vbl) => vbl.Declaration switch {
			VarDeclaration vd => vd.Stream!,
			SqlTransformStatement sql => sql.Ctes.First(c => c.Name == vbl.Name).Stream,
			_ => throw new NotImplementedException()
		};

		public static Variable? GetInputStream(SqlTransformStatement node, Ast.PanSqlFile file)
		{
			if (node.Tables[0].Type == "Cte") {
				var result = node.Ctes.SelectMany(cte => cte.Model.Model.Inputs).FirstOrDefault(i => i.Type == TableType.Stream);
				return result == null ? null : file.Vars[result.Name];
			}
			return node.Tables[0];
		}
	}
}
