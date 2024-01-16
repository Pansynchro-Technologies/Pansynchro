using System;
using System.Linq;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.Helpers
{
	internal static class VariableHelper
	{
		public static StreamDefinition GetStream(Variable vbl) => vbl.Declaration switch {
			VarDeclaration vd => vd.Stream!,
			SqlTransformStatement sql => sql.Ctes.First(c => c.Name == vbl.Name).Stream,
			_ => throw new NotImplementedException()
		};
	}
}
