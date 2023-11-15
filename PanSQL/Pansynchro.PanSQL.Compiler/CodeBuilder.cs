using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler
{
	internal class CodeBuilder
	{
		private int _nameCounter;

		internal Identifier NewNameReference(string n)
		{
			++_nameCounter;
			return new Identifier($"{n}__{_nameCounter}");
		}
	}
}
