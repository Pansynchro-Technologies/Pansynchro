using System.IO;
using System.Linq;

using Antlr4.Runtime;

using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.Parser
{
	public static class PanSqlData
	{
		public static PanSqlFile ParseFile(string filename)
		{ 
			var result = Parse(File.ReadAllText(filename));
			result.Filename = filename;
			return result;
		}

		public static PanSqlFile Parse(string data)
		{
			var reader = new StringReader(data);
			var stream = new AntlrInputStream(reader);
			var lexer = new PanSqlLexer(stream);
			var parser = new PanSqlParser(new CommonTokenStream(lexer));
			var file = parser.file();
			if (file.exception != null || file.children.OfType<ParserRuleContext>().Any(c => c.exception != null)) {
				throw new CompilerError("Invalid PanSQL syntax", new PanSqlFile([]));
			}
			var result = (PanSqlFile)file.Accept(new PanSqlParserVisitor());
			return result;
		}
	}
}
