using System;
using System.IO;
using System.Linq;

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
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

		private static (Exception, ParserRuleContext)? ParseError(ParserRuleContext context)
		{
			if (context.exception != null) {
				return (context.exception, context);
			}
			foreach (var child in context.children.OfType<ParserRuleContext>()) {
				var result = ParseError(child);
				if (result != null) {
					return result;
				}
			}
			return null;
		}

		public static PanSqlFile Parse(string data)
		{
			var reader = new StringReader(data);
			var stream = new AntlrInputStream(reader);
			var lexer = new PanSqlLexer(stream);
			var cts = new CommonTokenStream(lexer);
			var parser = new PanSqlParser(cts);
			var file = parser.file();
			var err = ParseError(file);
			if (err != null) {
				throw new CompilerError("Invalid PanSQL syntax", new PanSqlFile([]));
			}
			var result = (PanSqlFile)file.Accept(new PanSqlParserVisitor(cts));
			return result;
		}
	}
}
