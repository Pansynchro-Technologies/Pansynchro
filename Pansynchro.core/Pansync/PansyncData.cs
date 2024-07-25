using Antlr4.Runtime;
using System.IO;

namespace Pansynchro.Core.Pansync
{
	public static class PansyncData
	{
		public static PansyncFile ParseFile(string filename) => Parse(File.ReadAllText(filename));

		public static PansyncFile Parse(string data)
		{
			var reader = new StringReader(data);
			var stream = new AntlrInputStream(reader);
			var lexer = new PansyncLexer(stream);
			var parser = new PansyncParser(new CommonTokenStream(lexer));
			parser.RemoveErrorListeners();
			var file = parser.file();
			var result = (PansyncFile)file.Accept(new PansyncVisitor());
			return result;
		}
	}
}
