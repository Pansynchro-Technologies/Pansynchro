using Antlr4.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            var file = parser.file();
            var result = (PansyncFile)file.Accept(new PansyncVisitor());
            return result;
        }
    }
}
