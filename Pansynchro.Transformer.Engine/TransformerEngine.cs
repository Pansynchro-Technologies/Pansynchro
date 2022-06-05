using System;
using System.IO;
using System.Linq;

using Boo.Lang.Interpreter;

using Pansynchro.Core;
using Pansynchro.Transformer.Macros;

namespace Pansynchro.Transformer.Engine
{

	public class TransformerEngine
    {
		private readonly InteractiveInterpreter _interpreter = new();

        public TransformerEngine()
        {
            _interpreter.References.Add(typeof(TransformerMacro).Assembly);
            _interpreter.Eval("import Pansynchro.Transformer.Macros");
        }

        public ITransformer Transform(string path)
        {
            var contents = File.ReadAllText(path);
            var expansion = _interpreter.Eval(contents);
            var result = expansion.GeneratedAssembly.GetTypes().First(t => typeof(ITransformer).IsAssignableFrom(t));
            return (ITransformer)Activator.CreateInstance(result);
        }
    }
}
