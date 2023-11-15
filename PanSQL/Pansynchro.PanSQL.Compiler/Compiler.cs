using System;
using System.IO;
using System.Linq;

using Pansynchro.PanSQL.Compiler.Parser;

namespace Pansynchro.PanSQL.Compiler
{
	public class Compiler
	{
		public Script Compile(string name, string script, string? basePath = null)
		{
			var file = PanSqlData.Parse(script);
			var pipeline = Pipeline.Build();
			return pipeline.Process(name, file, basePath ?? Environment.CurrentDirectory);
		}

		public Script CompileFile(string filename, string basePath)
		{
			var file = PanSqlData.ParseFile(filename);
			var pipeline = Pipeline.Build();
			return pipeline.Process(Path.GetFileNameWithoutExtension(filename), file, basePath);
		}

		public Script[] CompileFiles(string basePath, params string[] filenames) => filenames
			.SelectMany(filename => Directory.GetFiles(Path.GetDirectoryName(Path.GetFullPath(filename))!, Path.GetFileName(filename)))
			.Select(filename => CompileFile(filename, basePath))
			.ToArray();
	}
}
