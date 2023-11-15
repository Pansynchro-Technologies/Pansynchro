using System;
using System.Collections.Generic;

using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal abstract class CompileStep
	{
		protected CodeBuilder CodeBuilder { get; private set; } = null!;
		protected string BasePath { get; private set; }

		public void Initialize(CodeBuilder cb, string basePath)
		{
			CodeBuilder = cb;
			BasePath = basePath;
		}

		public abstract void Execute(PanSqlFile f);

		protected static (Type, Func<CompileStep>) Dependency<T>() where T : CompileStep, new()
			=> (typeof(T), () => new T());

		public virtual IEnumerable<(Type, Func<CompileStep>)> Dependencies()
		{
			yield break;
		}
	}
}
