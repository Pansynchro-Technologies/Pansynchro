using System;
using System.Collections.Generic;
using System.Linq;

namespace Pansynchro.PanSQL.Compiler
{
	using Ast;
	using Steps;

	internal class Pipeline
	{
		private readonly List<CompileStep> _steps;
		private readonly CodeBuilder _codeBuilder = new();

		private Pipeline(IEnumerable<CompileStep> steps)
		{
			_steps = steps.ToList();
		}

		internal static Pipeline Build()
		{
			var steps = BuildDependencyChain();
			var result = new Pipeline(OrderByDependencies(steps));
			if (result._steps[^1] is not Codegen) {
				throw new Exception("Pipeline dependencies are missing");
			}
			return result;
		}

		private static CompileStep[] BuildDependencyChain()
		{
			var map = new Dictionary<Type, CompileStep>();
			Queue<(Type, Func<CompileStep>)> openList = new();
			openList.Enqueue((typeof(Codegen), () => new Codegen()));
			while (openList.Count > 0) {
				var (type, creator) = openList.Dequeue();
				if (map.ContainsKey(type))
					continue;
				var instance = creator();
				foreach (var dep in instance.Dependencies()) {
					openList.Enqueue(dep);
				}
				map.Add(type, instance);
			}
			return [.. map.Values];
		}

		private static IEnumerable<CompileStep> OrderByDependencies(CompileStep[] steps)
		{
			var seen = new HashSet<Type>();
			while (steps.Length > 0) {
				var freeList = steps.Where(s => !s.Dependencies().Select(d => d.Item1).Except(seen).Any()).ToArray();
				if (freeList.Length == 0) {
					throw new Exception("Pipeline dependency loop");
				}
				foreach (var step in freeList) {
					yield return step;
					seen.Add(step.GetType());
				}
				steps = steps.Except(freeList).ToArray();
			}
		}

		internal Script Process(string name, PanSqlFile f, string basePath)
		{
			foreach (var step in _steps) {
				step.Initialize(_codeBuilder, basePath);
				step.Execute(f);
			}
			var cg = (Codegen)_steps[^1];
			var result = cg.Output;
			result.Name = name;
			return result;
		}
	}
}
