using System;
using System.Collections.Generic;
using System.Linq;
using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal class BuildMappings : VisitorCompileStep
	{
		private readonly Dictionary<string, string> _mappings = new();

		public override void OnFile(PanSqlFile node)
		{
			_file = node;
			foreach (var map in node.Lines.OfType<MapStatement>()) {
				try {
					OnMapStatement(map);
				} catch (ArgumentException) {
					throw new CompilerError($"Cannot map '{map.Source}' with more than one map or SQL statement", map);
				}
			}
			foreach (var sql in node.Lines.OfType<SqlTransformStatement>()) {
				try {
					OnSqlStatement(sql);
				} catch (ArgumentException) {
					throw new CompilerError($"Cannot map '{sql.Tables[0]}' with more than one map or SQL statement", sql);
				}
			}
			_file.Mappings = _mappings;
		}

		public override void OnSqlStatement(SqlTransformStatement node)
		{
			var srcName = VariableHelper.GetStream(node.Tables[0]).Name.ToString();
			var dstName = VariableHelper.GetStream(node.Output).Name.ToString();
			AddMapping(srcName, dstName, node);
		}

		private void AddMapping(string srcName, string dstName, Statement node)
		{
			if (_mappings.TryGetValue(srcName, out var value)) {
				throw new CompilerError($"The stream '{srcName}' has already been mapped to '{value}' in either a SQL query or a map statement.  It cannot be mapped again.", node);
			}
			_mappings.Add(srcName, dstName);
		}

		public override void OnMapStatement(MapStatement node)
		{
			if (node.Mappings.Length > 0) {
				CheckFieldMappings(node);
			}
			if (node.Source.Name != node.Dest.Name) {
				AddMapping(StreamName(node.Source), StreamName(node.Dest), node);
			}
		}

		private void CheckFieldMappings(MapStatement node)
		{
			var mappings = node.Mappings.ToDictionary(m => m.Key.Name, m => m.Value.Name);
			var sourceFields = OrderedFields(node.Streams.s, mappings);
			var destFields = OrderedFields(node.Streams.d, null);
			if (sourceFields.SequenceEqual(destFields)) {
				return;
			}
			if (sourceFields.Select(p => p.Key).SequenceEqual(destFields.Select(p => p.Key))) {
				RearrangeFields(node, sourceFields, destFields);
			} else {
				throw new CompilerError($"Unable to map the fields of '{node.Streams.s.Name}' to '{node.Streams.d.Name}'", node);
			}
		}

		private void RearrangeFields(MapStatement node, KeyValuePair<string, int>[] sourceFields, KeyValuePair<string, int>[] destFields)
		{
			throw new NotImplementedException();
		}

		private static KeyValuePair<string, int>[] OrderedFields(StreamDefinition stream, Dictionary<string,string>? mappings) 
			=> stream.Fields
				.Select((f, i) => KeyValuePair.Create(mappings?.TryGetValue(f.Name, out var mappedName) == true ? mappedName : f.Name, i))
				.OrderBy(p => p.Key)
				.ToArray();

		private static string StreamName(CompoundIdentifier name) => string.Join('.', name.ToString().Split('.')[1..]);


		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies() => [Dependency<SqlAnalysis>()];
	}
}
