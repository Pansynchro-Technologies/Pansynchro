﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Microsoft.SqlServer.TransactSql.ScriptDom;

using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.Helpers;

using TableReference = Microsoft.SqlServer.TransactSql.ScriptDom.TableReference;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal class DefineVars : VisitorCompileStep
	{
		public override void OnFile(PanSqlFile node)
		{
			_file.Vars.Add("FS_DICT", new Variable("FS_DICT", "Data", new MagicDeclarationStatement("FileSystem")));
			base.OnFile(node);
		}

		public override void OnLoadStatement(LoadStatement node)
		{
			var filename = node.Filename;
			if (filename.Contains('\\') && '\\' != Path.DirectorySeparatorChar) {
				filename = filename.Replace('\\', '/');
			}
			if (filename.Contains('/') && '/' != Path.DirectorySeparatorChar) {
				filename = filename.Replace('/', '\\');
			}
			filename = Path.GetFullPath(filename, Path.GetDirectoryName(_file.Filename) ?? BasePath);
			if (!File.Exists(filename)) {
				throw new CompilerError($"Data dictionary file '{filename}' was not found", node);
			}
			try {
				node.Dict = DataDictionary.LoadFromFile(filename);
				_file.AddVar(new(node.Name, "Data", node), node);
			} catch (Exception e) {
				throw new CompilerError($"Unable to load data dictionary from '{filename}'", e, node);
			}
		}

		public override void OnSaveStatement(SaveStatement node)
		{
			VerifyDictionaryName(node.Name, node);
		}

		public override void OnTypeDefinition(TypeDefinition node)
		{
			if (!_file.Types.TryAdd(node.Definition.Name.Name, node)) {
				throw new CompilerError($"A type named '{node.Definition.Name}' has already been defined.", node);
			}
		}

		public override void OnVarDeclaration(VarDeclaration node)
		{
			var cid = node.Identifier;
			var dict = VerifyDictionaryName(cid.Parent.ToString()!, cid);
			try {
				node.Stream = dict.Declaration switch {
					LoadStatement l => l.Dict.GetStream(cid.Name!),
					AnalyzeStatement a => throw new CompilerError("A data dictionary needs to be loaded with the LOAD command to be used here.", node),
					MagicDeclarationStatement m => GetMagicDict(m.Value).GetStream(cid.Name!),
					_ => throw new NotSupportedException()
				};
			} catch (KeyNotFoundException) {
				throw new CompilerError($"'{cid.Parent}' does not contain a stream named {cid.Name}", node);
			}
			var newvar = new Variable(node.Name, node.Type.ToString(), node);
			_file.AddVar(newvar, node);
		}

		internal static DataDictionary GetMagicDict(string value) => value switch {
			"FileSystem" => DataBuiltin.FileSystem.Dict,
			_ => throw new NotImplementedException()
		};

		public override void OnScriptVarDeclarationStatement(ScriptVarDeclarationStatement node)
		{
			var newvar = new Variable(node.Name.Name, "ScriptVar", node);
			_file.AddVar(newvar, node);
		}

		public override void OnScriptVarExpression(ScriptVarExpression node)
		{
			if (!_file.Vars.TryGetValue(node.Name, out var sVar)) {
				throw new CompilerError($"No variable named '{node.Name}' has been defined", node);
			}
			if (sVar.Type != "ScriptVar") {
				throw new CompilerError($"The variable '{node.Name}' is not a script variable", node);
			}
			sVar.Used = true;
		}

		public override void OnOpenStatement(OpenStatement node)
		{
			switch (node.Type) {
				case OpenType.Read:
				case OpenType.Write:
					if (node.Dictionary == null) {
						throw new CompilerError("Cannot open a read or write connection without an existing dictionary", node);
					}
					var dictName = node.Dictionary.Name;
					VerifyDictionaryName(dictName, node);
					if (node.Source != null) {
						foreach (var src in node.Source) {
							if (!_file.Vars.TryGetValue(src.Name, out var sVar)) {
								throw new CompilerError($"No variable named '{node.Source}' has been declared.", node);
							}
							sVar.Used = true;
						}
					}
					var newvar = new Variable(node.Name, node.Type == OpenType.Read ? "Reader" : "Writer", node);
					_file.AddVar(newvar, node);
					break;
				case OpenType.Analyze:
					if (node.Dictionary != null) {
						throw new CompilerError("Cannot open an analyze connection with an existing dictionary", node);
					}
					newvar = new Variable(node.Name, "Analyzer", node);
					_file.AddVar(newvar, node);
					break;
				case OpenType.Source:
				case OpenType.Sink:
					newvar = new Variable(node.Name, node.Type == OpenType.Source ? "Source" : "Sink", node);
					_file.AddVar(newvar, node);
					break;
				case OpenType.ProcessRead:
				case OpenType.ProcessWrite:
					newvar = new Variable(node.Name, node.Type.ToString(), node);
					_file.AddVar(newvar, node);
					break;
			}
		}

		private Variable VerifyDictionaryName(string dictName, Node node)
		{
			if (!_file.Vars.TryGetValue(dictName, out var dict)) {
				throw new CompilerError($"No variable named '{dictName}' has been defined", node);
			}
			if (dict.Type != "Data") {
				throw new CompilerError($"The variable '{dictName}' is not a data dictionary", node);
			}
			dict.Used = true;
			return dict;
		}

		public override void OnSqlStatement(SqlTransformStatement node)
		{
			var stmt = node.SqlNode;
			node.Metadata = ProcessSelect(stmt, node.Dest, node);
		}

		public override void OnExistsExpression(ExistsExpression node)
		{
			var metadata = ProcessSelect(node.Stmt, null, node);
			if (metadata.TransactionType.HasFlag(TransactionType.ToStream) | metadata.TransactionType.HasFlag(TransactionType.FromStream)) {
				throw new CompilerError("EXISTS expressions must operate purely on in-memory data, not streams.", node);
			}
			node.Metadata = metadata;
		}

		private SqlMetadata ProcessSelect(SelectStatement sel, Ast.Identifier? dest, Node node)
		{
			var grouped = false;
			var oneAgg = false;
			var result = new SqlMetadata();
			result.Tables.AddRange(VerifySelect(sel, node).Distinct());
			if (dest != null) {
				result.Output = VerifyTableName(dest.Name, false, node)[0];
			}
			var spec = (QuerySpecification)sel.QueryExpression;
			grouped = spec.GroupByClause != null;
			if (!grouped) {
				oneAgg = new OneAggVisitor().Check(spec);
			}

			var tt = result.Tables.Any(v => v.Type == "Stream") ? TransactionType.FromStream : TransactionType.PureMemory;
			if (result.Tables.Where(t => t.Type != "Cte").Count() > 1) {
				tt |= TransactionType.Joined;
			}
			if (result.Output?.Type == "Stream") {
				tt |= TransactionType.ToStream;
			}
			if (grouped || oneAgg) {
				tt |= TransactionType.Grouped;
			}
			if (sel.WithCtesAndXmlNamespaces?.CommonTableExpressions?.Count > 0) {
				tt |= TransactionType.WithCte;
			}
			if (dest == null) {
				tt |= TransactionType.Exists;
			}
			result.TransactionType = tt;
			return result;
		}

		private IEnumerable<Variable> VerifySelect(SelectStatement sel, Node node)
		{
			if (sel.WithCtesAndXmlNamespaces?.CommonTableExpressions?.Count > 0) {
				foreach (var cte in sel.WithCtesAndXmlNamespaces.CommonTableExpressions) {
					foreach (var result in VerifyQuerySpec((QuerySpecification)cte.QueryExpression, node)) {
						yield return result;
					}
					AddCte(cte, node);
				}
			}
			var from = ((QuerySpecification)sel.QueryExpression).FromClause;
			var tableRequired = false;
			foreach (var table in from.TableReferences) {
				foreach (var result in VerifyTable(table, tableRequired, node)) {
					yield return result;
				}
				tableRequired = true;
			}
		}

		private void AddCte(CommonTableExpression cte, Node node)
		{
			var name = cte.ExpressionName.Value.ToPropertyName();
			if (_file.Vars.ContainsKey(name)) {
				throw new CompilerError($"CTE name '{name}' must not be used elsewhere in the script.", node);
			}
			var result = new Variable(name, "Cte", node);
			_file.AddVar(result, node);
		}

		private IEnumerable<Variable> VerifyQuerySpec(QuerySpecification spec, Node node) 
			=> VerifyQueryFromClause(spec.FromClause, node);

		private IEnumerable<Variable> VerifyQueryFromClause(FromClause from, Node node)
		{
			foreach (var table in from.TableReferences) {
				foreach (var result in VerifyTable(table, false, node)) {
					yield return result;
				}
			}

		}

		private IEnumerable<Variable> VerifyTable(TableReference table, bool tableRequired, Node node)
		{
			switch (table) {
				case NamedTableReference tRef:
					var name = tRef.SchemaObject.BaseIdentifier.Value;
					foreach (var result in VerifyTableName(name, tableRequired, node)) {
						yield return result;
					}
					break;
				case JoinTableReference join:
					foreach (var result in VerifyTable(join.FirstTableReference, tableRequired, node).Concat(VerifyTable(join.SecondTableReference, true, node))) {
						yield return result;
					}
					break;
				default: throw new NotImplementedException();
			}
		}

		private Variable[] VerifyTableName(string name, bool tableRequired, Node node)
		{
			if (!_file.Vars.TryGetValue(name, out var tVar)) {
				throw new CompilerError($"Table names in a SQL FROM or JOIN clause must be declared. '{name}' has not been defined.", node);
			}
			if (!(tVar.Type is "Table" or "Stream" or "Cte")) {
				throw new CompilerError($"Table names in a SQL FROM or JOIN clause must be declared. '{name}' is not a table or stream variable, or a CTE.", node);
			}
			if (tableRequired && tVar.Type == "Stream") {
				throw new CompilerError($"The target of a SQL JOIN clause must be declared as a table. '{name}' is a stream variable.", node);
			}
			if (tVar.Used && tVar.Type == "Stream") {
				throw new CompilerError($"The stream '{name}' has already been processed in an earlier command.  If it needs to be used multiple times, it should be declared as 'table'.", node);
			}
			if (tVar.Type == "Cte") {
				if (tVar.Declaration != node) { 
					throw new CompilerError($"The CTE '{name}' can only be used in the SQL statement that declared it.", node);
				}
			}
			tVar.Used = true;
			return [tVar];
		}

		public override void OnAnalyzeStatement(AnalyzeStatement node)
		{
			var connection = node.Conn.Name;
			if (!_file.Vars.TryGetValue(connection, out var conn)) {
				throw new CompilerError($"No variable named '{connection}' has been defined.", node);
			}
			if (conn.Type != "Analyzer") {
				throw new CompilerError($"The variable '{conn}' is not an analyzer.", node);
			}
			conn.Used = true;
			var opts = node.Options;
			if (opts != null) {
				var groups = opts.ToLookup(o => o.Type).ToDictionary(g => g.Key, g => g.Count());
				var surplus = groups.Where(p => p.Value > 1).FirstOrDefault();
				if (surplus.Value != 0) {
					throw new CompilerError($"The analyzer option '{surplus.Key.ToString().ToLowerInvariant()}' is specified multiple times.", node);
				}
				if (groups.ContainsKey(AnalyzeOptionType.Include) && groups.ContainsKey(AnalyzeOptionType.Exclude)) {
					throw new CompilerError("The options 'include' and 'exclude' cannot be specified on the same analyzer.", node);
				}
			}
			if (_file.Vars.ContainsKey(node.Dict.Name)) {
				throw new CompilerError($"A variable named '{node.Dict}' has already been declared.", node);
			}
			_file.AddVar(new(node.Dict.Name, "Data", node), node);
		}

		public override void OnMapStatement(MapStatement node)
		{
			if (!node.IsNS) {
				var s = CheckStreamVar(node.Source);
				var d = CheckStreamVar(node.Dest);
				node.Streams = (s, d);
			}
		}

		private StreamDefinition CheckStreamVar(CompoundIdentifier id)
		{
			var dict = VerifyDictionaryName(id.Parent.ToString()!, id);
			try {
				return ((LoadStatement)dict.Declaration).Dict.GetStream(id.Name!);
			} catch (KeyNotFoundException) {
				throw new CompilerError($"No stream named {id.Name} is defined in {id.Parent}", id);
			}
		}

		public override void OnSyncStatement(SyncStatement node)
		{
			VerifyConnName(node.Input.Name, "Reader", node);
			VerifyConnName(node.Output.Name, "Writer", node);
		}

		private void VerifyConnName(string name, string type, Node node)
		{
			if (!_file.Vars.TryGetValue(name, out var conn)) {
				throw new CompilerError($"Connectors must be opened before use. '{name}' has not been defined.", node);
			}
			if (conn.Type != type) {
				throw new CompilerError($"Invalid connector type. '{name}' is not a {type.ToLower()}.", node);
			}
			conn.Used = true;
		}

		private class OneAggVisitor: TSqlFragmentVisitor
		{
			private bool _found = false;

			public override void Visit(FunctionCall fragment)
			{
				var name = fragment.FunctionName.Value.ToLower().ToPropertyName();
				if (SqlAnalysis.AGGREGATE_FUNCTIONS_SUPPORTED.ContainsKey(name)) {
					_found = true;
				}
				base.Visit(fragment);
			}

			internal bool Check(QuerySpecification queryExpression)
			{
				foreach (var expr in queryExpression.SelectElements)
				{
					expr.Accept(this);
					if (_found) {
						return true;
					}
				}
				return false;
			}
		}
	}
}
