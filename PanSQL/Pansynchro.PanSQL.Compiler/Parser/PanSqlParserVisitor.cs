using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using Antlr4.Runtime.Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Pansynchro.Core.Helpers;
using Pansynchro.PanSQL.Compiler.Ast;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using Identifier = Pansynchro.PanSQL.Compiler.Ast.Identifier;

namespace Pansynchro.PanSQL.Compiler.Parser
{
	internal class PanSqlParserVisitor : PanSqlParserBaseVisitor<Node>, IPanSqlParserVisitor<Node>
	{
		private readonly CommonTokenStream _cts;

		public PanSqlParserVisitor(CommonTokenStream cts)
		{
			_cts = cts;
		}

		public override Identifier VisitId([NotNull] PanSqlParser.IdContext context) => new Identifier(context.GetText());

		public override Node VisitCompoundId([NotNull] PanSqlParser.CompoundIdContext context)
		{
			var parent = context.IDENTIFIER(0).GetText();
			var name = string.Join('.', context.IDENTIFIER().Skip(1).Select(i => i.GetText()));
			return new CompoundIdentifier(new Identifier(parent), name);
		}

		Node IPanSqlParserVisitor<Node>.VisitCompoundId(PanSqlParser.CompoundIdContext context)
		{
			return VisitCompoundId(context);
		}

		Node IPanSqlParserVisitor<Node>.VisitFunctionCall(PanSqlParser.FunctionCallContext context)
		{
			int startIdx = context.Start.TokenIndex;
			int stopIdx = context.Stop.TokenIndex;
			var list = _cts.GetTokens(startIdx, stopIdx);
			var text = string.Concat(list.Select(t => t.Text));
			using var reader = new StringReader(text);
			var parsed = new TSql170Parser(false, SqlEngineType.Standalone).ParseExpression(reader, out var errors);
			if (errors?.Count > 0) {
				throw new Exception(errors[0].Message);
			}
			return new TSqlExpression(parsed);
		}

		public override Expression? VisitExpression(PanSqlParser.ExpressionContext context)
		{
			if (context == null) {
				return null;
			}
			return (Expression)Visit(context.children[0]) ?? throw new NotImplementedException();
		}

		public override CredentialExpression VisitCredentials([NotNull] PanSqlParser.CredentialsContext context)
		{
			var expr = VisitExpression(context.expression());
			return expr switch {
				null => throw new Exception("Value should not be null"),
				TSqlExpression { Expr: FunctionCall { FunctionName.Value: string name, Parameters: { } args} } ts => args switch {
					[StringLiteral sl] => new CredentialExpression(name, new StringLiteralExpression(sl.Value)),
					_ => new CredentialExpression("", ts)
				},
				FunctionCallExpression { Method: string name, Args : [StringLiteralExpression sl] } => new CredentialExpression(name, sl),
				TypedExpression te => new CredentialExpression("__direct", te),
				_ => throw new Exception($"'{expr}' is not a valid credentials value")
			};
		}

		private static string VisitString(ITerminalNode terminalNode)
		{
			var text = terminalNode.GetText();
			if (!(text.Length >= 2 && text[0] == '\'' && text[^1] == '\'')) {
				throw new ArgumentException("Malformed string node");
			}
			var result = text[1..^1].Replace("''", "'");
			return result;
		}

		Node IPanSqlParserVisitor<Node>.VisitCredentials(PanSqlParser.CredentialsContext context)
		{
			throw new NotImplementedException();
		}

		Node IPanSqlParserVisitor<Node>.VisitFile(PanSqlParser.FileContext context)
		{
			var lines = context.line().Select(VisitLine).ToArray();
			return new PanSqlFile(lines);
		}

		Node IPanSqlParserVisitor<Node>.VisitId(PanSqlParser.IdContext context) => new Identifier(context.GetText());

		private new Statement VisitLine(PanSqlParser.LineContext ctx) => (Statement)Visit(ctx.statement()) ?? throw new NotImplementedException();

		Node IPanSqlParserVisitor<Node>.VisitAlterStatement(PanSqlParser.AlterStatementContext context)
		{
			var table = VisitIdElement(context.idElement());
			var element = VisitId(context.id());
			var value = VisitExpression(context.expression())!;
			return new AlterStatement(table, element, value);
		}

		Node IPanSqlParserVisitor<Node>.VisitCallStatement(PanSqlParser.CallStatementContext context)
		{
			var call = (FunctionCallExpression)VisitFunctionCall(context.functionCall());
			return new CallStatement(call);
		}

		Node IPanSqlParserVisitor<Node>.VisitReadStatement(PanSqlParser.ReadStatementContext context)
		{
			var table = VisitId(context.id());
			return new ReadStatement(table);
		}

		Node IPanSqlParserVisitor<Node>.VisitLoadStatement(PanSqlParser.LoadStatementContext context)
		{
			var name = context.id().GetText();
			var filename = VisitString(context.STRING());
			return new LoadStatement(name, filename);
		}

		Node IPanSqlParserVisitor<Node>.VisitMapping(PanSqlParser.MappingContext context)
		{
			throw new NotImplementedException();
		}

		private new MappingExpression VisitMapping(PanSqlParser.MappingContext context)
		{
			var k = new Identifier(context.id(0).GetText());
			var v = new Identifier(context.id(1).GetText());
			return new MappingExpression(k, v);
		}

		Node IPanSqlParserVisitor<Node>.VisitMappingList(PanSqlParser.MappingListContext context)
		{
			throw new NotImplementedException();
		}

		private new MappingExpression[] VisitMappingList(PanSqlParser.MappingListContext ctx)
		{
			return ctx == null ? [] : ctx.mapping().Select(VisitMapping).ToArray();
		}

		Node IPanSqlParserVisitor<Node>.VisitMapStatement(PanSqlParser.MapStatementContext context)
		{
			if (context.NAMESPACE() != null) {
				var ids = context.nullableId();
				var l = ids[0].NULL() != null ? null : ids[0].id().GetText();
				var r = ids[1].NULL() != null ? null : ids[1].id().GetText();
				return new MapStatement(new CompoundIdentifier(null, l), new CompoundIdentifier(null, r), [], true);
			}

			var source = (CompoundIdentifier)VisitCompoundId(context.compoundId(0));
			var dest = (CompoundIdentifier)VisitCompoundId(context.compoundId(1));
			var mappings = VisitMappingList(context.mappingList());
			return new MapStatement(source, dest, mappings, false);
		}

		Node IPanSqlParserVisitor<Node>.VisitOpenStatement(PanSqlParser.OpenStatementContext context)
		{
			var ids = context.id();
			var name = ids[0].GetText();
			var connector = ids[1].GetText();
			var ot = Enum.Parse<OpenType>(context.openType().GetText(), true);
			var dict = ids.Length == 3 ? new Identifier(ids[2].GetText()) : null;
			var creds = (CredentialExpression)VisitCredentials(context.credentials());
			var sources = context.dataSourceSink();
			var sourceIds = sources?.Select(source => new Identifier(source.id().GetText())).ToArray();
			return new OpenStatement(name, connector, ot, dict, creds, sourceIds?.Length > 0 ? sourceIds : null);
		}

		Node IPanSqlParserVisitor<Node>.VisitOpenType(PanSqlParser.OpenTypeContext context)
		{
			throw new NotImplementedException();
		}

		Node IPanSqlParserVisitor<Node>.VisitSqlStatement(PanSqlParser.SqlStatementContext context)
		{
			var id = context.id().GetText();
			int startIdx = context.Start.TokenIndex;
			int stopIdx = context.INTO().Symbol.TokenIndex - 1;
			var list = _cts.GetTokens(startIdx, stopIdx);
			var text = string.Concat(list.Select(t => t.Text));
			using var reader = new StringReader(text);
			var parser = new TSql170Parser(false, SqlEngineType.Standalone);
			var altSqlNode = parser.Parse(reader, out IList<ParseError> errors) as TSqlScript;
			var batches = altSqlNode?.Batches;
			if (errors.Count > 0) {
				throw new Exception("Error(s) found parsing SQL script:" + Environment.NewLine +
									string.Join(Environment.NewLine, errors.Select(e => e.Message)));
			}
			if (batches == null || batches.Count > 1 || batches[0].Statements.Count > 1) {
				throw new Exception("SQL scripts should only contain one statement at a time");
			}
			var stmt = (SelectStatement)batches[0].Statements[0];
			return new SqlTransformStatement(stmt, new Identifier(id));
		}

		Node IPanSqlParserVisitor<Node>.VisitVarDeclaration(PanSqlParser.VarDeclarationContext context)
		{
			var type = Enum.Parse<VarDeclarationType>(context.varType().GetText(), true);
			var name = context.id().GetText();
			var source = (CompoundIdentifier)VisitCompoundId(context.compoundId());
			return new VarDeclaration(type, name, source);
		}

		public override Expression VisitIdElement(PanSqlParser.IdElementContext context)
		{
			if (context.compoundId() != null) {
				return (Expression)VisitCompoundId(context.compoundId());
			}
			var text = context.id().GetText();
			if (text == "CURRENT_TIMESTAMP") { // the only SQL Server niladic function that PanSQL will support
				return new FunctionCallExpression("CurrentTimestamp", []);
			}
			return new Identifier(text);
		}

		Node IPanSqlParserVisitor<Node>.VisitVarType(PanSqlParser.VarTypeContext context)
		{
			throw new NotImplementedException();
		}

		Node IPanSqlParserVisitor<Node>.VisitSyncStatement(PanSqlParser.SyncStatementContext context)
		{
			var input = new Identifier(context.id(0).GetText());
			var output = new Identifier(context.id(1).GetText());
			return new SyncStatement(input, output);
		}

		public override Node VisitAnalyzeStatement(PanSqlParser.AnalyzeStatementContext context)
		{
			var conn = new Identifier(context.id(0).GetText());
			var dict = new Identifier(context.id(1).GetText());
			var options = (context.analyzeOption() != null) ? context.analyzeOption().Select(a => (AnalyzeOption)VisitAnalyzeOption(a)).ToArray() : null ;
			return new AnalyzeStatement(conn, dict, options);
		}

		public override Node VisitAnalyzeOption(PanSqlParser.AnalyzeOptionContext context)
			=> context.analyzeList() == null
				? new AnalyzeOption(AnalyzeOptionType.Optimize)
				: (Node)(AnalyzeOption)VisitAnalyzeList(context.analyzeList());

		public override Node VisitAnalyzeList(PanSqlParser.AnalyzeListContext context)
		{
			var type = Enum.Parse<AnalyzeOptionType>(context.GetChild(0).GetText(), true);
			var items = context.idList().idElement().Select(Visit).Cast<Expression>().ToArray();
			return new AnalyzeOption(type, items);
		}

		public override Node VisitSaveStatement(PanSqlParser.SaveStatementContext context)
		{
			var name = context.id().GetText();
			var filename = VisitString(context.STRING());
			return new SaveStatement(name, filename);
		}

		public override Node VisitScriptVarDeclaration(PanSqlParser.ScriptVarDeclarationContext context)
		{
			var name = VisitScriptVarRef(context.scriptVarRef());
			if (name is not ScriptVarExpression sv) {
				throw new Exception("DECLARE statements cannot declare a variable with a compound name");
			}
			var type = VisitScriptVarType(context.scriptVarType());
			var expr = VisitExpression(context.expression());
			return new ScriptVarDeclarationStatement(sv, type, expr);
		}

		public override Statement VisitIfStatement([NotNull] PanSqlParser.IfStatementContext context)
		{
			var cond = VisitExpression(context.expression()) ?? throw new NotImplementedException();
			var body = context.statement().Select(s => VisitStatement(s) as Statement ?? throw new NotImplementedException()).ToArray();
			return new SqlIfStatement(cond, body);
		}

		public override Statement VisitAbortStatement([NotNull] PanSqlParser.AbortStatementContext context)
		{
			return new AbortStatement();
		}

		public override Expression VisitScriptVarRef(PanSqlParser.ScriptVarRefContext context)
		{
			var ids = context.IDENTIFIER();
			Expression result = new ScriptVarExpression(ids[0].GetText());
			for (int i = 1; i < ids.Length; ++i) {
				var id = ids[i];
				result = new CompoundIdentifier(result, id.GetText());
			}
			return result;
		}

		public override TypeReferenceExpression VisitScriptVarType(PanSqlParser.ScriptVarTypeContext context)
		{
			var name = context.id().GetText();
			var isArr = context.ARRAY() != null;
			if (context.scriptVarSize()?.id() != null) {
				var id = context.scriptVarSize()?.id().GetText()!;
				if (name.Equals("Record", StringComparison.OrdinalIgnoreCase)) {
					return new RecordTypeReferenceExpression(id, isArr);
				}
				throw new Exception("Named types must be used with the Record() type constructor.");
			} else {
				var magnitudeStr = context.scriptVarSize()?.GetText().ToLower();
				int? magnitude = magnitudeStr switch {
					null => null,
					"max" => null,
					_ => int.Parse(magnitudeStr),
				};
				return new TypeReferenceExpression(name, magnitude, isArr);
			}
		}

		public override Expression VisitExistsExpression([NotNull] PanSqlParser.ExistsExpressionContext context)
		{
			var stmt = Visit(context.sqlStatement()) as SqlTransformStatement ?? throw new NotImplementedException();
			if (stmt.Dest.Name != "_") {
				throw new Exception("EXISTS expressions must end with 'INTO _'.");
			}
			return new ExistsExpression((SelectStatement)stmt.SqlNode);
		}

		public override LiteralExpression VisitLiteral(PanSqlParser.LiteralContext context)
		{
			var text = context.GetText();
			if (text.StartsWith('\'')) {
				return new StringLiteralExpression(VisitString((ITerminalNode)context.children[0]));
			}
			if (int.TryParse(text, out var value)) {
				return new IntegerLiteralExpression(value);
			}
			throw new NotImplementedException();
		}

		public override Expression VisitJsonExpression(PanSqlParser.JsonExpressionContext context)
			=> context.jsonObject() != null ? VisitJsonObject(context.jsonObject()) : VisitJsonArray(context.jsonArray());

		public override Expression VisitJsonArray([NotNull] PanSqlParser.JsonArrayContext context)
		{
			try {
				var arr = JsonArray.Parse(context.GetText())!;
				return new JsonLiteralExpression(arr);
			} catch (JsonException) {
				return VisitInterpolatedJsonArray(context);
			}
		}

		private JsonInterpolatedExpression VisitInterpolatedJsonArray([NotNull] PanSqlParser.JsonArrayContext context)
		{
			var arr = new JsonArray();
			var ints = new List<KeyValuePair<JsonIndexing, Expression>>();
			foreach (var item in context.jsonValue()) {
				switch (item.GetChild(0)) {
					case PanSqlParser.JsonObjectContext o:
						var obj = VisitJsonObject(o);
						if (obj is JsonLiteralExpression jl) {
							arr.Add(jl.Value);
						} else {
							JsonMerge(arr, ints, (JsonInterpolatedExpression)obj);
						}
						break;
					case PanSqlParser.JsonArrayContext a:
						var sub = VisitJsonArray(a);
						if (sub is JsonLiteralExpression jl2) {
							arr.Add(jl2.Value);
						} else {
							JsonMerge(arr, ints, (JsonInterpolatedExpression)sub);
						}
						break;
					case PanSqlParser.ScriptVarRefContext v:
						ints.Add(KeyValuePair.Create(new JsonIndexing(arr.Count + ints.Count), (Expression)Visit(v)));
						break;
					default:
						arr.Add(JsonNode.Parse(item.GetText()));
						break;

				}
			}
			return new JsonInterpolatedExpression(arr, ints);
		}

		public override Expression VisitJsonObject([NotNull] PanSqlParser.JsonObjectContext context)
		{
			try {
				var obj = JsonObject.Parse(context.GetText())!;
				return new JsonLiteralExpression(obj);

			} catch (JsonException) {
				return VisitInterpolatedJsonObject(context);
			}
		}

		private JsonInterpolatedExpression VisitInterpolatedJsonObject([NotNull] PanSqlParser.JsonObjectContext context)
		{
			var obj = new JsonObject();
			var ints = new List<KeyValuePair<JsonIndexing, Expression>>();
			foreach (var pair in context.jsonPair()) {
				var name = JsonNode.Parse(pair.JSONSTRING().GetText())!.AsValue().ToString();
				switch (pair.jsonValue().GetChild(0)) {
					case PanSqlParser.JsonObjectContext o:
						var sub = VisitJsonObject(o);
						if (sub is JsonLiteralExpression jl) {
							obj.Add(name, jl.Value);
						} else {
							JsonMerge(obj, ints, name, (JsonInterpolatedExpression)sub);
						}
						break;
					case PanSqlParser.JsonArrayContext a:
						var arr = VisitJsonArray(a);
						if (arr is JsonLiteralExpression jl2) {
							obj.Add(name, jl2.Value);
						} else {
							JsonMerge(obj, ints, name, (JsonInterpolatedExpression)arr);
						}
						break;
					case PanSqlParser.ScriptVarRefContext v:
						ints.Add(KeyValuePair.Create(new JsonIndexing(name), (Expression)Visit(v)));
						break;
					case PanSqlParser.FunctionCallContext f:
						ints.Add(KeyValuePair.Create(new JsonIndexing(name), (Expression)Visit(f)));
						break;
					default:
						obj.Add(name, JsonNode.Parse(pair.jsonValue().GetText()));
						break;
				}
			}
			return new JsonInterpolatedExpression(obj, ints);
		}

		private static void JsonMerge(JsonObject obj, List<KeyValuePair<JsonIndexing, Expression>> ints, string key, JsonInterpolatedExpression value)
		{
			foreach(var interp in value.Ints) {
				ints.Add(KeyValuePair.Create(interp.Key.Reparent(key), interp.Value));
			}
			obj.Add(key, value.Value);
		}

		private static void JsonMerge(JsonArray arr, List<KeyValuePair<JsonIndexing, Expression>> ints, JsonInterpolatedExpression value)
		{
			var idx = arr.Count + ints.Count;
			foreach(var interp in value.Ints) {
				ints.Add(KeyValuePair.Create(interp.Key.Reparent(idx), interp.Value));
			}
			arr.Add(value.Value);
		}
	}
}
