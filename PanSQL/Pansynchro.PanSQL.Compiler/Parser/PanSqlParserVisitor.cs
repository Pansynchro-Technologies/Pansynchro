using System;
using System.Linq;
using System.Collections.Generic;

using Antlr4.Runtime.Misc;
using Antlr4.Runtime.Tree;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.Parser
{
	using Microsoft.SqlServer.Management.SqlParser.Parser;

	internal class PanSqlParserVisitor : PanSqlParserBaseVisitor<Node>, IPanSqlParserVisitor<Node>
	{
		public override Node VisitId([NotNull] PanSqlParser.IdContext context)
		{
			throw new NotImplementedException();
		}

		public override Node VisitCompoundId([NotNull] PanSqlParser.CompoundIdContext context)
		{
			var parent = context.IDENTIFIER(0).GetText();
			var name = context.IDENTIFIER(1).GetText();
			return new CompoundIdentifier(parent, name);
		}

		Node IPanSqlParserVisitor<Node>.VisitCompoundId(PanSqlParser.CompoundIdContext context)
		{
			return VisitCompoundId(context);
		}

		Node IPanSqlParserVisitor<Node>.VisitFunctionCall(PanSqlParser.FunctionCallContext context)
		{
			var type = context.id().GetText();
			var values = context.argList().expression()?.Select(VisitExpression).WhereNotNull().ToArray()
				?? Array.Empty<Expression>();
			return new FunctionCallExpression(type, values);
		}

		public override Expression? VisitExpression(PanSqlParser.ExpressionContext context)
		{
			if (context == null) {
				return null;
			}
			return (Expression)Visit(context.children[0]);
		}

		public override CredentialExpression VisitCredentials([NotNull] PanSqlParser.CredentialsContext context)
		{
			var expr = VisitExpression(context.expression());
			return expr switch {
				null => throw new Exception("Value should not be null"),
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

		private new Statement VisitLine(PanSqlParser.LineContext ctx) => (Statement)Visit(ctx.statement());

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
			var source = (CompoundIdentifier)VisitCompoundId(context.compoundId(0));
			var dest = (CompoundIdentifier)VisitCompoundId(context.compoundId(1));
			var mappings = VisitMappingList(context.mappingList());
			return new MapStatement(source, dest, mappings);
		}

		Node IPanSqlParserVisitor<Node>.VisitOpenStatement(PanSqlParser.OpenStatementContext context)
		{
			var ids = context.id();
			var name = ids[0].GetText();
			var connector = ids[1].GetText();
			var ot = Enum.Parse<OpenType>(context.openType().GetText(), true);
			var dict = ids.Length == 3 ? new Identifier(ids[2].GetText()) : null;
			var creds = (CredentialExpression)VisitCredentials(context.credentials());
			var source = context.dataSourceSink();
			var sourceId = source == null ? null : new Identifier(source.id().GetText());
			return new OpenStatement(name, connector, ot, dict, creds, sourceId);
		}

		Node IPanSqlParserVisitor<Node>.VisitOpenType(PanSqlParser.OpenTypeContext context)
		{
			throw new NotImplementedException();
		}

		Node IPanSqlParserVisitor<Node>.VisitSqlStatement(PanSqlParser.SqlStatementContext context)
		{
			var start = context.WITH() != null ? "with" : "select";
			var sql = start + ' ' + string.Join(' ', context.sqlToken().Select(t => t.GetText())).Replace(" . ", ".").Replace(" , ", ", ").Replace(" @ ", " @");
			var id = context.id().GetText();
			var sqlNode = Parser.Parse(sql);
			var batches = sqlNode.Script.Batches;
			if (batches.Count > 1 || batches[0].Statements.Count > 1) {
				throw new Exception("SQL scripts should only contain one statement at a time");
			}
			var stmt = batches[0].Statements[0];
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
			return new Identifier(context.id().GetText());
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
			var type = VisitScriptVarType(context.scriptVarType());
			var expr = VisitExpression(context.expression());
			return new ScriptVarDeclarationStatement(name, type, expr);
		}

		public override ScriptVarExpression VisitScriptVarRef(PanSqlParser.ScriptVarRefContext context)
			=> new(context.IDENTIFIER().GetText());

		public override TypeReferenceExpression VisitScriptVarType(PanSqlParser.ScriptVarTypeContext context)
		{
			var name = context.id().GetText();
			var magnitudeStr = context.scriptVarSize()?.GetText().ToLower();
			int? magnitude = magnitudeStr switch {
				null => null,
				"max" => null,
				_ => int.Parse(magnitudeStr),
			};
			var isArr = context.ARRAY() != null;
			return new TypeReferenceExpression(name, magnitude, isArr);
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
	}
}
