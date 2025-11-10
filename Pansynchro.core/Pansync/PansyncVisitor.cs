using System;
using System.Collections.Generic;

using Antlr4.Runtime.Tree;

namespace Pansynchro.Core.Pansync
{
	internal class PansyncVisitor : IPansyncParserVisitor<PansyncNode>
	{
		public PansyncNode Visit(IParseTree tree)
		{
			throw new NotImplementedException();
		}

		PansyncNode IPansyncParserVisitor<PansyncNode>.VisitBlock(PansyncParser.BlockContext context)
		{
			throw new NotImplementedException();
		}

		public Statement[]? VisitBlock(PansyncParser.BlockContext context)
		{
			if (context == null) {
				return null;
			}
			var statements = context.statement();
			var result = new Statement[statements.Length];
			for (int i = 0; i < statements.Length; ++i) {
				result[i] = VisitStatement(statements[i]);
			}
			return result;
		}

		public PansyncNode VisitChildren(IRuleNode node)
		{
			throw new NotImplementedException();
		}

		public PansyncNode VisitErrorNode(IErrorNode node)
		{
			throw new NotImplementedException();
		}

		public PansyncNode VisitExpression(PansyncParser.ExpressionContext context)
		{
			var name = context.name();
			if (name != null) {
				return new NameNode(name.GetText());
			}
			var str = context.@string();
			if (str != null) {
				return VisitString(str);
			}
			var num = context.INTEGER();
			if (num != null) {
				var numValue = num.GetText().TrimEnd('L');
				return new IntegerNode(long.Parse(numValue));
			}
			var nl = context.named_list();
			if (nl != null) {
				return VisitNamedList(nl);
			}
			var kvl = context.kv_list();
			if (kvl != null) {
				return VisitKvList(kvl);
			}
			throw new NotImplementedException("Unknown expression type.");
		}

		public Expression[] VisitExpressionList(PansyncParser.Expression_listContext context)
		{
			var exprs = context.expression();
			var result = new Expression[exprs.Length];
			for (int i = 0; i < exprs.Length; ++i) {
				result[i] = (Expression)VisitExpression(exprs[i]);
			}
			return result;
		}

		public PansyncNode VisitExpression_list(PansyncParser.Expression_listContext context)
		{
			throw new NotImplementedException();
		}

		public PansyncNode VisitFile(PansyncParser.FileContext context)
		{
			var body = context.statement();
			var result = new Statement[body.Length];
			for (int i = 0; i < body.Length; ++i) {
				result[i] = VisitStatement(body[i]);
			}
			return new PansyncFile(result);
		}

		PansyncNode IPansyncParserVisitor<PansyncNode>.VisitStatement(PansyncParser.StatementContext context)
		{
			throw new NotImplementedException();
		}

		public Statement VisitStatement(PansyncParser.StatementContext context)
		{
			var cmd = context.command();
			if (cmd != null) {
				return (Statement)VisitCommand(cmd);
			}
			var dl = context.data_list();
			if (dl != null) {
				return VisitDataList(dl);
			}
			throw new NotImplementedException("Unknown statement type.");
		}


		public PansyncNode VisitCommand(PansyncParser.CommandContext context)
		{
			var id = context.IDENTIFIER().GetText();
			var args = context.expression_list();
			var result = context.command_result();
			var body = context.block();
			return new Command(
				id,
				VisitExpressionList(args),
				VisitCommandResult(result),
				VisitBlock(body));
		}

		public Expression[]? VisitCommandResult(PansyncParser.Command_resultContext context)
		{
			if (context == null)
				return null;
			var result = context.expression_list();
			return result == null ? null : VisitExpressionList(result);
		}

		public PansyncNode VisitCommand_result(PansyncParser.Command_resultContext context)
		{
			throw new NotImplementedException();
		}

		public DataListNode VisitDataList(PansyncParser.Data_listContext context)
		{
			var exprs = context.expression();
			var values = new Expression[exprs.Length];
			for (int i = 0; i < values.Length; ++i) {
				values[i] = (Expression)VisitExpression(exprs[i]);
			}
			return new DataListNode(values);
		}

		public PansyncNode VisitData_list(PansyncParser.Data_listContext context)
		{
			throw new NotImplementedException();
		}

		public PansyncNode VisitString(PansyncParser.StringContext context)
		{
			var result = context.GetText();
			return new StringNode(result[1..^1]);
		}

		public PansyncNode VisitTerminal(ITerminalNode node)
		{
			throw new NotImplementedException();
		}

		public PansyncNode VisitName(PansyncParser.NameContext context)
		{
			throw new NotImplementedException();
		}

		public NamedListNode VisitNamedList(PansyncParser.Named_listContext context)
		{
			var name = new NameNode(VisitTitle(context.title()).ToString());
			var args = VisitExpressionList(context.expression_list());
			return new NamedListNode(name, args);
		}

		public PansyncNode VisitNamed_list(PansyncParser.Named_listContext context)
		{
			throw new NotImplementedException();
		}

		public KvListNode VisitKvList(PansyncParser.Kv_listContext context)
		{
			var values = context.kv_pair();
			var result = new KeyValuePair<string, Expression>[values.Length];
			for (int i = 0; i < result.Length; ++i) {
				result[i] = VisitKvPair(values[i]);
			}
			return new KvListNode(result);
		}

		public PansyncNode VisitKv_list(PansyncParser.Kv_listContext context)
		{
			throw new NotImplementedException();
		}

		public KeyValuePair<string, Expression> VisitKvPair(PansyncParser.Kv_pairContext context)
		{
			var key = context.@string().GetText()[1..^1];
			var value = (Expression)VisitExpression(context.expression());
			return KeyValuePair.Create(key, value);
		}

		public PansyncNode VisitKv_pair(PansyncParser.Kv_pairContext context)
		{
			throw new NotImplementedException();
		}

		public PansyncNode VisitTitle(PansyncParser.TitleContext context)
		{
			if (context.name() != null) {
				return new NameNode(context.name().IDENTIFIER().GetText());
			}
			var str = (StringNode)VisitString(context.@string());
			return new NameNode(str.Value);
		}
	}
}
