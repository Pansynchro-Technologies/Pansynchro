using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	record DataModel(
		TableReference[] Inputs,
		JoinSpec[] Joins,
		DbExpression? Filter,
		MemberReferenceExpression[]? GroupKey,
		DbExpression[] Outputs,
		AggregateExpression[] AggOutputs,
		BooleanExpression? AggFilter,
		OrderingExpression[]? Ordering);

	enum TableType
	{
		Stream,
		Table
	}

	record TableReference(string Name, TableType Type, StreamDefinition Stream)
	{
		public TableReference(Ast.Variable t) :
			this(
				t.Name,
				t.Type switch { "Table" => TableType.Table, "Stream" => TableType.Stream, _ => throw new ArgumentException($"Invalid table type: {t.Type}") },
				((VarDeclaration)t.Declaration).Stream
			)
		{ }
	}

	enum JoinType
	{
		Inner,
		Left
	}

	record JoinSpec(JoinType Type, TableReference Target, MemberReferenceExpression[] TargetFields, BooleanExpression Condition);

	abstract class DbExpression
	{
		internal FieldType? Type { get; set; }

		internal abstract bool Match(DbExpression other);

		virtual public bool IsLiteral => false;
	}

	class ReferenceExpression(string name) : DbExpression
	{
		public string Name { get; } = name;

		internal override bool Match(DbExpression other) 
			=> other is ReferenceExpression r && r.GetType() == typeof(ReferenceExpression) && r.Name == Name;

		public override string ToString() => Name;
	}

	class MemberReferenceExpression(ReferenceExpression parent, string name) : ReferenceExpression(name)
	{
		public ReferenceExpression Parent { get; } = parent;

		public override string ToString() => $"{Parent}.{Name}";

		internal override bool Match(DbExpression other)
			=> other is MemberReferenceExpression mr && mr.Name == Name && mr.Parent.Match(Parent);
	}

	class AliasedExpression(DbExpression expr, string alias) : DbExpression
	{
		public DbExpression Expr { get; } = expr;
		public string Alias { get; } = alias;

		public override bool IsLiteral => Expr.IsLiteral;
		internal override bool Match(DbExpression other) 
			=> other is AliasedExpression a && a.Alias == Alias && a.Expr.Match(Expr);
		public override string ToString() => $"Alias({Expr} as {Alias})";
	}

	class CallExpression(ReferenceExpression func, DbExpression[] args) : DbExpression
	{
		public ReferenceExpression Function { get; } = func;
		public DbExpression[] Args { get; } = args;

		internal override bool Match(DbExpression other)
			=> other is CallExpression ce
			   && ce.Function.Match(Function)
			   && ce.Args.Length == Args.Length
			   && ce.Args.Zip(Args).All(p => p.First.Match(p.Second));
	}

	enum BoolExpressionType
	{
		Equals,
		NotEquals,
		GreaterThan,
		GreaterThanOrEqual,
		LessThan,
		LessThanOrEqual,
	}

	class BooleanExpression(BoolExpressionType type, DbExpression left, DbExpression right) : DbExpression
	{
		public BoolExpressionType Op { get; } = type;
		public DbExpression Left { get; } = left;
		public DbExpression Right { get; } = right;

		public string OpString => Op switch {
			BoolExpressionType.Equals => "==",
			BoolExpressionType.NotEquals => "!=",
			BoolExpressionType.GreaterThan => ">",
			BoolExpressionType.GreaterThanOrEqual => ">=",
			BoolExpressionType.LessThan => "<",
			BoolExpressionType.LessThanOrEqual => "<=",
			_ => throw new NotImplementedException(), 
		};

		internal override bool Match(DbExpression other)
			=> other is BooleanExpression be && be.Op == Op && be.Left.Match(Left) && be.Right.Match(Right);

		public override string ToString() => $"{Left} {OpString} {Right}";
	}

	class AggregateExpression(string name, DbExpression arg) : DbExpression
	{
		public string Name { get; } = name;
		public DbExpression Arg { get; } = arg;

		internal override bool Match(DbExpression other) => other is AggregateExpression agg && agg.Name == Name && agg.Arg.Match(Arg);

		public override string ToString() => $"{Name}({Arg})";
	}

	class CountExpression : DbExpression
	{
		internal override bool Match(DbExpression other) => other is CountExpression;
	}

	abstract class LiteralExpression : DbExpression
	{
		public override bool IsLiteral => true;
	}

	class IntegerLiteralExpression(int value) : LiteralExpression
	{
		public int Value { get; } = value;

		internal override bool Match(DbExpression other) => other is IntegerLiteralExpression il && il.Value == Value;
		public override string ToString() => Value.ToString();
	}

	class StringLiteralExpression(string value) : LiteralExpression
	{
		public string Value { get; } = value;

		internal override bool Match(DbExpression other) => other is StringLiteralExpression sl && sl.Value == Value;
		public override string ToString() => Value.ToLiteral();
	}

	class NullLiteralExpression : LiteralExpression
	{
		internal override bool Match(DbExpression other) => other is NullLiteralExpression;

		public override string ToString() => "DBNull.Value";
	}

	class CSharpStringExpression(string value) : DbExpression
	{
		public string Value { get; } = value;

		internal override bool Match(DbExpression other) => other is CSharpStringExpression cs && cs.Value == Value;
		public override string ToString() => Value;
	}

	class OrderingExpression(DbExpression expr, bool desc) : DbExpression
	{
		public DbExpression Expr { get; } = expr;
		public bool Desc { get; } = desc;

		internal override bool Match(DbExpression other) => other is OrderingExpression oe && oe.Desc == Desc && oe.Expr.Match(Expr);

		public override string ToString() => Desc ? $"{Expr} descending" : Expr.ToString()!;
	}

	class Iteration(ReferenceExpression iter, ReferenceExpression collection) : DbExpression
	{
		public ReferenceExpression Iter { get; } = iter;
		public ReferenceExpression Collection { get; } = collection;

		internal override bool Match(DbExpression other) => other is Iteration i && Iter.Match(i.Iter) && Collection.Match(i.Collection);

		public override string ToString() => $"{Iter} in {Collection}";
	}

	class LinqJoin(Iteration iteration, MemberReferenceExpression onLeft, MemberReferenceExpression onRight) : DbExpression
	{
		public Iteration Iteration { get; } = iteration;
		public MemberReferenceExpression OnLeft { get; } = onLeft;
		public MemberReferenceExpression OnRight { get; } = onRight;

		internal override bool Match(DbExpression other) => other is LinqJoin lj && Iteration.Match(lj.Iteration) && OnLeft.Match(lj.OnLeft) && OnRight.Match(lj.OnRight);

		public override string ToString() => $"join {Iteration} on {OnLeft} equals {OnRight}";
	}

	class LinqQuery(Iteration fromIter, LinqJoin[]? joins, DbExpression? filter, OrderingExpression[]? ordering, DbExpression[] outputs) : DbExpression
	{
		public Iteration FromIter { get; } = fromIter;
		public LinqJoin[]? Joins { get; } = joins;
		public DbExpression? Filter { get; } = filter;
		public OrderingExpression[]? Ordering { get; } = ordering;
		public DbExpression[] Outputs { get; } = outputs;

		internal override bool Match(DbExpression other)
		{
			if (other is not LinqQuery lq) { 
				return false; 
			}
			if (!FromIter.Match(lq.FromIter)) {
				return false;
			}
			if (Joins?.Length != lq.Joins?.Length) {
				return false;
			}
			if (Joins != null && !(Joins.Zip(lq.Joins!).All(pair => pair.First.Match(pair.Second)))) {
				return false;
			}
			if ((Filter == null) != (lq.Filter == null)) {
				return false;
			}
			if (Filter != null && !Filter.Match(lq.Filter!)) { 
				return false; 
			}
			if (Ordering?.Length != lq.Ordering?.Length) {
				return false;
			}
			if (Ordering != null && !(Ordering.Zip(lq.Ordering!).All(pair => pair.First.Match(pair.Second)))) {
				return false;
			}
			if (Outputs.Length != lq.Outputs.Length) {
				return false;
			}
			if (!(Outputs.Zip(lq.Outputs!).All(pair => pair.First.Match(pair.Second)))) {
				return false;
			}
			return true;
		}

		public override string ToString()
		{
			var joins = Joins is null ? string.Empty : ' ' + string.Join(' ', (IEnumerable<LinqJoin>)Joins);
			var filter = Filter == null ? string.Empty : " where " + Filter.ToString();
			var order = Ordering == null ? string.Empty : " orderby " + string.Join(' ', (IEnumerable<OrderingExpression>)Ordering);
			var fields = Outputs.Select(GetOutputName).ToArray();
			return $"from {FromIter}{joins}{filter}{order} select new {{ {string.Join(", ", fields)} }}";
		}

		private static string GetOutputName(DbExpression expression) => expression switch {
			MemberReferenceExpression mre => mre.ToString(),
			AliasedExpression ae => $"{ae.Alias} = {GetOutputName(ae.Expr)}",
			_ => throw new NotImplementedException()
		};
	}
}
