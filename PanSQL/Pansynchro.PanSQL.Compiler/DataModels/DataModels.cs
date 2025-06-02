using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
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
		string[] ScriptVariables,
		AggregateExpression[] AggOutputs,
		BooleanExpression? AggFilter,
		OrderingExpression[]? Ordering,
		string? OutputTable);

	enum TableType
	{
		Stream,
		Table,
		Cte
	}

	record TableReference(string Name, TableType Type, StreamDefinition Stream)
	{
		public TableReference(Ast.Variable t) :
			this(
				t.Name,
				t.Type switch { "Table" => TableType.Table, "Stream" => TableType.Stream, "Cte" => TableType.Cte, _ => throw new ArgumentException($"Invalid table type: {t.Type}") },
				(t.Declaration as VarDeclaration)?.Stream ?? ((SqlTransformStatement)t.Declaration).Ctes.First(c => c.Name == t.Name).Stream
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
		internal IFieldType? Type { get; set; }

		internal abstract bool Match(DbExpression other);

		virtual public bool IsLiteral => false;

		protected bool MatchAll(DbExpression[] l, DbExpression[] r) => l.Length == r.Length
			   && l.Zip(r).All(p => p.First.Match(p.Second));
	}

	class ReferenceExpression(string name) : DbExpression
	{
		public string Name { get; } = name;

		internal override bool Match(DbExpression other) 
			=> other is ReferenceExpression r && r.GetType() == this.GetType() && r.Name == Name;

		public override string ToString() => Name;
	}

	class MemberReferenceExpression(ReferenceExpression parent, string name) : ReferenceExpression(name)
	{
		public ReferenceExpression Parent { get; } = parent;

		public override string ToString() => $"{Parent}.{Name}";

		internal override bool Match(DbExpression other)
			=> other is MemberReferenceExpression mr && mr.Name == Name && mr.Parent.Match(Parent);
	}

	class VariableReferenceExpression(string name) : ReferenceExpression(name)
	{
		public string ScriptVarName { get; set; } = null!;
	}

	class StarExpression(ReferenceExpression? table) : DbExpression
	{
		public ReferenceExpression? Table { get; } = table;
		internal override bool Match(DbExpression other)
			=> other is StarExpression s && ((s.Table == null && Table == null) || ((s.Table != null && Table != null && s.Table.Match(Table))));
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
		public ReferenceExpression Function { get; internal set; } = func;
		public DbExpression[] Args { get; } = args;

		public bool IsProp { get; internal set; }
		public bool IsStaticProp { get; internal set; }
		public Func<DbExpression[], Func<DbExpression, string>?, string>? SpecialCodegen { get; internal set; }

		internal override bool Match(DbExpression other)
			=> other is CallExpression ce
			   && ce.Function.Match(Function)
			   && MatchAll(ce.Args, Args);

		public override string ToString() => $"{Function}({string.Join<DbExpression>(", ", Args)})";
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

	enum UnaryExpressionType
	{
		Plus,
		Minus,
		Not,
	}

	class UnaryExpression(UnaryExpressionType type, DbExpression value) : DbExpression
	{
		public UnaryExpressionType Op { get; } = type;
		public DbExpression Value { get; } = value;

		public string OpString => Op switch {
			UnaryExpressionType.Plus => "+",
			UnaryExpressionType.Minus => "-",
			UnaryExpressionType.Not => "!",
			_ => throw new NotImplementedException(),
		};

		internal override bool Match(DbExpression other)
			=> other is UnaryExpression ue && ue.Op == Op && ue.Value.Match(Value);

		public override string ToString() => $"{OpString}({Value})";
	}

	enum BinExpressionType
	{
		And,
		Or,
		Add,
		Subtract,
		Multiply,
		Divide,
		Mod,
		BitAnd,
		BitOr,
		BitXor,
	}

	class BinaryExpression(BinExpressionType type, DbExpression left, DbExpression right) : DbExpression
	{
		public BinExpressionType Op { get; } = type;
		public DbExpression Left { get; } = left;
		public DbExpression Right { get; } = right;

		public string OpString => Op switch {
			BinExpressionType.And => "&&",
			BinExpressionType.Or => "||",
			BinExpressionType.Add => "+",
			BinExpressionType.Subtract => "-",
			BinExpressionType.Multiply => "*",
			BinExpressionType.Divide => "/",
			BinExpressionType.Mod => "%",
			BinExpressionType.BitAnd => "&",
			BinExpressionType.BitOr => "|",
			BinExpressionType.BitXor => "^",
			_ => throw new NotImplementedException(),
		};

		internal override bool Match(DbExpression other)
			=> other is BinaryExpression be && be.Op == Op && be.Left.Match(Left) && be.Right.Match(Right);

		public override string ToString() => $"{Left} {OpString} {Right}";
	}

	class IsNullExpression(DbExpression value) : DbExpression
	{
		public DbExpression Value { get; } = value;

		internal override bool Match(DbExpression other) => other is IsNullExpression i && i.Value.Match(Value);

		public override string ToString() => $"{Value} == null";
	}

	class LikeExpression(DbExpression left, DbExpression right) : DbExpression
	{
		public DbExpression Left { get; } = left;
		public DbExpression Right { get; } = right;

		internal override bool Match(DbExpression other)
			=> other is LikeExpression l && l.Left.Match(Left) && l.Right.Match(Right);

		public override string ToString() => $@"LikeImpl.Like({Left}, {Right}, '\\')";
	}

	class AggregateExpression(string name, params DbExpression[] args) : DbExpression
	{
		public string Name { get; } = name;
		public DbExpression[] Args { get; } = args;

		internal override bool Match(DbExpression other) 
			=> other is AggregateExpression agg && agg.Name == Name && MatchAll(agg.Args, Args);

		public override string ToString() => $"{Name}({string.Join<DbExpression>(", ", Args)})";
	}

	class CollectionExpression(DbExpression[] values) : DbExpression
	{
		public DbExpression[] Values { get; } = values;

		public override bool IsLiteral => Values.All(v => v.IsLiteral);

		internal override bool Match(DbExpression other) =>
			other is CollectionExpression c && c.Values.Length == Values.Length && c.Values.Zip(Values).All(pair => pair.First.Match(pair.Second));

		public override string ToString() => $"new[]{{{string.Join<DbExpression>(", ", Values)}}}";
	}

	class ContainsExpression(DbExpression collection, DbExpression value) : DbExpression
	{
		public DbExpression Collection { get; } = collection;
		public DbExpression Value { get; } = value;

		public override bool IsLiteral => Collection.IsLiteral && Value.IsLiteral;
		internal override bool Match(DbExpression other) =>
			other is ContainsExpression ce && ce.Collection.Match(Collection) && ce.Value.Match(Value);

		public override string ToString() => $"{Collection}.Contains({Value})";
	}

	class CountExpression : DbExpression
	{
		internal override bool Match(DbExpression other) => other is CountExpression;
	}

	class CastExpression : DbExpression
	{
		public CastExpression(DbExpression value, IFieldType type)
		{
			Value = value;
			Type = type;
		}

		public DbExpression Value { get; }

		internal override bool Match(DbExpression other)
		{
			throw new NotImplementedException();
		}

		public override string ToString() => $"(({TypesHelper.FieldTypeToCSharpType(Type!)}){Value})";
	}

	class TryCastExpression : DbExpression
	{
		public TryCastExpression(DbExpression value, IFieldType type)
		{
			Value = value;
			Type = type;
		}

		public DbExpression Value { get; }

		internal override bool Match(DbExpression other)
		{
			throw new NotImplementedException();
		}

		public override string ToString()
		{
			var suffix = TypesHelper.IsReferenceType(Type!) ? 'R' : 'V';
			return $"SqlFunctions.TryCast{suffix}<{TypesHelper.FieldTypeToCSharpType(Type!).TrimEnd('?')}, {TypesHelper.FieldTypeToCSharpType(Value.Type!).TrimEnd('?')}>({Value})";
		}
	}

	class ParseExpression : DbExpression
	{
		public ParseExpression(DbExpression value, IFieldType type, bool isTry)
		{
			Value = value;
			Type = isTry ? type.MakeNull() : type;
			IsTry = isTry;
		}

		public DbExpression Value { get; }

		public bool IsTry { get; }

		internal override bool Match(DbExpression other)
		{
			throw new NotImplementedException();
		}

		public override string ToString()
		{
			var suffix = TypesHelper.IsReferenceType(Type) ? 'R' : 'V';
			return IsTry
			? $"SqlFunctions.TryParse{suffix}<{TypesHelper.FieldTypeToCSharpType(Type!).TrimEnd('?')}>({Value})"
			: $"{TypesHelper.FieldTypeToCSharpType(Type!).TrimEnd('?')}>.TryParse{suffix}({Value})";
		}
	}

	class IfThenExpression(DbExpression cond, DbExpression result) : DbExpression
	{
		public DbExpression Cond { get; } = cond;
		public DbExpression Result { get; } = result;

		internal override bool Match(DbExpression other)
		{
			throw new NotImplementedException();
		}
	}

	class IfExpression(IfThenExpression[] cases, DbExpression? elseCase) : DbExpression
	{
		public IfThenExpression[] Cases { get; } = cases;
		public DbExpression? ElseCase { get; } = elseCase;

		internal override bool Match(DbExpression other)
		{
			throw new NotImplementedException();
		}
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

	class FloatLiteralExpression(double value) : LiteralExpression
	{
		public double Value { get; } = value;

		internal override bool Match(DbExpression other) => other is FloatLiteralExpression dl && dl.Value == Value;
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

	class LinqQuery(Iteration fromIter, LinqJoin[]? joins, DbExpression? filter, OrderingExpression[]? ordering, DbExpression[]? outputs) : DbExpression
	{
		public Iteration FromIter { get; } = fromIter;
		public LinqJoin[]? Joins { get; } = joins;
		public DbExpression? Filter { get; } = filter;
		public OrderingExpression[]? Ordering { get; } = ordering;
		public DbExpression[]? Outputs { get; } = outputs;

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
			if (Outputs?.Length != lq.Outputs?.Length) {
				return false;
			}
			if (Outputs != null && !(Outputs.Zip(lq.Outputs!).All(pair => pair.First.Match(pair.Second)))) {
				return false;
			}
			return true;
		}

		public override string ToString()
		{
			var joins = Joins is null ? string.Empty : ' ' + string.Join(' ', (IEnumerable<LinqJoin>)Joins);
			var filter = Filter == null ? string.Empty : " where " + Filter.ToString();
			var order = Ordering == null ? string.Empty : " orderby " + string.Join(' ', (IEnumerable<OrderingExpression>)Ordering);
			var fields = Outputs?.Select(GetOutputName).ToArray();
			var fieldCode = fields == null ? FromIter.Iter.ToString() : $"new {{ {string.Join(", ", fields)} }}";
			return $"from {FromIter}{joins}{filter}{order} select {fieldCode}";
		}

		internal static string GetOutputName(DbExpression expression) => expression switch {
			MemberReferenceExpression mre => mre.ToString(),
			AliasedExpression ae => $"{ae.Alias} = {GetOutputName(ae.Expr)}",
			BinaryExpression bin => $"{GetOutputName(bin.Left)} {bin.OpString} {GetOutputName(bin.Right)}",
			CallExpression ce => ce.SpecialCodegen != null ? ce.SpecialCodegen(ce.Args, GetOutputName) : $"{ce.Function}({string.Join(", ", ce.Args.Select(GetOutputName))})",
			LiteralExpression => expression.ToString()!,
			_ => throw new NotImplementedException()
		};
	}
}
