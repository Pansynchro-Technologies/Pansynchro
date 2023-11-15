using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.Core.Helpers;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	abstract class SqlModel(DataModel model)
	{
		public DataModel Model { get; set; } = model;

		abstract public Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports);

		abstract public string? Validate();
	}

	abstract class StreamedSqlModel(DataModel model) : SqlModel(model)
	{
		public override string? Validate() 
			=> Model.Ordering?.Length > 0 ? "ORDER BY is not supported for queries involving a STREAM input." : null;

		protected void WriteJoins(IndexData indices, List<string> filters, List<CSharpStatement> whileBody)
		{
			foreach (var j in Model.Joins) {
				var tableName = j.Target.Name.ToPropertyName();
				foreach (var tf in j.TargetFields) {
					var idx = indices.Lookup(tf);
					var varName = tf.Parent.Name;
					if (idx.Unique) {
						var sourceField = LookupSourceField(tf, j.Condition);
						whileBody.Add(new VarDecl(varName, new CSharpStringExpression($"__db.{idx.Name}.GetByUniqueIndex({GetInput(sourceField)})")));
					} else {
						throw new NotImplementedException("Joins on a non-unique index are not yet available.");
					}
					if (j.Type == JoinType.Inner) {
						filters.Add($"{varName} != null");
					}
				}
			}
		}

		protected string GetInput(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => GetMreInput(mre),
			AliasedExpression ae => GetInput(ae.Expr),
			BooleanExpression be => $"{GetInput(be.Left)} {be.OpString} {GetInput(be.Right)}",
			LiteralExpression => expr.ToString()!,
			AggregateExpression ag => GetInput(ag.Arg),
			_ => throw new NotImplementedException()
		};

		protected static MemberReferenceExpression GetInputField(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => mre,
			AliasedExpression ae => GetInputField(ae.Expr),
			AggregateExpression ag => GetInputField(ag.Arg),
			CountExpression => new(new("__agg__"), "__Count__"),
			_ => throw new NotImplementedException()
		};

		private string GetMreInput(MemberReferenceExpression mre) {
			if (mre.Parent.Name == Model.Inputs[0].Name) {
				var fields = Model.Inputs[0].Stream.Fields;
				var idx = fields.IndexWhere(f => f.Name.Equals(mre.Name, StringComparison.InvariantCultureIgnoreCase)).First();
				var field = fields[idx];
				return TypesHelper.FieldTypeToGetter(field.Type, idx);
			}
			var table = Model.Inputs.First(t => t.Name == mre.Parent.Name);
			var fld = table.Stream.Fields.First(f => f.Name.Equals(mre.Name, StringComparison.InvariantCultureIgnoreCase));
			return $"{table.Name}.{fld.Name}";
		}

		protected static DbExpression LookupSourceField(MemberReferenceExpression tf, BooleanExpression cond)
		{
			if (cond.Right == tf) {
				return cond.Left;
			}
			throw new NotImplementedException();
		}

		protected string GetOutput(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => GetMreOutput(mre),
			CountExpression => GetMreOutput(new(new("__agg__"), "__Count__")),
			_ => throw new NotImplementedException()
		};
		
		private string GetMreOutput(MemberReferenceExpression field)
		{
			var name = field.ToString();
			var idx = Model.Outputs.IndexWhere(e => GetInputField(e).Match(field)).First();
			return $"result[{idx}]";
		}
	}

	abstract class MemorySqlModel(DataModel model) : SqlModel(model)
	{
		protected DbExpression BuildLinqExpression(DataModel model)
		{
			var start = model.Inputs[0];
			var iter = GetIteration(start);
			var joins = model.Joins.Length > 0 ? model.Joins.Select(GetLinqJoin).ToArray() : null;
			var filter = model.Filter == null ? null : GetField(model.Filter);
			var ordering = model.Ordering != null ? model.Ordering.Select(GetField).Cast<OrderingExpression>().ToArray() : null;
			var hasQuery = (filter != null || joins != null || ordering != null);
			return hasQuery ? new LinqQuery(iter, joins, filter, ordering, model.Outputs.Where(e => !e.IsLiteral).Select(GetField).ToArray()) : iter.Collection;
		}

		private LinqJoin GetLinqJoin(JoinSpec spec)
		{
			var iter = GetIteration(spec.Target);
			if (spec.Condition is BooleanExpression { Op: BoolExpressionType.Equals, Left: MemberReferenceExpression l, Right: MemberReferenceExpression r}) {
				return new LinqJoin(iter, GetField(l), GetField(r));
			}
			throw new NotImplementedException();
		}

		private DbExpression GetField(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => GetField(mre),
			AliasedExpression ae => new AliasedExpression(GetField(ae.Expr), ae.Alias),
			BooleanExpression be => new BooleanExpression(be.Op, GetField(be.Left), GetField(be.Right)),
			AggregateExpression agg => GetField(agg.Arg),
			OrderingExpression ord => new OrderingExpression(GetField(ord.Expr), ord.Desc),
			CountExpression => new MemberReferenceExpression(new("__agg__"), "__Count__"),
			LiteralExpression => expr,
			_ => throw new NotImplementedException()
		};

		private MemberReferenceExpression GetField(MemberReferenceExpression r)
		{
			var table = r.Parent;
			var stream = Model.Inputs.First(i => i.Name.Equals(table.Name, StringComparison.InvariantCultureIgnoreCase));
			var field = stream.Stream.Fields.First(f => f.Name.Equals(r.Name, StringComparison.InvariantCultureIgnoreCase));
			return new MemberReferenceExpression(new("__" + stream.Name), field.Name);
		}

		private static Iteration GetIteration(TableReference table) => new(new("__" + table.Name), new MemberReferenceExpression(new("__db"), table.Stream.Name.Name));

		protected string GetInput(DbExpression expr)
		{
			if (expr.IsLiteral) {
				return GetLiteralValue(expr);
			}
			var result = expr switch {
				MemberReferenceExpression mre => GetMreInput(mre),
				AliasedExpression ae => GetAliasedInput(ae),
				AggregateExpression ag => GetInput(ag.Arg),
				_ => throw new NotImplementedException()
			};
			return expr.Type?.Nullable == true ? $"(object?){result} ?? DBNull.Value" : result;
		}

		private static string GetLiteralValue(DbExpression expr) => expr is AliasedExpression ae ? ae.Expr.ToString()! : expr.ToString()!;

		protected static MemberReferenceExpression GetInputField(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => mre,
			AliasedExpression ae => GetInputField(ae.Expr),
			AggregateExpression ag => GetInputField(ag.Arg),
			CountExpression => new(new("__agg__"), "__Count__"),
			_ => throw new NotImplementedException()
		};

		private static string GetAliasedInput(AliasedExpression ae) => "__item." + ae.Alias;

		private string GetMreInput(MemberReferenceExpression mre)
		{
			var name = GetField(mre).Name;
			return new MemberReferenceExpression(new("__item"), name).ToString();
		}

	}
}
