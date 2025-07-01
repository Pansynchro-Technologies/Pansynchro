using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.Helpers;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	abstract class SqlModel(DataModel model)
	{
		public DataModel Model { get; set; } = model;

		abstract public Method GetScript(CodeBuilder cb, IndexData indices, List<ImportModel> imports, Dictionary<string, string> ctes);

		abstract public string? Validate();

		abstract protected string GetInput(DbExpression expr);

		protected IEnumerable<CSharpStatement> InvokeCtes(Dictionary<string, string> ctes, bool streamInput = true)
		{
			foreach (var input in Model.Inputs) {
				if (input.Type == TableType.Cte) {
					yield return new ExpressionStatement(new CallExpression(new(ctes[input.Name]), streamInput ? [new ReferenceExpression("r")] : []));
				}
			}
		}

		protected VarDecl BuildAggregateProcessor(
			CodeBuilder cb,
			AggregateExpression agg,
			MemberReferenceExpression[]? groupKey,
			Dictionary<AggregateExpression, string> aggs)
		{
			var name = cb.NewNameReference("aggregator");
			aggs.Add(agg, name.Name);
			var callName = "Aggregates." + agg.Name;
			var typeArgs = groupKey == null ? "bool" : string.Join(", ", groupKey.Select(mre => TypesHelper.FieldTypeToCSharpType(mre.Type!)));
			var typeArg2 = TypesHelper.FieldTypeToCSharpType(agg.Type!);
			var args = agg.Args?.Length > 1 ? string.Join(", ", agg.Args[1..].Select(GetInput)) : "";
			var invocation = agg.Name == "Count" ? $"{callName}<{typeArgs}>()" : $"{callName}<{typeArgs}, {typeArg2}>({args})";
			return new VarDecl(name.Name, new CSharpStringExpression(invocation));
		}

		protected string GetFieldSet(MemberReferenceExpression[] group)
			=> group == null ? "true" : group.Length == 1 ? GetInput(group[0]) : $"({string.Join(", ", group.Select(GetInput))})";

		protected CSharpStatement WriteHavingClause(BooleanExpression having)
		{
			var filter = GetAggInput(having);
			return new CSharpStringExpression($"if (!({filter})) continue");
		}

		protected DbExpression GetAggInput(DbExpression expr) => expr switch {
			BinaryExpression bin => new BinaryExpression(bin.Op, GetAggInput(bin.Left), GetAggInput(bin.Right)),
			BooleanExpression be => new BooleanExpression(be.Op, GetAggInput(be.Left), GetAggInput(be.Right)),
			AggregateExpression agg => GetAggInput(agg),
			AliasedExpression ae => GetAggInput(ae.Expr),
			MemberReferenceExpression mre => GetAggInput(mre),
			LiteralExpression => expr,
			_ => throw new NotImplementedException()
		};

		private CSharpStringExpression GetAggInput(MemberReferenceExpression mre)
		{
			for (int i = 0; i < Model.GroupKey!.Length; ++i) {
				if (mre.Match(Model.GroupKey[i])) {
					return new CSharpStringExpression(Model.GroupKey.Length == 1 ? "pair.Key" : $"pair.Key.Item{i + 1}");
				}
			}
			throw new Exception($"Field '{mre}' does not match any field declared in the GROUP BY clause");
		}

		private CSharpStringExpression GetAggInput(AggregateExpression agg)
		{
			for (int i = 0; i < Model.AggOutputs.Length; ++i) {
				if (agg.Match(Model.AggOutputs[i])) {
					return new CSharpStringExpression(Model.AggOutputs.Length == 1 ? "pair.Value" : $"pair.Value.Item{i + 1}");
				}
			}
			throw new Exception($"HAVING aggregate '{agg}' does not match any aggregate declared in the SELECT clause");
		}

		protected LinqJoin GetLinqJoin(JoinSpec spec)
		{
			var iter = GetIteration(spec.Target);
			if (spec.Condition is BooleanExpression { Op: BoolExpressionType.Equals, Left: MemberReferenceExpression l, Right: MemberReferenceExpression r }) {
				return new LinqJoin(iter, GetField(l), GetField(r));
			}
			throw new NotImplementedException();
		}

		protected static Iteration GetIteration(TableReference table) => new(new("__" + table.Name), new MemberReferenceExpression(new("__db"), table.Stream.Name.ToTableName()));

		protected DbExpression GetField(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => GetField(mre),
			CollectionExpression coll => new CollectionExpression([.. coll.Values.Select(GetField)]),
			AliasedExpression ae => new AliasedExpression(GetField(ae.Expr), ae.Alias),
			UnaryExpression ue => new UnaryExpression(ue.Op, GetField(ue.Value)),
			BinaryExpression bin => new BinaryExpression(bin.Op, GetField(bin.Left), GetField(bin.Right)),
			BooleanExpression be => new BooleanExpression(be.Op, GetField(be.Left), GetField(be.Right)),
			LikeExpression lk => new LikeExpression(GetField(lk.Left), GetField(lk.Right)),
			ContainsExpression cont => new ContainsExpression(GetField(cont.Collection), GetField(cont.Value)),
			CallExpression ce => new CallExpression(ce.Function, ce.Args.Select(GetField).ToArray()),
			CastExpression cast => GetField(cast),
			TryCastExpression tCast => GetField(tCast),
			AggregateExpression agg => GetField(agg.Args[0]),
			OrderingExpression ord => new OrderingExpression(GetField(ord.Expr), ord.Desc),
			CountExpression => new MemberReferenceExpression(new("__agg__"), "__Count__"),
			IsNullExpression ine => new IsNullExpression(GetField(ine.Value)),
			LiteralExpression => expr,
			VariableReferenceExpression vr => new MemberReferenceExpression(new("Program"), vr.ScriptVarName),
			_ => throw new NotImplementedException()
		};

		protected MemberReferenceExpression GetField(MemberReferenceExpression r)
		{
			var table = r.Parent;
			var stream = Model.Inputs.First(i => i.Name.Equals(table.Name, StringComparison.InvariantCultureIgnoreCase));
			var field = stream.Stream.Fields.First(f => f.Name.Equals(r.Name, StringComparison.InvariantCultureIgnoreCase));
			return new MemberReferenceExpression(new("__" + stream.Name), field.Name) { Type = r.Type };
		}

		protected DbExpression GetField(CastExpression expr)
		{
			var type = TypesHelper.FieldTypeToDotNetType(expr.Type!);
			var fromType = TypesHelper.FieldTypeToDotNetType(expr.Value.Type!);
			var fromValue = GetField(expr.Value);
			if (fromType == type) {
				return fromValue;
			}
			if (type.IsArray && type.GetElementType() == fromType) {
				return GetField(new CollectionExpression([fromValue]));
			}
			if (fromType == typeof(string)) {
				if (type.GetInterfaces()?.Any(i => i.Name.StartsWith("IParsable")) == true) {
					return new ParseExpression(fromValue, expr.Type!, false);
				}
			}
			return new CastExpression(fromValue, expr.Type!);
		}

		protected DbExpression GetField(TryCastExpression expr)
		{
			var type = TypesHelper.FieldTypeToDotNetType(expr.Type!);
			var fromType = TypesHelper.FieldTypeToDotNetType(expr.Value.Type!);
			var fromValue = GetField(expr.Value);
			if (fromType == type) {
				return fromValue;
			}
			if (type.IsArray && type.GetElementType() == fromType) {
				return GetField(new CollectionExpression([fromValue]));
			}
			if (expr.Type!.Nullable && !TypesHelper.IsReferenceType(expr.Type)) {
				type = Nullable.GetUnderlyingType(type);
			}
			if (fromType == typeof(string)) {
				if (type.GetInterfaces()?.Any(i => i.Name.StartsWith("IParsable")) == true) {
					return new ParseExpression(fromValue, expr.Type!, true);
				}
			}
			return new TryCastExpression(fromValue, expr.Type!);
		}

		protected void GetFields(DbExpression expr, HashSet<DbExpression> fields)
		{
			switch (expr) {
				case MemberReferenceExpression mre: fields.Add(GetField(mre)); break;
				case AliasedExpression ae: fields.Add(GetField(ae)); break;
				case BinaryExpression bin:
					GetFields(bin.Left, fields);
					GetFields(bin.Right, fields);
					break;
				case BooleanExpression be:
					GetFields(be.Left, fields);
					GetFields(be.Right, fields);
					break;
				case CallExpression ce:
					foreach (var arg in ce.Args) {
						GetFields(arg, fields);
					}
					break;
				case AggregateExpression agg: GetFields(agg.Args[0], fields); break;
				case OrderingExpression ord: GetFields(ord.Expr, fields); break;
				case CountExpression: fields.Add(new MemberReferenceExpression(new("__agg__"), "__Count__")); break;
				case LiteralExpression: break;
				default: throw new NotImplementedException();
			}
		}

		protected static bool IsPassThrough(FieldDefinition[] inFields, HashSet<DbExpression> outFields)
		{
			if (outFields.Any(f => f is not MemberReferenceExpression)) {
				return false;
			}
			var outMres = outFields.Cast<MemberReferenceExpression>().ToArray();
			var tableRefs = outMres.Select(m => m.Parent.Name).Distinct().ToArray();
			if (tableRefs.Length != 1) {
				return false;
			}
			foreach (var mre in outMres) {
				var fieldName = mre.Name;
				if (!inFields.Any(f => f.Name == fieldName)) {
					return false;
				}
			}
			return true;
		}

		private string SpecialCodegen(CallExpression call)
		{
			var result = call.SpecialCodegen!(call.Args, GetInput);
			if (result.Contains("System.DBNull.Value")) {
				result = result.Replace("System.DBNull.Value", "null");
			}
			if (result.Contains("?.")) {
				result = $"((object)({result.Replace(" ?? null", "").Replace("(object)", "")}) ?? System.DBNull.Value)";
			}
			return result;
		}

		protected string GetInput(DbExpression[] args) => string.Join(", ", args.Select(GetInput));

		protected string GetInput(CallExpression call)
		{
			if (call.SpecialCodegen != null) {
				return SpecialCodegen(call);
			}
			if (call.IsStaticProp) {
				return call.Function.ToString();
			}
			if (call.IsProp) {
				return $"{GetInput(call.Args[0])}.{call.Function}";
			}
			return $"{call.Function}({GetInput(call.Args)})";
		}
	}

	abstract class StreamedSqlModel(DataModel model) : SqlModel(model)
	{
		protected bool HasCtes { get; set; }

		public override string? Validate()
		{
			if (Model.Ordering?.Length > 0 && Model.Inputs.Any(t => t.Type == TableType.Stream)) {
				return "ORDER BY is not supported for queries involving a STREAM input.";
			}
			return null;
		}

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

		protected override string GetInput(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => GetMreInput(mre),
			AliasedExpression ae => GetInput(ae.Expr),
			IsNullExpression isn => $"({GetInput(isn.Value)} == System.DBNull.Value)",
			UnaryExpression ue => $"{ue.OpString}({GetInput(ue.Value)})",
			BooleanExpression be => $"{GetInput(be.Left)} {be.OpString} {GetInput(be.Right)}",
			BinaryExpression b2 => $"{GetInput(b2.Left)} {b2.OpString} {GetInput(b2.Right)}",
			CollectionExpression col => $"[{string.Join(", ", col.Values.Select(GetInput))}]",
			ContainsExpression con => $"{GetInput(con.Collection)}.Contains({GetInput(con.Value)})",
			LiteralExpression => expr.ToString()!,
			AggregateExpression ag => GetInput(ag.Args),
			CallExpression call => GetInput(call),
			CastExpression cast => GetInput(cast),
			LikeExpression like => $@"LikeImpl.Like({GetInput(like.Left)}, {GetInput(like.Right)}, '\\')",
			IfExpression ie => GetInput(ie),
			VariableReferenceExpression vr => $"Program.{vr.ScriptVarName}",
			_ => throw new NotImplementedException()
		};

		protected string GetInput(CastExpression cast)
		{
			var type = TypesHelper.FieldTypeToDotNetType(cast.Type!);
			var fromType = TypesHelper.FieldTypeToDotNetType(cast.Value.Type!);
			var fromValue = GetInput(cast.Value);
			if (fromType == type) {
				return fromValue;
			}
			if (type.IsArray && type.GetElementType() == fromType) {
				return '[' + fromValue + ']';
			}
			if (fromType == typeof(string)) {
				if (type.GetInterfaces()?.Any(i => i.Name.StartsWith("IParsable")) == true) {
					return $"{TypesHelper.FieldTypeToCSharpType(cast.Type!)}.Parse({fromValue})";
				}
			}
			return $"({TypesHelper.FieldTypeToCSharpType(cast.Type!)}){fromValue}";
		}

		protected string GetInput(IfExpression expr)
		{
			var result = string.Concat(expr.Cases.Select(c => $"{GetInput(c.Cond)} ? {GetInput(c.Result)} : "));
			result += expr.ElseCase == null ? "System.DBNull.Value" : GetInput(expr.ElseCase);
			return result;
		}

		protected static MemberReferenceExpression GetInputField(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => mre,
			AliasedExpression ae => GetInputField(ae.Expr),
			AggregateExpression ag => GetInputField(ag.Args[0]),
			CountExpression => new(new("__agg__"), "__Count__"),
			_ => throw new NotImplementedException()
		};

		private string GetMreInput(MemberReferenceExpression mre) {
			if (HasCtes) {
				var name = GetField(mre).Name;
				return new MemberReferenceExpression(new("__item"), name).ToString();
			}
			if (mre.Parent.Name == Model.Inputs[0].Name && Model.Inputs[0].Type == TableType.Stream) {
				var fields = Model.Inputs[0].Stream.Fields;
				var idx = fields.IndexWhere(f => f.Name.Equals(mre.Name, StringComparison.InvariantCultureIgnoreCase)).First();
				var field = fields[idx];
				return TypesHelper.FieldTypeToGetter(field.Type, idx);
			}
			var table = Model.Inputs.First(t => t.Name == mre.Parent.Name);
			var fld = table.Stream.Fields.First(f => f.Name.Equals(mre.Name, StringComparison.InvariantCultureIgnoreCase));
			return $"{table.Name}.{fld.Name.ToPropertyName()}";
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
		protected DbExpression BuildLinqExpression(DataModel model)
		{
			var start = model.Inputs[0];
			var iter = GetIteration(start);
			var joins = model.Joins.Length > 0 ? model.Joins.Select(GetLinqJoin).ToArray() : null;
			var filter = model.Filter == null ? null : GetField(model.Filter);
			var ordering = model.Ordering != null ? model.Ordering.Select(GetField).Cast<OrderingExpression>().ToArray() : null;
			var hasQuery = (filter != null || joins != null || ordering != null);
			if (hasQuery) {
				var fields = new HashSet<DbExpression>(EqualityComparer<DbExpression>.Create((l, r) => l?.ToString() == r?.ToString(), expr => expr.ToString()!.GetHashCode()));
				foreach (var o in model.Outputs) {
					GetFields(o, fields);
				}
				return new LinqQuery(iter, joins, filter, ordering, IsPassThrough(start.Stream.Fields, fields) ? null : fields.ToArray());
			}
			return iter.Collection;
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
			if (hasQuery) {
				var fields = new HashSet<DbExpression>(EqualityComparer<DbExpression>.Create((l, r) => l?.ToString() == r?.ToString(), expr => expr.ToString()!.GetHashCode()));
				foreach (var o in model.Outputs) {
					GetFields(o, fields);
				}
				return new LinqQuery(iter, joins, filter, ordering, IsPassThrough(start.Stream.Fields, fields) ? null : fields.ToArray());
			}
			return iter.Collection;
		}

		protected string GetInput(DbExpression[] args) => string.Join(", ", args.Select(GetInput));

		protected override string GetInput(DbExpression expr)
		{
			if (expr.IsLiteral) {
				return GetLiteralValue(expr);
			}
			var result = expr switch {
				MemberReferenceExpression mre => GetMreInput(mre),
				AliasedExpression ae => GetAliasedInput(ae),
				AggregateExpression ag => GetInput(ag.Args),
				CallExpression ce => base.GetInput(ce),
				BinaryExpression bin => $"{GetInput(bin.Left)} {bin.OpString} {GetInput(bin.Right)}",
				VariableReferenceExpression vr => "Program." + vr.ScriptVarName,
				LiteralExpression => expr.ToString()!,
				_ => throw new NotImplementedException()
			};
			return expr.Type?.Nullable == true ? $"(object?){result} ?? DBNull.Value" : result;
		}

		private static string GetLiteralValue(DbExpression expr) => expr is AliasedExpression ae ? ae.Expr.ToString()! : expr.ToString()!;

		protected static MemberReferenceExpression GetInputField(DbExpression expr) => expr switch {
			MemberReferenceExpression mre => mre,
			AliasedExpression ae => GetInputField(ae.Expr),
			AggregateExpression ag => GetInputField(ag.Args[0]),
			CountExpression => new(new("__agg__"), "__Count__"),
			_ => throw new NotImplementedException()
		};

		private string GetAliasedInput(AliasedExpression ae)
			=> ae.Expr is ReferenceExpression ? "__item." + ae.Alias : GetInput(ae.Expr);

		private string GetMreInput(MemberReferenceExpression mre)
		{
			var name = GetField(mre).Name;
			return new MemberReferenceExpression(new("__item"), name).ToString();
		}

	}
}
