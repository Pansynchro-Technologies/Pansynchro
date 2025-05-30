using Microsoft.SqlServer.TransactSql.ScriptDom;
using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;
using BinaryExpression = Pansynchro.PanSQL.Compiler.DataModels.BinaryExpression;
using BooleanExpression = Pansynchro.PanSQL.Compiler.DataModels.BooleanExpression;
using Identifier = Microsoft.SqlServer.TransactSql.ScriptDom.Identifier;
using TableReference = Pansynchro.PanSQL.Compiler.DataModels.TableReference;
using UnaryExpression = Pansynchro.PanSQL.Compiler.DataModels.UnaryExpression;
using UnaryExpressionType = Pansynchro.PanSQL.Compiler.DataModels.UnaryExpressionType;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	using IntegerLiteralExpression = DataModels.IntegerLiteralExpression;
	using ScriptBinaryExpression = Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression;
	using StringLiteralExpression = DataModels.StringLiteralExpression;

	internal class SqlAnalysis : VisitorCompileStep
	{
		public override void OnSqlStatement(SqlTransformStatement node)
		{
			var tt = node.TransactionType;
			if (tt.HasFlag(TransactionType.WithCte)) {
				foreach (var cte in ((SelectStatement)node.SqlNode).WithCtesAndXmlNamespaces.CommonTableExpressions) {
					var name = cte.ExpressionName.Value.ToPropertyName();
					var cteModel = BuildDataModel(cte, node);
					var tt2 = cteModel.Inputs.Any(i => i.Type == TableType.Stream) ? TransactionType.FromStream : TransactionType.PureMemory;
					if (cteModel.AggOutputs?.Length > 0) {
						tt2 |= TransactionType.Grouped;
					}
					var typedef = TypesHelper.BuildStreamDefFromDataModel(cteModel, name);
					node.Ctes.Add(new(name, BuildMemoryModel(cteModel, tt2), typedef));
				}
			}
			var model = BuildDataModel(node);
			var modelGen = tt.HasFlag(TransactionType.ToStream) ? BuildToStreamModel(model, tt) : BuildMemoryModel(model, tt);
			node.DataModel = modelGen;
			node.Indices = BuildIndexData(node);
		}

		private IndexData BuildIndexData(SqlTransformStatement node)
		{
			var indices = new List<IndexRecord>();
			var targets = node.DataModel.Model.Joins.SelectMany(j => j.TargetFields).DistinctBy(f => f.ToString()).ToArray();
			var tables = targets.Select(t => t.Parent.Name).Distinct().Select(n => _file.Vars[n]).ToDictionary(v => v.Name);
			foreach (var tf in targets) {
				var table = ((VarDeclaration)tables[tf.Parent.ToString()].Declaration).Stream!;
				var unique = table.Identity.Length == 1 && tf.Name == table.Identity[0];
				var indexTf = new MemberReferenceExpression(new(table.Name.ToString()), tf.Name);
				indices.Add(new(indexTf.ToIndexName(), unique));
			}
			var lookups = tables.ToDictionary(kvp => kvp.Key, kvp => ((VarDeclaration)kvp.Value.Declaration).Stream.Name.ToString());
			return new IndexData(indices.ToDictionary(i => i.Name), lookups);
		}

		private static SqlModel BuildToStreamModel(DataModel model, TransactionType tt)
			=> (tt.HasFlag(TransactionType.FromStream), tt.HasFlag(TransactionType.Grouped)) switch {
				(true, true) => new AggregateStreamModel(model),
				(true, false) => new IterateStreamModel(model),
				(false, true) => new AggregateStreamGeneratorModel(model),
				(false, false) => new StreamGeneratorModel(model),
			};

		private static SqlModel BuildMemoryModel(DataModel model, TransactionType tt)
			=> (tt.HasFlag(TransactionType.FromStream), tt.HasFlag(TransactionType.Grouped)) switch {
				(true, true) => new StreamToAggregateMemoryModel(model),
				(true, false) => throw new NotImplementedException(),
				(false, true) => throw new NotImplementedException(),
				(false, false) => new PureMemoryModel(model),
			};

		private const int MAX_AGGS = 7;

		private DataModel BuildDataModel(TSqlFragment obj, SqlTransformStatement node)
		{
			var builder = new DataModelBuilder(_file);
			try { 
				obj.Accept(builder);
			} catch (Exception e) {
				throw new CompilerError(e.Message, node);
			}
			if (builder.Model.AggOutputs.Length > MAX_AGGS) {
				throw new CompilerError($"PanSQL only supports a maximum of {MAX_AGGS} aggregate functions in a single query.", node);
			}
			return builder.Model;
		}

		private DataModel BuildDataModel(SqlTransformStatement node) => BuildDataModel(node.SqlNode, node);

		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies() => [Dependency<DefineVars>()];

		private class DataModelBuilder : TSqlFragmentVisitor
		{
			protected PanSqlFile _file;
			private readonly List<TableReference> _tables = [];
			private readonly List<JoinSpec> _joins = [];
			private readonly List<DbExpression> _selects = [];
			private readonly HashSet<string> _scriptVars = [];
			private DbExpression? _where;
			private MemberReferenceExpression[]? _groupKey;
			private BooleanExpression? _having;
			private readonly Dictionary<string, Variable> _aliases = [];
			private readonly DataExpressionVisitor _expressionVisitor;
			private readonly List<AggregateExpression> _aggs = [];
			private OrderingExpression[]? _orderBy;
			private string? _outputTable;

			public DataModel Model => new([.. _tables], [.. _joins], _where, _groupKey, [.. _selects], [.. _scriptVars], [.. _aggs], _having, _orderBy, _outputTable);

			public DataModelBuilder(PanSqlFile file)
			{
				_file = file;
				_expressionVisitor = new(_file, _aliases, _scriptVars);
			}


			public override void ExplicitVisit(CommonTableExpression cte)
			{
				cte.QueryExpression.Accept(this);
				_outputTable = cte.ExpressionName.Value.ToPropertyName();
			}

			public override void ExplicitVisit(SelectStatement statement)
			{
				statement.QueryExpression.Accept(this);
				if (statement.Into != null) {
					throw new Exception("Should not see an INTO here");
				}
				_expressionVisitor.Finish();
			}

			public override void ExplicitVisit(QuerySpecification codeObject)
			{
				foreach (var table in codeObject.FromClause.TableReferences) { 
					table.Accept(this);
				}
				_expressionVisitor.Tables = [.. _tables];
				if (codeObject.ForClause != null) {
					throw new CompilerError("FOR clauses are not supported in SQL scripts", _file);
				}
				codeObject.WhereClause?.Accept(this);
				codeObject.GroupByClause?.Accept(this);
				codeObject.HavingClause?.Accept(this);
				codeObject.WindowClause?.Accept(this);
				codeObject.OrderByClause?.Accept(this);
				foreach (var se in codeObject.SelectElements) {
					var expr = _expressionVisitor.VisitValue(se);
					_selects.Add(expr);
					_aggs.AddRange(FindAggregateExpressions(expr));
				}
				codeObject.TopRowFilter?.Accept(this);
			}

			public override void ExplicitVisit(QualifiedJoin codeObject)
			{
				codeObject.FirstTableReference.Accept(this);
				codeObject.SecondTableReference.Accept(this);
				_expressionVisitor.Reset();
				_expressionVisitor.Tables = [.. _tables];
				var expr = (BooleanExpression)_expressionVisitor.VisitValue(codeObject.SearchCondition);
				var op = codeObject.QualifiedJoinType switch {
					QualifiedJoinType.Inner => JoinType.Inner,
					QualifiedJoinType.LeftOuter => JoinType.Left,
					_ => throw new NotImplementedException()
				};
				if (codeObject.SecondTableReference is NamedTableReference tr) {
					var table = new TableReference(_file.Vars[tr.SchemaObject.BaseIdentifier.Value]);
					_joins.Add(new JoinSpec(op, table, _expressionVisitor.TargetFields, expr));
				} else {
					throw new NotImplementedException();
				}
			}

			public override void ExplicitVisit(NamedTableReference codeObject)
			{
				var vbl = _file.Vars[codeObject.SchemaObject.BaseIdentifier.Value];
				if (codeObject.Alias != null) {
					_aliases.Add(codeObject.Alias.Value, vbl);
				}
				_tables.Add(new(vbl));
			}

			private static IEnumerable<AggregateExpression> FindAggregateExpressions(DbExpression expr)
			{
				switch (expr) {
					case AggregateExpression agg:
						foreach (var result in agg.Args.SelectMany(FindAggregateExpressions)) {
							yield return result;
						}
						yield return agg;
						break;
					case AliasedExpression ae:
						foreach (var result in FindAggregateExpressions(ae.Expr)) {
							yield return result;
						}
						break;
					case BinaryExpression bin:
						foreach (var result in FindAggregateExpressions(bin.Left)) {
							yield return result;
						}
						foreach (var result in FindAggregateExpressions(bin.Right)) {
							yield return result;
						}
						break;
					case BooleanExpression bo:
						foreach (var result in FindAggregateExpressions(bo.Left)) {
							yield return result;
						}
						foreach (var result in FindAggregateExpressions(bo.Right)) {
							yield return result;
						}
						break;
					case CallExpression call:
						foreach (var result in call.Args.SelectMany(FindAggregateExpressions)) {
							yield return result;
						}
						break;
					case CollectionExpression coll:
						foreach (var result in coll.Values.SelectMany(FindAggregateExpressions)) {
							yield return result;
						}
						break;
					case ContainsExpression cont:
						foreach (var result in FindAggregateExpressions(cont.Collection)) {
							yield return result;
						}
						foreach (var result in FindAggregateExpressions(cont.Value)) {
							yield return result;
						}
						break;
				}
			}

			public override void ExplicitVisit(WhereClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_where = _expressionVisitor.VisitValue(codeObject.SearchCondition);
			}

			public override void ExplicitVisit(GroupByClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_groupKey = codeObject.GroupingSpecifications.Select(x => (MemberReferenceExpression)_expressionVisitor.VisitValue(x)).ToArray();
			}

			public override void ExplicitVisit(HavingClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_having = (BooleanExpression)_expressionVisitor.VisitValue(codeObject.SearchCondition);
			}

			public override void ExplicitVisit(OrderByClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_orderBy = codeObject.OrderByElements.Select(i => (OrderingExpression)_expressionVisitor.VisitValue(i)).ToArray();
			}
		}

		internal static readonly Dictionary<string, int> AGGREGATE_FUNCTIONS_SUPPORTED = new()
			{ {"Avg", 1}, {"Sum", 1}, {"Count", 1}, {"Min", 1}, {"Max", 1}, {"String_agg", 2} };

		private class DataExpressionVisitor(PanSqlFile file, Dictionary<string, Variable> aliases, HashSet<string> scriptVars) : TSqlFragmentVisitor
		{
			private readonly PanSqlFile _file = file;
			private readonly Dictionary<string, Variable> _aliases = aliases;
			private readonly List<MemberReferenceExpression> _targetFields = [];
			private readonly HashSet<string> _scriptVars = scriptVars;

			private readonly Stack<DbExpression> _stack = [];

			internal TableReference[] Tables { get; set; } = [];

			public DbExpression VisitValue(TSqlFragment value)
			{
#if DEBUG
				try {
					value.Accept(this);
					return _stack.Pop();
				} finally {
					Finish();
				}
#else
				value.Accept(this);
				return _stack.Pop();
#endif
			}

			public void Finish()
			{
				if (_stack.Count > 0) throw new NotImplementedException("Unimplemented SQL operation detected");
			}

			private static AggregateExpression CheckAggExpression(string name, DbExpression[]? args)
			{
				name = name.ToLower().ToPropertyName();
				if (!AGGREGATE_FUNCTIONS_SUPPORTED.TryGetValue(name, out var argCount)) {
					throw new Exception($"The '{name}' function is not supported by PanSQL at this time");
				}
				if (name == "Count" && args == null) {
					return new AggregateExpression(name, new CountExpression());
				}
				if (args?.Length != argCount) {
					throw new Exception($"The '{name}' function requires {argCount} argument(s).");
				}
				return new AggregateExpression(name, args);
			}

			public override void ExplicitVisit(BooleanComparisonExpression codeObject)
			{
				var l = VisitValue(codeObject.FirstExpression);
				var r = VisitValue(codeObject.SecondExpression);
				var op = codeObject.ComparisonType switch {
					BooleanComparisonType.Equals => BoolExpressionType.Equals,
					BooleanComparisonType.LessThan => BoolExpressionType.LessThan,
					BooleanComparisonType.NotEqualToBrackets or BooleanComparisonType.NotEqualToExclamation => BoolExpressionType.NotEquals,
					BooleanComparisonType.GreaterThan => BoolExpressionType.GreaterThan,
					BooleanComparisonType.GreaterThanOrEqualTo => BoolExpressionType.GreaterThanOrEqual,
					BooleanComparisonType.LessThanOrEqualTo => BoolExpressionType.LessThanOrEqual,
					_ => throw new NotImplementedException($"Unsupported operator type: {codeObject.ComparisonType}"),
				};
				if (r is MemberReferenceExpression mr) {
					_targetFields.Add(mr);
				}
				_stack.Push(new BooleanExpression(op, l, r));
			}

			public override void ExplicitVisit(StringLiteral codeObject) => _stack.Push(new StringLiteralExpression(codeObject.Value));

			public override void ExplicitVisit(IntegerLiteral codeObject) => _stack.Push(new IntegerLiteralExpression(int.Parse(codeObject.Value)));

			public override void ExplicitVisit(NumericLiteral codeObject) => _stack.Push(new FloatLiteralExpression(double.Parse(codeObject.Value)));

			public override void ExplicitVisit(NullLiteral codeObject) => _stack.Push(new NullLiteralExpression());

			public override void ExplicitVisit(MultiPartIdentifier codeObject)
			{
				var names = codeObject.Identifiers;
				if (names.Count is not (1 or 2)) {
					throw new Exception($"The name '{string.Join(".", names.Select(i => i.Value))}' is not a valid reference");
				}
				ReferenceExpression? result = null;
				foreach (var name in names) {
					result = result == null ? new ReferenceExpression(LookupAliasedName(name.Value)) : new MemberReferenceExpression(result, name.Value);
				}
				_stack.Push(result!);
			}

			public override void ExplicitVisit(Identifier codeObject)
			{
				var fieldName = codeObject.Value;
				var match = TypesHelper.LookupField(Tables, fieldName);
				_stack.Push(new MemberReferenceExpression(new(match.Key), match.Value.Name));
			}

			public override void ExplicitVisit(ColumnReferenceExpression codeObject)
			{
				var id = codeObject.MultiPartIdentifier;
				if (id == null) {
					if (codeObject.ColumnType != ColumnType.Wildcard) {
						throw new Exception("Unknown column reference type");
					}
					_stack.Push(new CountExpression());
				} else if (id.Identifiers.Count == 1) {
					id.Identifiers[0].Accept(this);
				} else {
					id.Accept(this);
				}
			}

			public override void ExplicitVisit(SelectScalarExpression codeObject)
			{
				var result = VisitValue(codeObject.Expression);
				_stack.Push(codeObject.ColumnName == null ? result : new AliasedExpression(result, codeObject.ColumnName.Value));
			}

			public override void ExplicitVisit(SelectStarExpression node)
			{
				ReferenceExpression? table = node.Qualifier == null ? null : (ReferenceExpression)VisitValue(node.Qualifier);
				_stack.Push(new StarExpression(table));
			}

			public override void ExplicitVisit(VariableReference codeObject)
			{
				var varName = codeObject.Name[1..];
				if (!(_file.Vars.TryGetValue(varName, out var value) && value.Declaration is ScriptVarDeclarationStatement)) {
					throw new Exception($"No script variable named '{codeObject.Name}' is declared in this script.");
				}
				var refName = '_' + varName;
				_scriptVars.Add(refName);
				_stack.Push(new VariableReferenceExpression(refName));
			}

			public override void ExplicitVisit(CastCall codeObject)
			{
				var value = VisitValue(codeObject.Parameter);
				var typeName = codeObject.DataType.Name.BaseIdentifier.Value;
				if (!Enum.TryParse<TypeTag>(typeName, out var type)) {
					throw new Exception("Unknown data type: " + typeName);
				}
				_stack.Push(new CastExpression(value, new BasicField(type, false, null, false)));
			}

			private static Dictionary<BinaryExpressionType, BinExpressionType> BIN_OPS = new()
			{   { BinaryExpressionType.Add, BinExpressionType.Add }, { BinaryExpressionType.Subtract, BinExpressionType.Subtract },
				{ BinaryExpressionType.Multiply, BinExpressionType.Multiply }, { BinaryExpressionType.Divide, BinExpressionType.Divide },
				{ BinaryExpressionType.Modulo, BinExpressionType.Mod }, { BinaryExpressionType.BitwiseAnd, BinExpressionType.BitAnd },
				{ BinaryExpressionType.BitwiseOr, BinExpressionType.BitOr }, { BinaryExpressionType.BitwiseXor, BinExpressionType.BitXor },
			};

			public override void ExplicitVisit(ScriptBinaryExpression codeObject)
			{
				var l = VisitValue(codeObject.FirstExpression);
				var r = VisitValue(codeObject.SecondExpression);
				if (!BIN_OPS.TryGetValue(codeObject.BinaryExpressionType, out var type)) {
					throw new Exception($"SQL operator '{codeObject.BinaryExpressionType}' is not currently supported by PanSQL");
				}
				_stack.Push(new BinaryExpression(type, l, r));
			}

			public override void ExplicitVisit(ParameterlessCall codeObject)
			{
				var name = codeObject.ParameterlessCallType.ToString();
				_stack.Push(new CallExpression(new ReferenceExpression(name), []));
			}

			public override void ExplicitVisit(FunctionCall codeObject)
			{
				var args = codeObject.Parameters?.Select(VisitValue).ToArray() ?? [];
				if (AGGREGATE_FUNCTIONS_SUPPORTED.ContainsKey(codeObject.FunctionName.Value.ToLower().ToPropertyName())) {
					_stack.Push(CheckAggExpression(codeObject.FunctionName.Value, args));
					return;
				}
				var name = codeObject.FunctionName.Value;
				if (name.Equals("trim", StringComparison.InvariantCultureIgnoreCase)) {
					var type = new IntegerLiteralExpression(codeObject.TrimOptions?.Value.ToLowerInvariant() switch {
						"leading" => 1,
						"trailing" => 2,
						_ => 0
					});
					args = args.Length == 1 ? [args[0], new NullLiteralExpression(), type] : [..args, type];
				}
				_stack.Push(new CallExpression(new ReferenceExpression(name), args));
			}

			public override void ExplicitVisit(InPredicate codeObject)
			{
				var value = VisitValue(codeObject.Expression);
				DbExpression coll;
				coll = codeObject.Subquery != null 
					? VisitValue(codeObject.Subquery)
					: new CollectionExpression(codeObject.Values.Select(VisitValue).ToArray());
				DbExpression result = new ContainsExpression(coll, value);
				if (codeObject.NotDefined) {
					result = new UnaryExpression(UnaryExpressionType.Not, result);
				}
				_stack.Push(result);
			}

			public override void ExplicitVisit(LikePredicate codeObject)
			{
				var l = VisitValue(codeObject.FirstExpression);
				var r = VisitValue(codeObject.SecondExpression);
				if (codeObject.EscapeExpression != null) {
					throw new NotImplementedException();
				}
				DbExpression result = new LikeExpression(l, r);
				if (codeObject.NotDefined) {
					result = new UnaryExpression(UnaryExpressionType.Not, result);
				}
				_stack.Push(result);

			}

			public override void ExplicitVisit(BooleanBinaryExpression codeObject)
			{
				var l = VisitValue(codeObject.FirstExpression);
				var r = VisitValue(codeObject.SecondExpression);
				var type = (BinExpressionType)codeObject.BinaryExpressionType;
				_stack.Push(new BinaryExpression(type, l, r));
			}

			public override void ExplicitVisit(BooleanIsNullExpression codeObject)
			{
				var value = VisitValue(codeObject.Expression);
				DbExpression result = new IsNullExpression(value);
				if (codeObject.IsNot) {
					result = new UnaryExpression(UnaryExpressionType.Not, result);
				}
				_stack.Push(result);
			}

			public override void ExplicitVisit(ExpressionGroupingSpecification codeObject) => codeObject.Expression.Accept(this);

			public override void ExplicitVisit(ExpressionWithSortOrder codeObject)
			{
				var expr = VisitValue(codeObject.Expression);
				_stack.Push(new OrderingExpression(expr, codeObject.SortOrder == SortOrder.Descending));
			}

			public override void ExplicitVisit(SearchedWhenClause codeObject)
			{
				var when = VisitValue(codeObject.WhenExpression);
				var result = VisitValue(codeObject.ThenExpression);
				_stack.Push(new IfThenExpression(when, result));
			}

			public override void ExplicitVisit(SearchedCaseExpression codeObject)
			{
				var when = codeObject.WhenClauses.Select(VisitValue).Cast<IfThenExpression>().ToArray();
				var elseVal = codeObject.ElseExpression == null ? null : VisitValue(codeObject.ElseExpression);
				_stack.Push(new IfExpression(when, elseVal));
			}

			internal void Reset()
			{
				_targetFields.Clear();
			}

			internal MemberReferenceExpression[] TargetFields => _targetFields.DistinctBy(x => x.ToString()).ToArray();

			private string LookupAliasedName(string name) => _aliases.TryGetValue(name, out var value) ? value.Name : name;
		}
	}
}
