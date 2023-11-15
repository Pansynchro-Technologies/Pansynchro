using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal class SqlAnalysis : VisitorCompileStep
	{
		public override void OnSqlStatement(SqlTransformStatement node)
		{
			var tt = node.TransactionType;
			var model = tt.HasFlag(TransactionType.ToStream) ? BuildStreamedModel(node, tt) : BuildMemoryModel(node, tt);
			node.DataModel = model;
			node.Indices = BuildIndexData(node);
		}

		private IndexData BuildIndexData(SqlTransformStatement node)
		{
			var indices = new List<IndexRecord>();
			var targets = node.DataModel.Model.Joins.SelectMany(j => j.TargetFields).DistinctBy(f => f.ToString()).ToArray();
			var tables = targets.Select(t => t.Parent.Name).Distinct().Select(n => _file.Vars[n]).ToDictionary(v => v.Name);
			foreach (var tf in targets) {
				var table = ((VarDeclaration)tables[tf.Parent.ToString()].Declaration).Stream;
				var unique = table.Identity.Length == 1 && tf.Name == table.Identity[0];
				var indexTf = new MemberReferenceExpression(new(table.Name.ToString()), tf.Name);
				indices.Add(new(indexTf.ToIndexName(), unique));
			}
			var lookups = tables.ToDictionary(kvp => kvp.Key, kvp => ((VarDeclaration)kvp.Value.Declaration).Stream.Name.ToString());
			return new IndexData(indices.ToDictionary(i => i.Name), lookups);
		}

		private SqlModel BuildStreamedModel(SqlTransformStatement node, TransactionType tt) 
			=> tt.HasFlag(TransactionType.Streamed) ? BuildTransformerModel(node, tt) : BuildStreamGeneratorModel(node, tt);

		private SqlModel BuildTransformerModel(SqlTransformStatement node, TransactionType tt)
		{
			var model = BuildDataModel(node);
			return tt.HasFlag(TransactionType.Grouped) ? new AggregateStreamModel(model) : new IterateStreamModel(model);
		}

		private MemorySqlModel BuildStreamGeneratorModel(SqlTransformStatement node, TransactionType tt)
		{
			var model = BuildDataModel(node);
			return tt.HasFlag(TransactionType.Grouped) ? new AggregateStreamGeneratorModel(model) : new StreamGeneratorModel(model); 
		}

		private SqlModel BuildMemoryModel(SqlTransformStatement node, TransactionType tt)
		{
			throw new NotImplementedException();
		}

		private const int MAX_AGGS = 7;

		private DataModel BuildDataModel(SqlTransformStatement node)
		{
			var builder = new DataModelBuilder(_file);
			try { 
				node.SqlNode.Accept(builder);
			} catch (Exception e) {
				throw new CompilerError(e.Message, node);
			}
			if (builder.Model.AggOutputs.Length > MAX_AGGS) {
				throw new CompilerError($"PanSQL only supports a maximum of {MAX_AGGS} aggregate functions in a single query.", node);
			}
			return builder.Model;
		}

		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies() => [Dependency<DefineVars>()];

		private class DataModelBuilder : AbstractSqlCodeVisitor
		{
			protected PanSqlFile _file;
			private readonly List<TableReference> _tables = [];
			private readonly List<JoinSpec> _joins = [];
			private readonly List<DbExpression> _selects = [];
			private DbExpression? _where;
			private MemberReferenceExpression[]? _groupKey;
			private BooleanExpression? _having;
			private readonly Dictionary<string, Variable> _aliases = [];
			private readonly DataExpressionVisitor _expressionVisitor;
			private readonly List<AggregateExpression> _aggs = [];
			private OrderingExpression[]? _orderBy;

			public DataModel Model => new([.. _tables], [.. _joins], _where, _groupKey, [.. _selects], [.. _aggs], _having, _orderBy);

			public DataModelBuilder(PanSqlFile file)
			{
				_file = file;
				_expressionVisitor = new(_file, _aliases);
			}

			public override void Visit(SqlSelectStatement statement)
			{
				statement.SelectSpecification.QueryExpression.Accept(this);

				//apparently SelectSpecification.QueryExpression does not contain the ORDER BY clause, so we have to do it here
				statement.SelectSpecification.OrderByClause?.Accept(this);
			}

			public override void Visit(SqlQuerySpecification codeObject)
			{
				foreach (var table in codeObject.FromClause.TableExpressions) { 
					table.Accept(this);
				}
				if (codeObject.IntoClause != null) {
					throw new Exception("Should not see an INTO here");
				}
				if (codeObject.ForClause != null) {
					throw new CompilerError("FOR clauses are not supported in SQL scripts", _file);
				}
				codeObject.WhereClause?.Accept(this);
				codeObject.GroupByClause?.Accept(this);
				codeObject.HavingClause?.Accept(this);
				codeObject.WindowClause?.Accept(this);
				codeObject.OrderByClause?.Accept(this);
				codeObject.SelectClause.Accept(this);
			}

			public override void Visit(SqlQualifiedJoinTableExpression codeObject)
			{
				codeObject.Left.Accept(this);
				codeObject.Right.Accept(this);
				_expressionVisitor.Reset();
				_expressionVisitor.Tables = [.. _tables];
				var expr = (BooleanExpression)codeObject.OnClause.Expression.Accept(_expressionVisitor);
				var op = codeObject.JoinOperator switch {
					SqlJoinOperatorType.InnerJoin => JoinType.Inner,
					SqlJoinOperatorType.LeftOuterJoin => JoinType.Left,
					_ => throw new NotImplementedException()
				};
				if (codeObject.Right is SqlTableRefExpression tr) {
					var table = new TableReference(_file.Vars[tr.ObjectIdentifier.ObjectName.Value]);
					_joins.Add(new JoinSpec(op, table, _expressionVisitor.TargetFields, expr));
				} else {
					throw new NotImplementedException();
				}
			}

			public override void Visit(SqlTableRefExpression codeObject)
			{
				var vbl = _file.Vars[codeObject.ObjectIdentifier.ObjectName.Value];
				if (codeObject.Alias != null) {
					_aliases.Add(codeObject.Alias.Value, vbl);
				}
				_tables.Add(new(vbl));
			}

			public override void Visit(SqlSelectClause codeObject)
			{
				codeObject.Top?.Accept(this);
				_expressionVisitor.Tables = [.. _tables];
				foreach (var se in codeObject.SelectExpressions) {
					var expr = se.Accept(_expressionVisitor);
					_selects.Add(expr);
					if (expr is AggregateExpression agg) {
						_aggs.Add(agg);
					} else if (expr is AliasedExpression { Expr : AggregateExpression agg2}) {
						_aggs.Add(agg2);
					}
				}
			}

			public override void Visit(SqlWhereClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_where = codeObject.Expression.Accept(_expressionVisitor);
			}

			public override void Visit(SqlGroupByClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_groupKey = codeObject.Items.Select(x => (MemberReferenceExpression)x.Accept(_expressionVisitor)).ToArray();
			}

			public override void Visit(SqlHavingClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_having = (BooleanExpression)codeObject.Expression.Accept(_expressionVisitor);
			}

			public override void Visit(SqlOrderByClause codeObject)
			{
				_expressionVisitor.Tables = [.. _tables];
				_orderBy = codeObject.Items.Select(i => (OrderingExpression)i.Accept(_expressionVisitor)).ToArray();
			}
		}

		private class DataExpressionVisitor(PanSqlFile file, Dictionary<string, Variable> aliases) : AbstractSqlCodeVisitor<DbExpression>
		{
			private readonly PanSqlFile _file = file;
			private readonly Dictionary<string, Variable> _aliases = aliases;
			private readonly List<MemberReferenceExpression> _targetFields = [];

			internal TableReference[] Tables { get; set; } = [];

			private static readonly string[] AGGREGATE_FUNCTIONS_SUPPORTED = ["Avg", "Sum", "Count", "Min", "Max"];

			public override DbExpression Visit(SqlAggregateFunctionCallExpression codeObject)
			{
				var name = codeObject.FunctionName.ToLower().ToPropertyName();
				if (!AGGREGATE_FUNCTIONS_SUPPORTED.Contains(name)) {
					throw new Exception($"The '{name}' function is not supported by PanSQL at this time");
				}
				if (name == "Count" && codeObject.Arguments == null) {
					return new AggregateExpression(name, new CountExpression());
				}
				if (codeObject.Arguments.Count != 1) {
					throw new Exception($"The '{name}' function only accepts one argument.");
				}
				var arg = codeObject.Arguments[0].Accept(this);
				return new AggregateExpression(name, arg);
			}

			public override DbExpression Visit(SqlComparisonBooleanExpression codeObject)
			{
				var l = codeObject.Left.Accept(this);
				var r = codeObject.Right.Accept(this);
				var op = codeObject.ComparisonOperator switch {
					SqlComparisonBooleanExpressionType.Equals => BoolExpressionType.Equals,
					SqlComparisonBooleanExpressionType.LessThan => BoolExpressionType.LessThan,
					SqlComparisonBooleanExpressionType.NotEqual => BoolExpressionType.NotEquals,
					SqlComparisonBooleanExpressionType.GreaterThan => BoolExpressionType.GreaterThan,
					SqlComparisonBooleanExpressionType.GreaterThanOrEqual => BoolExpressionType.GreaterThanOrEqual,
					SqlComparisonBooleanExpressionType.LessThanOrEqual => BoolExpressionType.LessThanOrEqual,
					_ => throw new NotImplementedException($"Unsupported operator type: {codeObject.ComparisonOperator}"),
				};
				if (r is MemberReferenceExpression mr) {
					_targetFields.Add(mr);
				}
				return new BooleanExpression(op, l, r);
			}

			public override DbExpression Visit(SqlScalarRefExpression codeObject) => codeObject.MultipartIdentifier.Accept(this);

			public override DbExpression Visit(SqlLiteralExpression codeObject) => codeObject.Type switch {
				LiteralValueType.Integer => new IntegerLiteralExpression(int.Parse(codeObject.Value)),
				LiteralValueType.String => new StringLiteralExpression(codeObject.Value),
				LiteralValueType.Null => new NullLiteralExpression(),
				_ => throw new NotImplementedException(),
			};

			public override DbExpression Visit(SqlObjectIdentifier codeObject)
			{
				var names = codeObject.Sql.Split('.');
				if (!(names.Length is 1 or 2)) {
					throw new Exception($"The name '{codeObject.Sql}' is not a valid reference");
				}
				ReferenceExpression? result = null;
				foreach ( var name in names ) {
					result = result == null ? new ReferenceExpression(LookupAliasedName(name)) : new MemberReferenceExpression(result, name);
				}
				return result!;
			}

			public override DbExpression Visit(SqlIdentifier codeObject)
			{
				var fieldName = codeObject.Value;
				var matches = Tables
					.SelectMany(t => t.Stream.Fields, (tr, f) => KeyValuePair.Create(tr.Name, f))
					.Where(f => f.Value.Name.Equals(fieldName, StringComparison.InvariantCultureIgnoreCase))
					.ToArray();
				return matches.Length switch {
					0 => throw new Exception($"No field named {fieldName} is available"),
					1 => new MemberReferenceExpression(new(matches[0].Key), matches[0].Value.Name),
					_ => throw new Exception($"Ambiguous field name: {fieldName}. {matches.Length} different tables contain a field by that name.  Make sure to qualify the name.")
				};
			}

			public override DbExpression Visit(SqlSelectScalarExpression codeObject)
			{
				var result = codeObject.Expression.Accept(this);
				return codeObject.Alias == null ? result : new AliasedExpression(result, codeObject.Alias.Value);
			}

			public override DbExpression Visit(SqlScalarExpression codeObject) => codeObject switch
			{
				SqlColumnRefExpression cr => Visit(cr.ColumnName),
				_ => throw new NotImplementedException()
			};

			public override DbExpression Visit(SqlSimpleGroupByItem codeObject) => Visit(codeObject.Expression);

			public override DbExpression Visit(SqlOrderByItem codeObject)
			{
				var expr = codeObject.Expression.Accept(this);
				return new OrderingExpression(expr, codeObject.SortOrder == SqlSortOrder.Descending);
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
