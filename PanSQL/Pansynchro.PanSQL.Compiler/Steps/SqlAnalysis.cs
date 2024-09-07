using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;

using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	using StringLiteralExpression = DataModels.StringLiteralExpression;
	using IntegerLiteralExpression = DataModels.IntegerLiteralExpression;

	internal class SqlAnalysis : VisitorCompileStep
	{
		public override void OnSqlStatement(SqlTransformStatement node)
		{
			var tt = node.TransactionType;
			if (tt.HasFlag(TransactionType.WithCte)) {
				foreach (var cte in ((SqlSelectStatement)node.SqlNode).QueryWithClause.CommonTableExpressions) {
					var name = cte.Name.Value.ToPropertyName();
					var cteModel = BuildDataModel(cte, node);
					var tt2 = cteModel.Inputs.Any(i => i.Type == TableType.Stream) ? TransactionType.Streamed : TransactionType.PureMemory;
					if (cteModel.AggOutputs?.Length > 0) {
						tt2 |= TransactionType.Grouped;
					}
					var typedef = TypesHelper.BuildStreamDefFromDataModel(cteModel, name);
					node.Ctes.Add(new(name, BuildMemoryModel(cteModel, tt2), typedef));
				}
			}
			var model = BuildDataModel(node);
			var modelGen = tt.HasFlag(TransactionType.ToStream) ? BuildStreamedModel(model, tt) : BuildMemoryModel(model, tt);
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

		private static SqlModel BuildStreamedModel(DataModel model, TransactionType tt) 
			=> tt.HasFlag(TransactionType.Streamed) ? BuildTransformerModel(model, tt) : BuildStreamGeneratorModel(model, tt);

		private static SqlModel BuildTransformerModel(DataModel model, TransactionType tt)
			=> tt.HasFlag(TransactionType.Grouped) ? new AggregateStreamModel(model) : new IterateStreamModel(model);

		private static MemorySqlModel BuildStreamGeneratorModel(DataModel model, TransactionType tt) 
			=> tt.HasFlag(TransactionType.Grouped) ? new AggregateStreamGeneratorModel(model) : new StreamGeneratorModel(model);

		private static SqlModel BuildMemoryModel(DataModel model, TransactionType tt)
			=> tt.HasFlag(TransactionType.Streamed) ? BuildStreamedToMemoryModel(model, tt) : throw new NotImplementedException();

		private static SqlModel BuildStreamedToMemoryModel(DataModel model, TransactionType tt)
			=> tt.HasFlag(TransactionType.Grouped) ? new StreamToAggregateMemoryModel(model) : throw new NotImplementedException();

		private const int MAX_AGGS = 7;

		private DataModel BuildDataModel(SqlCodeObject obj, SqlTransformStatement node)
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

		private class DataModelBuilder : AbstractSqlCodeVisitor
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

			public override void Visit(SqlCommonTableExpression cte)
			{
				cte.QueryExpression.Accept(this);
				_outputTable = cte.Name.Value.ToPropertyName();
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
					_aggs.AddRange(FindAggregateExpressions(expr));
				}
			}

			private IEnumerable<AggregateExpression> FindAggregateExpressions(DbExpression expr)
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

		private class DataExpressionVisitor(PanSqlFile file, Dictionary<string, Variable> aliases, HashSet<string> scriptVars) : AbstractSqlCodeVisitor<DbExpression>
		{
			private readonly PanSqlFile _file = file;
			private readonly Dictionary<string, Variable> _aliases = aliases;
			private readonly List<MemberReferenceExpression> _targetFields = [];
			private readonly HashSet<string> _scriptVars = scriptVars;

			internal TableReference[] Tables { get; set; } = [];

			private static readonly Dictionary<string, int> AGGREGATE_FUNCTIONS_SUPPORTED = new()
			{ {"Avg", 1}, {"Sum", 1}, {"Count", 1}, {"Min", 1}, {"Max", 1}, {"String_agg", 2} };

			public override DbExpression Visit(SqlAggregateFunctionCallExpression codeObject)
			{
				var name = codeObject.FunctionName;
				var args = codeObject.Arguments?.Select(a => a.Accept(this)).ToArray();
				return CheckAggExpression(name, args);
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
				var match = TypesHelper.LookupField(Tables, fieldName);
				return new MemberReferenceExpression(new(match.Key), match.Value.Name);
			}

			public override DbExpression Visit(SqlColumnRefExpression codeObject) => Visit(codeObject.ColumnName);
			
			public override DbExpression Visit(SqlSelectScalarExpression codeObject)
			{
				var result = codeObject.Expression.Accept(this);
				return codeObject.Alias == null ? result : new AliasedExpression(result, codeObject.Alias.Value);
			}

			public override DbExpression Visit(SqlScalarVariableRefExpression codeObject)
			{
				var varName = codeObject.VariableName[1..];
				if (!(_file.Vars.TryGetValue(varName, out var value) && value.Declaration is ScriptVarDeclarationStatement)) {
					throw new Exception($"No script variable named '{codeObject.VariableName}' is declared in this script.");
				}
				var refName = '_' + varName;
				_scriptVars.Add(refName);
				return new VariableReferenceExpression(refName);
			}

			public override DbExpression Visit(SqlScalarExpression codeObject) => codeObject switch {
				SqlColumnRefExpression cr => Visit(cr.ColumnName),
				_ => throw new NotImplementedException()
			};

			private static Dictionary<SqlBinaryScalarOperatorType, BinExpressionType> BIN_OPS = new()
			{   { SqlBinaryScalarOperatorType.Add, BinExpressionType.Add }, { SqlBinaryScalarOperatorType.Subtract, BinExpressionType.Subtract },
				{ SqlBinaryScalarOperatorType.Multiply, BinExpressionType.Multiply }, { SqlBinaryScalarOperatorType.Divide, BinExpressionType.Divide },
				{ SqlBinaryScalarOperatorType.Modulus, BinExpressionType.Mod }, { SqlBinaryScalarOperatorType.BitwiseAnd, BinExpressionType.BitAnd },
				{ SqlBinaryScalarOperatorType.BitwiseOr, BinExpressionType.BitOr }, { SqlBinaryScalarOperatorType.BitwiseXor, BinExpressionType.BitXor },
			};

			public override DbExpression Visit(SqlBinaryScalarExpression codeObject)
			{
				var l = codeObject.Left.Accept(this);
				var r = codeObject.Right.Accept(this);
				if (!BIN_OPS.TryGetValue(codeObject.Operator, out var type)) {
					throw new Exception($"SQL operator '{codeObject.Operator}' is not currently supported by PanSQL");
				}
				return new BinaryExpression(type, l, r);
			}

			public override DbExpression Visit(SqlBinaryBooleanExpression codeObject)
			{
				var l = codeObject.Left.Accept(this);
				var r = codeObject.Right.Accept(this);
				var type = (BinExpressionType)codeObject.Operator;
				return new BinaryExpression(type, l, r);
			}

			public override DbExpression Visit(SqlInBooleanExpression codeObject)
			{
				var value = codeObject.InExpression.Accept(this);
				var coll = codeObject.ComparisonValue.Accept(this);
				return new ContainsExpression(coll, value);
			}

			public override DbExpression Visit(SqlInBooleanExpressionCollectionValue codeObject)
			{
				var values = codeObject.Values.Select(v => v.Accept(this)).ToArray();
				return new CollectionExpression(values);
			}

			public override DbExpression Visit(SqlBuiltinScalarFunctionCallExpression codeObject)
			{
				var args = codeObject.Arguments?.Select(a => a.Accept(this)).ToArray() ?? Array.Empty<DbExpression>();
				if (AGGREGATE_FUNCTIONS_SUPPORTED.ContainsKey(codeObject.FunctionName.ToLower().ToPropertyName())) {
					return CheckAggExpression(codeObject.FunctionName, args);
				}
				return new CallExpression(new ReferenceExpression(codeObject.FunctionName), args);
			}

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
