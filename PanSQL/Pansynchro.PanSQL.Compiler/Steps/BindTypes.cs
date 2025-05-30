using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Functions;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	using StringLiteralExpression = DataModels.StringLiteralExpression;
	using IntegerLiteralExpression = DataModels.IntegerLiteralExpression;

	internal class BindTypes : VisitorCompileStep
	{
		public override void OnSqlStatement(SqlTransformStatement node)
		{
			foreach (var cte in node.Ctes) {
				BindModelTypes(cte.Model.Model, node);
			}
			BindModelTypes(node.DataModel.Model, node);
		}

		private void BindModelTypes(DataModel model, SqlTransformStatement node)
		{
			var fields = model.Outputs;
			if (fields.OfType<StarExpression>().Any()) {
				fields = ExpandStarExpressions(fields, model, node);
				node.DataModel.Model = model with { Outputs = fields };
			}
			var tables = node.Ctes.Count > 0 
				? node.Tables.Concat(node.Ctes.SelectMany(c => c.Model.Model.Inputs).Select(i => _file.Vars[i.Name])).Distinct().ToList()
				: node.Tables;
			DoBindTypes(fields, tables, node);
			DoBindTypes(model.Filter, tables, node);
			DoBindTypes(model.AggFilter, tables, node);
			DoBindTypes(model.Joins, tables, node);
			if (model.GroupKey != null) {
				DoBindTypes(model.GroupKey, tables, node);
			}
		}

		private static DbExpression[] ExpandStarExpressions(DbExpression[] fields, DataModel model, SqlTransformStatement node)
		{
			return [.. Impl()];
			
			IEnumerable<DbExpression> Impl()
			{
				foreach (var field in fields) {
					if (field is StarExpression se) {
						if (se.Table != null) {
							var table = model.Inputs.FirstOrDefault(t => t.Name.Equals(se.Table.Name, StringComparison.InvariantCultureIgnoreCase));
							if (table == null) {
								throw new CompilerError($"Table '{se.Table.Name}' not found.", node);
							}
							foreach (var column in table.Stream.Fields) {
								yield return new MemberReferenceExpression(new ReferenceExpression(table.Name), column.Name);
							}
						} else {
							foreach (var (table, column) in model.Inputs.SelectMany(i => i.Stream.Fields, KeyValuePair.Create)) {
								yield return new MemberReferenceExpression(new ReferenceExpression(table.Name), column.Name);
							}
						}
					} else {
						yield return field;
					}
				}
			}
		}

		public override void OnScriptVarDeclarationStatement(ScriptVarDeclarationStatement node)
		{
			var type = node.Type.GetFieldType();
			node.FieldType = type;
			node.ScriptName = CodeBuilder.NewNameReference(node.Name.Name);
		}

		public override void OnScriptVarExpression(ScriptVarExpression node)
		{
			if (!_file.Vars.TryGetValue(node.Name, out var vbl)) {
				throw new CompilerError($"No variable named '{node}' has been defined", node);
			}
			var sVar = ((ScriptVarDeclarationStatement)vbl.Declaration);
			node.VarType = sVar.FieldType;
			node.Name = sVar.ScriptName.Name;
		}

		private void DoBindTypes(DbExpression[] fields, List<Variable> tables, SqlTransformStatement node)
		{
			foreach (var field in fields) {
				LookupField(field, tables, node);
			}
		}

		private void DoBindTypes(DbExpression? expr, List<Variable> tables, SqlTransformStatement node)
		{
			switch (expr) {
				case null: break;
				case BooleanExpression b:
					DoBindTypes(b.Left, tables, node);
					DoBindTypes(b.Right, tables, node);
					break;
				case BinaryExpression b2:
					DoBindTypes(b2.Left, tables, node);
					DoBindTypes(b2.Right, tables, node);
					break;
				case ContainsExpression c:
					DoBindTypes(c.Collection, tables, node);
					DoBindTypes(c.Value, tables, node);
					break;
				default:
					LookupField(expr, tables, node);
					break;
			}
		}

		private void DoBindTypes(JoinSpec[] joins, List<Variable> tables, SqlTransformStatement node)
		{
			foreach (var js in joins) {
				DoBindTypes(js.Condition, tables, node);
			}
		}

		private void LookupField(DbExpression? field, List<Variable> tables, SqlTransformStatement node)
		{
			switch (field) {
				case null: break;
				case AliasedExpression a:
					LookupField(a.Expr, tables, node);
					a.Type = a.Expr.Type;
					break;
				case MemberReferenceExpression m:
					LookupField(m, tables, node);
					break;
				case VariableReferenceExpression v:
					v.Type = ((ScriptVarDeclarationStatement)_file.Vars[v.Name[1..]].Declaration).FieldType;
					break;
				case AggregateExpression ag:
					LookupField(ag.Args[0], tables, node);
					ag.Type = ag.Name == "Count" ? TypesHelper.IntType : ag.Args[0].Type;
					break;
				case CollectionExpression col:
					foreach (var value in col.Values) {
						LookupField(value, tables, node);
					}
					col.Type = col.Values.Length == 0 
						? TypesHelper.NullType 
						: col.Values[0].Type is CollectionField 
							? col.Values[0].Type 
							: new CollectionField(col.Values[0].Type!, CollectionType.Array, false);
					break;
				case ContainsExpression cont:
					LookupField(cont.Value, tables, node);
					LookupField(cont.Collection, tables, node);
					cont.Type = TypesHelper.BoolType;
					break;
				case IsNullExpression isn:
					LookupField(isn.Value, tables, node);
					isn.Type = TypesHelper.BoolType;
					break;
				case UnaryExpression ue:
					LookupField(ue.Value, tables, node);
					ue.Type = ue.Value.Type;
					break;
				case BinaryExpression b:
					LookupField(b.Left, tables, node);
					LookupField(b.Right, tables, node);
					b.Type = b.Left.Type;
					break;
				case LikeExpression lk:
					LookupField(lk.Left, tables, node);
					LookupField(lk.Right, tables, node);
					lk.Type = TypesHelper.BoolType;
					break;
				case CallExpression call:
					foreach (var value in call.Args) {
						LookupField(value, tables, node);
					}
					FunctionBinder.Bind(call, node);
					break;
				case CastExpression cast:
					LookupField(cast.Value, tables, node);
					break;
				case IfThenExpression it:
					LookupField(it.Cond, tables, node);
					LookupField(it.Result, tables, node);
					it.Type = it.Result.Type;
					break;
				case IfExpression ie:
					foreach (var c in ie.Cases) {
						LookupField(c, tables, node);
					}
					LookupField(ie.ElseCase, tables, node);
					ie.Type = ie.Cases[0].Type;
					break;
				case CountExpression:
				case IntegerLiteralExpression:
					field.Type = TypesHelper.IntType;
					break;
				case FloatLiteralExpression:
					field.Type = TypesHelper.DoubleType;
					break;
				case StringLiteralExpression sl:
					field.Type = TypesHelper.MakeStringType(sl.Value);
					break;
				case NullLiteralExpression:
					field.Type = TypesHelper.NullType;
					break;
				default: throw new NotImplementedException();
			}
		}

		private static void LookupField(MemberReferenceExpression m, List<Variable> tables, SqlTransformStatement node)
		{
			var tableName = m.Parent.ToString();
			var table = (GetStream(tables.FirstOrDefault(t => t.Name == tableName)))
				?? throw new CompilerError($"No input named '{tableName}' is defined in this SQL statement", node);
			var field = table.Fields.FirstOrDefault(f => f.Name.Equals(m.Name, StringComparison.InvariantCultureIgnoreCase))
				?? throw new CompilerError($"'{tableName}' does not contain a field named '{m.Name}'.", node);
			m.Type = field.Type;
		}

		private static StreamDefinition? GetStream(Variable? v) => v?.Declaration switch {
			null => null,
			VarDeclaration vd => vd.Stream,
			SqlTransformStatement sql => sql.Ctes.First(c => c.Name == v.Name).Stream,
			_ => throw new NotImplementedException()
		};

		public override void OnFunctionCallExpression(FunctionCallExpression node)
		{
			base.OnFunctionCallExpression(node);
			FunctionBinder.Bind(node);
		}

		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies() 
			=> [Dependency<SqlAnalysis>()];
	}
}
