using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.Core.DataDict;
using Pansynchro.Core.DataDict.TypeSystem;
using Pansynchro.Core.Pansync;
using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Functions;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	using IntegerLiteralExpression = DataModels.IntegerLiteralExpression;
	using StringLiteralExpression = DataModels.StringLiteralExpression;

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

		public override void OnTsqlExpression(TSqlExpression node)
		{
			base.OnTsqlExpression(node);
			DoBindTypes(node.Value, _file.Vars.Values.ToList(), node);
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
			var nodeType = node.Type;
			IFieldType type;
			if (nodeType is RecordTypeReferenceExpression rtr) {
				if (!_file.Vars.TryGetValue(rtr.Name, out var vbl) || vbl.Type != "Table") {
					throw new CompilerError($"Record type '{rtr.Name}' is not defined as a Table variable", node);
				}
				var stream = GetStream(vbl)!;
				type = BuildRecordType(stream);
			} else {
				type = nodeType.GetFieldType();
			}
			node.FieldType = type;
			node.ScriptName = CodeBuilder.NewNameReference(node.Name.Name);
			_file.Vars[node.ScriptName.Name] = _file.Vars[node.Name.Name];
			base.OnScriptVarDeclarationStatement(node);
		}

		private static TupleField BuildRecordType(StreamDefinition stream)
		{
			var fields = stream.Fields.Select(f => KeyValuePair.Create(f.Name, f.Type)).ToArray();
			return new TupleField(stream.Name.ToTableName(), fields, false);
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

		public override void OnCompoundIdentifier(CompoundIdentifier node)
		{
			if (node.Parent is ScriptVarExpression sv) {
				OnScriptVarExpression(sv);
				if (sv.ExpressionType is not TupleField tf) {
					throw new CompilerError($"Variable '{sv.Name}' does not contain any fields.", node);
				}
				var field = tf.Fields.FirstOrDefault(f => f.Key.Equals(node.Name, StringComparison.InvariantCultureIgnoreCase));
				if (field.Value == null) {
					throw new CompilerError($"Variable '{sv.Name}' does not contain a field named '{node.Name}'.", node);
				}
				node.Expr = new MemberReferenceExpression(new(sv.Name), node.Name!) { Type = field.Value };
			}
		}

		private void DoBindTypes(DbExpression[] fields, List<Variable> tables, Node node)
		{
			foreach (var field in fields) {
				LookupField(field, tables, node);
			}
		}

		private void DoBindTypes(DbExpression? expr, List<Variable> tables, Node node)
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

		private void DoBindTypes(JoinSpec[] joins, List<Variable> tables, Node node)
		{
			foreach (var js in joins) {
				DoBindTypes(js.Condition, tables, node);
			}
		}

		private void LookupField(DbExpression? field, List<Variable> tables, Node node)
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
					var decl = (ScriptVarDeclarationStatement)_file.Vars[v.Name].Declaration;
					v.Type = decl.FieldType;
					v.ScriptVarName = decl.ScriptName.Name;
					break;
				case ReferenceExpression:
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
				case TableFunctionCall tfc:
					foreach (var value in tfc.Args) {
						LookupField(value, tables, node);
					}
					break;
				case CastExpression cast:
					LookupField(cast.Value, tables, node);
					break;
				case TryCastExpression tCast:
					LookupField(tCast.Value, tables, node);
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

		private void LookupField(MemberReferenceExpression m, List<Variable> tables, Node node)
		{
			LookupField(m.Parent, tables, node);
			var tableName = m.Parent.Name;
			var vbl = tables.FirstOrDefault(t => t.Name == tableName);
			if (vbl == null) {
				throw new CompilerError($"No input named '{tableName}' is defined in this SQL statement", node);
			}
			m.Type = GetFieldType(vbl.Declaration, tableName, m.Name)
				?? throw new CompilerError($"'{tableName}' does not contain a field named '{m.Name}'.", node);
		}

		private static IFieldType? GetFieldType(Ast.Statement decl, string tableName, string name) => decl switch {
			VarDeclaration or SqlTransformStatement => (decl switch {
				VarDeclaration vd => vd.Stream,
				SqlTransformStatement sql => sql.Ctes.FirstOrDefault(c => c.Name == tableName)?.Stream,
			})?.Fields.FirstOrDefault(f => f.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase))?.Type,
			ScriptVarDeclarationStatement sv => (sv.FieldType as TupleField)?.Fields
				.FirstOrDefault(p => p.Key.Equals(name, StringComparison.InvariantCultureIgnoreCase)).Value,
			_ => throw new NotImplementedException()
		};

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
