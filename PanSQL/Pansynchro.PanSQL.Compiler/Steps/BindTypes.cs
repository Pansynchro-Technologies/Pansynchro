using System;
using System.Collections.Generic;
using System.Linq;

using Pansynchro.PanSQL.Compiler.Ast;
using Pansynchro.PanSQL.Compiler.DataModels;
using Pansynchro.PanSQL.Compiler.Helpers;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal class BindTypes : VisitorCompileStep
	{
		public override void OnSqlStatement(SqlTransformStatement node)
		{
			var model = node.DataModel.Model;
			var fields = model.Outputs;
			DoBindTypes(fields, node.Tables, node);
			if (model.GroupKey != null) {
				DoBindTypes(model.GroupKey, node.Tables, node);
			}
		}

		private static void DoBindTypes(DbExpression[] fields, List<Variable> tables, SqlTransformStatement node)
		{
			foreach (var field in fields) {
				LookupField(field, tables, node);
			}
		}

		private static void LookupField(DbExpression field, List<Variable> tables, SqlTransformStatement node)
		{
			switch (field) {
				case AliasedExpression a:
					LookupField(a.Expr, tables, node);
					a.Type = a.Expr.Type;
					break;
				case MemberReferenceExpression m:
					LookupField(m, tables, node);
					break;
				case AggregateExpression ag:
					LookupField(ag.Arg, tables, node);
					ag.Type = ag.Name == "Count" ? TypesHelper.IntType : ag.Arg.Type;
					break;
				case CountExpression:
				case IntegerLiteralExpression:
					field.Type = TypesHelper.IntType;
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
			var table = (((VarDeclaration?)tables.FirstOrDefault(t => t.Name == tableName)?.Declaration)?.Stream)
				?? throw new CompilerError($"No input named '{tableName}' is defined in this SQL statement", node);
			var field = table.Fields.FirstOrDefault(f => f.Name.Equals(m.Name, StringComparison.InvariantCultureIgnoreCase))
				?? throw new CompilerError($"'{tableName}' does not contain a field named '{m.Name}'.", node);
			m.Type = field.Type;
		}

		public override IEnumerable<(Type, Func<CompileStep>)> Dependencies() 
			=> [Dependency<SqlAnalysis>()];
	}
}
