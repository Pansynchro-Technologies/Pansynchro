using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal abstract class VisitorCompileStep : CompileStep, IVisitor
	{
		protected PanSqlFile _file = null!;

		public override void Execute(PanSqlFile f)
		{
			_file = f;
			OnFile(f);
		}

		public void Visit(Node? node)
		{
			node?.Accept(this);
		}

		public bool VisitList<T>(T[]? list) where T : Node
		{
			if (list == null) return false;

			for (int i = 0; i < list.Length; ++i) {
				Visit(list[i]);
			}
			return true;
		}

		public virtual void OnCompoundIdentifier(CompoundIdentifier node)
		{ }

		public virtual void OnCredentialExpression(CredentialExpression node)
		{
			Visit(node.Value);
		}

		public virtual void OnFunctionCallExpression(FunctionCallExpression node)
		{
			VisitList(node.Args);
		}

		public virtual void OnFile(PanSqlFile node)
		{
			VisitList(node.Lines);
		}

		public virtual void OnIdentifier(Identifier node)
		{ }

		public virtual void OnScriptVarExpression(ScriptVarExpression node)
		{ }

		public virtual void OnLoadStatement(LoadStatement node)
		{ }

		public virtual void OnSaveStatement(SaveStatement saveStatement)
		{ }

		public virtual void OnMappingExpression(MappingExpression node)
		{
			Visit(node.Key);
			Visit(node.Value);
		}

		public virtual void OnMapStatement(MapStatement node)
		{
			Visit(node.Source);
			Visit(node.Dest);
			VisitList(node.Mappings);
		}

		public virtual void OnOpenStatement(OpenStatement node)
		{
			Visit(node.Dictionary);
			Visit(node.Creds);
			VisitList(node.Source);
		}

		public virtual void OnSqlStatement(SqlTransformStatement node)
		{
			Visit(node.Dest);
		}

		public virtual void OnSyncStatement(SyncStatement node)
		{
			Visit(node.Input);
			Visit(node.Output);
		}

		public virtual void OnReadStatement(ReadStatement node)
		{
			Visit(node.Table);
		}

		public virtual void OnWriteStatement(WriteStatement node)
		{
			Visit(node.Table);
		}

		public virtual void OnVarDeclaration(VarDeclaration node)
		{
			Visit(node.Identifier);
		}

		public virtual void OnScriptVarDeclarationStatement(ScriptVarDeclarationStatement node)
		{
			Visit(node.Name);
			Visit(node.Type);
			Visit(node.Expr);
		}

		public virtual void OnAnalyzeStatement(AnalyzeStatement node)
		{
			Visit(node.Conn);
			Visit(node.Dict);
			VisitList(node.Options);
		}

		public virtual void OnAnalyzeOption(AnalyzeOption node)
		{
			VisitList(node.Values);
		}

		public virtual void OnAlterStatement(AlterStatement node)
		{
			Visit(node.Table);
			Visit(node.Property);
			Visit(node.Value);
		}

		public virtual void OnCallStatement(CallStatement node)
		{
			Visit(node.Call);
		}

		public virtual void OnTsqlExpression(TSqlExpression sqlExpression)
		{ }

		public virtual void OnTypeDefinition(TypeDefinition typeDefinition)
		{ }

		public virtual void OnStringLiteralExpression(StringLiteralExpression stringLiteralExpression)
		{ }

		public virtual void OnIntegerLiteralExpression(IntegerLiteralExpression integerLiteralExpression)
		{ }

		public virtual void OnJsonLiteralExpression(JsonLiteralExpression jsonLiteralExpression)
		{ }

		public virtual void OnJsonInterpolatedExpression(JsonInterpolatedExpression node)
		{
			foreach (var pair in node.Ints) {
				Visit(pair.Value);
			}
		}
	}
}
