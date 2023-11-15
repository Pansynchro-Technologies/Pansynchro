using System;

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
		{ }

		public virtual void OnFile(PanSqlFile node)
		{
			VisitList(node.Lines);
		}

		public virtual void OnIdentifier(Identifier node)
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
			Visit(node.Source);
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

		public virtual void OnVarDeclaration(VarDeclaration node)
		{
			Visit(node.Identifier);
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
	}
}
