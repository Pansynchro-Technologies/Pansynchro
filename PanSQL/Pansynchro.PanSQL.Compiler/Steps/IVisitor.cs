using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal interface IVisitor
	{
		void OnAnalyzeOption(AnalyzeOption analyzeOption);
		void OnAnalyzeStatement(AnalyzeStatement analyzeStatement);
		void OnCompoundIdentifier(CompoundIdentifier compoundIdentifier);
		void OnCredentialExpression(CredentialExpression credentialExpression);
		void OnFile(PanSqlFile panSqlFile);
		void OnIdentifier(Identifier identifier);
		void OnLoadStatement(LoadStatement loadStatement);
		void OnMappingExpression(MappingExpression mappingExpression);
		void OnMapStatement(MapStatement mapStatement);
		void OnOpenStatement(OpenStatement openStatement);
		void OnSaveStatement(SaveStatement saveStatement);
		void OnSqlStatement(SqlTransformStatement sqlStatement);
		void OnSyncStatement(SyncStatement syncStatement);
		void OnVarDeclaration(VarDeclaration varDeclaration);
	}
}