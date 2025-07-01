using Pansynchro.PanSQL.Compiler.Ast;

namespace Pansynchro.PanSQL.Compiler.Steps
{
	internal interface IVisitor
	{
		void OnAlterStatement(AlterStatement alterStatement);
		void OnAnalyzeOption(AnalyzeOption analyzeOption);
		void OnAnalyzeStatement(AnalyzeStatement analyzeStatement);
		void OnCallStatement(CallStatement callStatement);
		void OnCompoundIdentifier(CompoundIdentifier compoundIdentifier);
		void OnCredentialExpression(CredentialExpression credentialExpression);
		void OnFile(PanSqlFile panSqlFile);
		void OnFunctionCallExpression(FunctionCallExpression functionCallExpression);
		void OnIdentifier(Identifier identifier);
		void OnIntegerLiteralExpression(IntegerLiteralExpression integerLiteralExpression);
		void OnJsonInterpolatedExpression(JsonInterpolatedExpression jsonInterpolatedExpression);
		void OnJsonLiteralExpression(JsonLiteralExpression jsonLiteralExpression);
		void OnLoadStatement(LoadStatement loadStatement);
		void OnMappingExpression(MappingExpression mappingExpression);
		void OnMapStatement(MapStatement mapStatement);
		void OnOpenStatement(OpenStatement openStatement);
		void OnReadStatement(ReadStatement readStatement);
		void OnSaveStatement(SaveStatement saveStatement);
		void OnScriptVarDeclarationStatement(ScriptVarDeclarationStatement scriptVarDeclarationStatement);
		void OnScriptVarExpression(ScriptVarExpression scriptVarExpression);
		void OnSqlStatement(SqlTransformStatement sqlStatement);
		void OnStringLiteralExpression(StringLiteralExpression stringLiteralExpression);
		void OnSyncStatement(SyncStatement syncStatement);
		void OnTsqlExpression(TSqlExpression sqlExpression);
		void OnTypeDefinition(TypeDefinition typeDefinition);
		void OnVarDeclaration(VarDeclaration varDeclaration);
		void OnWriteStatement(WriteStatement writeStatement);
	}
}