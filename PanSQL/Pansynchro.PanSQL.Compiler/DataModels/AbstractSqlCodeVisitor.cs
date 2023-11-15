using System;

using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace Pansynchro.PanSQL.Compiler.DataModels
{
	internal class AbstractSqlCodeVisitor : ISqlCodeObjectVisitor
	{
		public virtual void Visit(SqlAggregateFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAllAnyComparisonBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAllowPageLocksIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAllowRowLocksIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAtTimeZoneExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBatch codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBetweenBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBinaryBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBinaryFilterExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBinaryQueryExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBinaryScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBooleanFilterExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBuiltinScalarFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCastExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlChangeTrackingContext codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCheckConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlClrAssemblySpecifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlClrClassSpecifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlClrFunctionBodyDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlClrMethodSpecifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCollateScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCollation codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlColumnAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDefaultConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlColumnDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlColumnIdentity codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlColumnRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCommonTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlComparisonBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCompressionPartitionRange codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlComputedColumnDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlConditionClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlConvertExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCubeGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCursorOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCursorVariableAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCursorVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDataCompressionIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDataType codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDataTypeSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDdlTriggerDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDefaultValuesInsertMergeActionSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDefaultValuesInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDeleteMergeAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDeleteSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDerivedTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDistinctPredicateComparisonBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDmlSpecificationTableSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDmlTriggerDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropExistingIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlExecuteArgument codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlExecuteAsClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlExistsBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlFillFactorIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlFilterClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForBrowseClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForeignKeyConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForXmlAutoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForXmlClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForXmlDirective codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForXmlExplicitClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForXmlPathClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlForXmlRawClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlFromClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlFullTextBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlFullTextColumn codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlGlobalScalarVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlGrandTotalGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlGrandTotalGroupingSet codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlGroupByClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlGroupBySets codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlGroupingSetItemsCollection codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlHavingClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIdentifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIdentityFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIgnoreDupKeyIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInBooleanExpressionCollectionValue codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInBooleanExpressionQueryValue codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIndexedColumn codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIndexHint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInlineIndexConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInlineFunctionBodyDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInlineTableRelationalFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInlineTableVariableDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInsertMergeAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInsertSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIntoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIsNullBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlJsonObjectArgument codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlLargeDataStorageInformation codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlLikeBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlLiteralExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlLoginPassword codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlMaxDegreeOfParallelismIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlMergeActionClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlMergeSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleCalledOnNullInputOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleEncryptionOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleExecuteAsOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleInlineOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleNativeCompilationOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleRecompileOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleReturnsNullOnNullInputOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleSchemaBindingOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlModuleViewMetadataOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlMultistatementFunctionBodyDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlMultistatementTableRelationalFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlNotBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlNullQualifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlQueryExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlObjectIdentifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlObjectReference codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlOnlineIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlOptimizeForSequentialKeyIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlResumableIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBucketCountIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCompressionDelayIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlMaxDurationIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlOffsetFetchClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlOrderByClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlOrderByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlOutputClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlOutputIntoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlPadIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlParameterDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlPivotClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlPivotTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlPrimaryKeyConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlStorageSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlProcedureDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlQualifiedJoinTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlQuerySpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlQueryWithClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRollupGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRowConstructorExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarClrFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarFunctionReturnType codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarRelationalFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarSubQueryExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarVariableAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScalarVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlScript codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSearchedCaseExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSearchedWhenClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectIntoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectSpecificationInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectStarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectVariableAssignmentExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSetClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSimpleCaseExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSimpleGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSimpleOrderByClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSimpleOrderByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSimpleWhenClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSortedDataIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSortedDataReorgIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSortInTempDbIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlStatisticsIncrementalIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlStatisticsNoRecomputeIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlStatisticsOnlyIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableClrFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableConstructorExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableConstructorInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableFunctionReturnType codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableHint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableValuedFunctionRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTableUdtInstanceMethodExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTargetTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTemporalPeriodDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTopSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTriggerAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTriggerDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTriggerEvent codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUdtInstanceDataMemberExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUdtInstanceMethodExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUdtStaticDataMemberExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUdtStaticMethodExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUnaryScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUniqueConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUnpivotClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUnpivotTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUnqualifiedJoinTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUpdateBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUpdateMergeAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUpdateSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUserDefinedScalarFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlValuesInsertMergeActionSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlVariableColumnAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlVariableDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlViewDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlWindowClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlWindowExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlWindowSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlWhereClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlXmlNamespacesDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAlterFunctionStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAlterLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAlterProcedureStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAlterTriggerStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlAlterViewStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBackupCertificateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBackupDatabaseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBackupLogStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBackupMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBackupServiceMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBackupTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlBreakStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCommentStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCompoundStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlContinueStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateFunctionStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateIndexStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateLoginFromAsymKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateLoginFromCertificateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateLoginFromWindowsStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateLoginWithPasswordStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateProcedureStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateRoleStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateSchemaStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateSynonymStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateTriggerStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserDefinedDataTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserDefinedTableTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserDefinedTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserFromAsymKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserFromCertificateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserWithImplicitAuthenticationStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserFromLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserFromExternalProviderStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateUserWithoutLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCreateViewStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlCursorDeclareStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDBCCStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDeleteStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDenyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropAggregateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropDatabaseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropDefaultStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropFunctionStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropProcedureStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropRuleStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropSchemaStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropSecurityPolicyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropSequenceStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropSynonymStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropTriggerStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropUserStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlDropViewStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlExecuteModuleStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlExecuteStringStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlGrantStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlIfElseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInlineTableVariableDeclareStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlInsertStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlMergeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRestoreDatabaseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRestoreInformationStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRestoreLogStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRestoreMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRestoreServiceMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRestoreTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlReturnStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlRevokeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSelectStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSetAssignmentStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlSetStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlTryCatchStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUpdateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlUseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlVariableDeclareStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual void Visit(SqlWhileStatement statement)
		{
			throw new NotImplementedException();
		}
	}

	internal class AbstractSqlCodeVisitor<T> : ISqlCodeObjectVisitor<T>
	{
		public virtual T Visit(SqlAggregateFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAllAnyComparisonBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAllowPageLocksIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAllowRowLocksIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAtTimeZoneExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBatch codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBetweenBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBinaryBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBinaryFilterExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBinaryQueryExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBinaryScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBooleanFilterExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBuiltinScalarFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCastExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlChangeTrackingContext codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCheckConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlClrAssemblySpecifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlClrClassSpecifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlClrFunctionBodyDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlClrMethodSpecifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCollateScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCollation codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlColumnAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDefaultConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlColumnDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlColumnIdentity codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlColumnRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCommonTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlComparisonBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCompressionPartitionRange codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlComputedColumnDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlConditionClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlConvertExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCubeGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCursorOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCursorVariableAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCursorVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDataCompressionIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDataType codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDataTypeSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDdlTriggerDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDefaultValuesInsertMergeActionSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDefaultValuesInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDeleteMergeAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDeleteSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDerivedTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDistinctPredicateComparisonBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDmlSpecificationTableSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDmlTriggerDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropExistingIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlExecuteArgument codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlExecuteAsClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlExistsBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlFillFactorIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlFilterClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForBrowseClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForeignKeyConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForXmlAutoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForXmlClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForXmlDirective codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForXmlExplicitClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForXmlPathClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlForXmlRawClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlFromClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlFullTextBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlFullTextColumn codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlGlobalScalarVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlGrandTotalGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlGrandTotalGroupingSet codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlGroupByClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlGroupBySets codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlGroupingSetItemsCollection codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlHavingClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIdentifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIdentityFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIgnoreDupKeyIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInBooleanExpressionCollectionValue codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInBooleanExpressionQueryValue codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIndexedColumn codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIndexHint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInlineIndexConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInlineFunctionBodyDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInlineTableRelationalFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInlineTableVariableDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInsertMergeAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInsertSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIntoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIsNullBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlJsonObjectArgument codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlLargeDataStorageInformation codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlLikeBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlLiteralExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlLoginPassword codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlMaxDegreeOfParallelismIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlMergeActionClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlMergeSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleCalledOnNullInputOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleEncryptionOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleExecuteAsOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleInlineOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleNativeCompilationOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleRecompileOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleReturnsNullOnNullInputOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleSchemaBindingOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlModuleViewMetadataOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlMultistatementFunctionBodyDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlMultistatementTableRelationalFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlNotBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlNullQualifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlQueryExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlObjectIdentifier codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlObjectReference codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlOnlineIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlOptimizeForSequentialKeyIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlResumableIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBucketCountIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCompressionDelayIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlMaxDurationIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlOffsetFetchClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlOrderByClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlOrderByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlOutputClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlOutputIntoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlPadIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlParameterDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlPivotClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlPivotTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlPrimaryKeyConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlStorageSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlProcedureDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlQualifiedJoinTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlQuerySpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlQueryWithClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRollupGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRowConstructorExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarClrFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarFunctionReturnType codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarRelationalFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarSubQueryExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarVariableAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScalarVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlScript codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSearchedCaseExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSearchedWhenClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectIntoClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectSpecificationInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectStarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectVariableAssignmentExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSetClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSimpleCaseExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSimpleGroupByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSimpleOrderByClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSimpleOrderByItem codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSimpleWhenClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSortedDataIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSortedDataReorgIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSortInTempDbIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlStatisticsIncrementalIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlStatisticsNoRecomputeIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlStatisticsOnlyIndexOption codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableClrFunctionDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableConstructorExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableConstructorInsertSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableFunctionReturnType codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableHint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableValuedFunctionRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableVariableRefExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTableUdtInstanceMethodExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTargetTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTemporalPeriodDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTopSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTriggerAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTriggerDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTriggerEvent codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUdtInstanceDataMemberExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUdtInstanceMethodExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUdtStaticDataMemberExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUdtStaticMethodExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUnaryScalarExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUniqueConstraint codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUnpivotClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUnpivotTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUnqualifiedJoinTableExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUpdateBooleanExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUpdateMergeAction codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUpdateSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUserDefinedScalarFunctionCallExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlValuesInsertMergeActionSource codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlVariableColumnAssignment codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlVariableDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlViewDefinition codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlWindowClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlWindowExpression codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlWindowSpecification codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlWhereClause codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlXmlNamespacesDeclaration codeObject)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAlterFunctionStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAlterLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAlterProcedureStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAlterTriggerStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlAlterViewStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBackupCertificateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBackupDatabaseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBackupLogStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBackupMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBackupServiceMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBackupTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlBreakStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCommentStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCompoundStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlContinueStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateFunctionStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateIndexStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateLoginFromAsymKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateLoginFromCertificateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateLoginFromWindowsStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateLoginWithPasswordStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateProcedureStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateRoleStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateSchemaStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateSynonymStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateTriggerStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserDefinedDataTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserDefinedTableTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserDefinedTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserFromAsymKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserFromCertificateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserWithImplicitAuthenticationStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserFromLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserFromExternalProviderStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateUserWithoutLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCreateViewStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlCursorDeclareStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDBCCStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDeleteStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDenyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropAggregateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropDatabaseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropDefaultStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropFunctionStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropLoginStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropProcedureStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropRuleStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropSchemaStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropSecurityPolicyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropSequenceStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropSynonymStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropTriggerStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropTypeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropUserStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlDropViewStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlExecuteModuleStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlExecuteStringStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlGrantStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlIfElseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInlineTableVariableDeclareStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlInsertStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlMergeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRestoreDatabaseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRestoreInformationStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRestoreLogStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRestoreMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRestoreServiceMasterKeyStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRestoreTableStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlReturnStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlRevokeStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSelectStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSetAssignmentStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlSetStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlTryCatchStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUpdateStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlUseStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlVariableDeclareStatement statement)
		{
			throw new NotImplementedException();
		}

		public virtual T Visit(SqlWhileStatement statement)
		{
			throw new NotImplementedException();
		}
	}
}
