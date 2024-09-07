parser grammar PanSqlParser;

options {
	tokenVocab = PanSqlLexer;
}

file : NEWLINE* line+ EOF;

line : statement ({InputStream.LA(1) == TokenConstants.EOF}? | NEWLINE+);

statement : loadStatement | saveStatement | openStatement | analyzeStatement | varDeclaration | scriptVarDeclaration | mapStatement | sqlStatement | syncStatement ;

loadStatement : LOAD id FROM STRING; 

openStatement : OPEN id AS id FOR openType WITH (id COMMA)? credentials dataSourceSink?;

dataSourceSink : COMMA id ;

openType : READ | WRITE | ANALYZE | SOURCE | SINK ;

saveStatement : SAVE id TO STRING ;

analyzeStatement : ANALYZE id AS id (WITH analyzeOption (COMMA analyzeOption)* )? ;

analyzeOption : OPTIMIZE | analyzeList ;

analyzeList : analyzeType LPAREN idList RPAREN ;

analyzeType : INCLUDE | EXCLUDE ;

idList : idElement (COMMA idElement)* ;

idElement : compoundId | id ;

credentials : expression;

functionCall : id LPAREN argList? RPAREN;

argList : expression (COMMA expression)*;

varDeclaration : varType id AS compoundId (WITH varOptions)?;

varType : STREAM | TABLE;

varOptions: id;

mapStatement : MAP ((NAMESPACE nullableId TO nullableId) | (compoundId TO compoundId (WITH mappingList)?));

nullableId : id | NULL;

mappingList : LBRACE mapping (COMMA mapping)* COMMA? RBRACE;

mapping : id EQUALS id;

sqlStatement : (SELECT | WITH) sqlToken+ INTO id;

sqlToken : ~INTO;

syncStatement : SYNC id TO id;

scriptVarDeclaration : DECLARE scriptVarRef AS? scriptVarType (EQUALS expression)?;

scriptVarType : id scriptVarSize? ARRAY?;

scriptVarSize : LPAREN (NUMBER | MAX) RPAREN;

expression : literal | idElement | functionCall | scriptVarRef | jsonExpression;

literal : STRING | NUMBER;


scriptVarRef : AT IDENTIFIER;
id : IDENTIFIER;
compoundId: IDENTIFIER (DOT IDENTIFIER)+;

jsonExpression : jsonObject | jsonArray ;

jsonObject : LBRACE NEWLINE* jsonPair NEWLINE* (COMMA NEWLINE* jsonPair NEWLINE*)* RBRACE
	| LBRACE NEWLINE* RBRACE
	;

jsonArray : LBRACK NEWLINE* jsonValue NEWLINE* (COMMA NEWLINE* jsonValue NEWLINE*)* RBRACK
	| LBRACK NEWLINE* RBRACK
	;

jsonPair : JSONSTRING NEWLINE* COLON NEWLINE* jsonValue;

jsonValue : JSONSTRING | JSONNUMBER | NUMBER | jsonObject | jsonArray | TRUE | FALSE | NULL | scriptVarRef;