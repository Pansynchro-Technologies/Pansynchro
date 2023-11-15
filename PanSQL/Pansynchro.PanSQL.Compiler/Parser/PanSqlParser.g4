parser grammar PanSqlParser;

options {
	tokenVocab = PanSqlLexer;
}

file : NEWLINE* line+ EOF;

line : statement ({InputStream.LA(1) == TokenConstants.EOF}? | NEWLINE+);

statement : loadStatement | saveStatement | openStatement | analyzeStatement | varDeclaration | mapStatement | sqlStatement | syncStatement;

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

credentials : STRING | credentialLocator;

credentialLocator : id LPAREN STRING RPAREN;

varDeclaration : varType id AS compoundId;

varType : STREAM | TABLE;

mapStatement : MAP compoundId TO compoundId (WITH mappingList)?;

mappingList : LBRACE mapping (COMMA mapping)* COMMA? RBRACE;

mapping : id EQUALS id;

sqlStatement : SELECT sqlToken+ INTO id;

sqlToken : ~INTO;

syncStatement : SYNC id TO id;

id : IDENTIFIER;
compoundId: IDENTIFIER DOT IDENTIFIER;