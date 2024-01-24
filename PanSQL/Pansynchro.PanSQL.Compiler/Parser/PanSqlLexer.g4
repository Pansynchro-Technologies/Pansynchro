lexer grammar PanSqlLexer;

ANALYZE : 'analyze';
AS : 'as';
DECLARE : 'declare';
EXCLUDE : 'exclude';
FOR : 'for';
FROM : 'from';
INCLUDE : 'include';
INTO : 'into';
LOAD : 'load';
MAP : 'map';
MAX : 'max';
NAMESPACE : 'namespace';
NULL : 'null';
OPEN : 'open';
OPTIMIZE : 'optimize';
SAVE : 'save';
SELECT : 'select';
SINK : 'sink';
SOURCE : 'source';
STREAM : 'stream';
SYNC : 'sync';
TABLE : 'table';
TO : 'to';
READ : 'read';
WITH : 'with';
WRITE : 'write';

ARRAY  : '[]';
AT     : '@';
COMMA  : ',';
DOT    : '.';
LPAREN : '(';
RPAREN : ')';
LBRACE : '{';
RBRACE : '}';
EQUALS : '=';

OPERATOR : '+' | '-' | '*' | '/' | '<>' | '>=' | '<=' | '>' | '<' | EQUALS | '||' | '|' | '&&' | '&' | '^' ;

IDENTIFIER
	:	ID_LETTER
		(	ID_LETTER
		|	DIGIT
		)*
	;

NUMBER
	: DIGIT+
	;

fragment
ID_LETTER
	:	[_a-zA-Z]
	|	[\u0080-\uFFFE] {char.IsLetter((char)InputStream.LA(-1))}?
	;

fragment
DIGIT
	:	[0-9]
	;

STRING
	:	'\''
		(	STR_ESC
		|	~'\''
		)*
		'\''
	;

fragment
STR_ESC
	:	'\'\''
	;

SL_COMMENT
	:	(	'--' ~[\r\n]*
		)
		-> channel(HIDDEN)
	;

ML_COMMENT
	:	'/*'
		(	'*' {InputStream.LA(1) != '/'}?
		|	ML_COMMENT
		|	NEWLINE
		|	~[*\r\n]
		)*
		'*/'
		-> channel(HIDDEN)
	;

WS
	:	(	[ \t\f]
		)+
		-> channel(HIDDEN)
	;

NEWLINE
	:	(	'\n'
		|	'\r' '\n'?
		)
	;