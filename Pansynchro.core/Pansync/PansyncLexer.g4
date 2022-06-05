lexer grammar PansyncLexer;

tokens {
	INDENT,
	DEDENT,
	EOL,
	EOS,
}

options {
    superClass=Python3LexerBase;
}

AS :	'as';

IDENTIFIER
	:	ID_LETTER
		(	ID_LETTER
		|	DIGIT
		|   DOT
		)*
	;

INTEGER : DIGIT+ 'L'?;

fragment
ID_LETTER
	:	[_a-zA-Z]
	|	[\u0080-\uFFFE] {char.IsLetter((char)_input.La(-1))}?
	;

fragment
DIGIT
	:	[0-9]
	;

DOT
	:	'.'
	;

COMMA: ',';

COLON : ':';

LPAREN : '(';

RPAREN : ')';

LBRACK : '[';

RBRACK : ']';

LBRACE : '{';
	
RBRACE : '}';

ADD: '+';

SUBTRACT: '-';

MODULUS: '%';

MULTIPLY: '*';

ASSIGN: '=';

EXPONENTIATION: '**';

DIVISION: '/';

LESS_THAN: '<';

CMP_OPERATOR
	:	'<='
	|	'>='
	|	'!='
	|	'=='
	|	'=~'
	;

WS
	:	(	[ \t\f]
		)+
	-> skip;

EOS: ';';

NEWLINE
	:	( '\r'? '\n' | '\r' ) SPACES?
		{
			this.onNewLine();
			if (SkipWhitespace)
				Channel = Hidden;
		}
	;

fragment SPACES
 : [ \t]+
 ;

DOUBLE_QUOTED_STRING
	:	'"'
		(	DQS_ESC
		|	~["\\\r\n]
		)*
		'"'
	;

SINGLE_QUOTED_STRING
	:	'\''
		(	SQS_ESC
		|	~['\\\r\n]
		)*
		'\''
	;

fragment
SQS_ESC
	:	'\\'
		(	SESC
		|	'\''
		)
	;

fragment
DQS_ESC
	:	'\\'
		(	SESC
		|	'"'
		)
	;

SESC
	:	'r' {setText("\r"); }
	|	'n' {setText("\n"); }
	|	't' {setText("\t"); }
	|	'a' {text.Length = _begin; text.Append("\a"); }
	|	'b' {text.Length = _begin; text.Append("\b"); }
	|	'f' {text.Length = _begin; text.Append("\f"); }
	|	'0' {text.Length = _begin; text.Append("\0"); }
	|	'u'
		HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT
		{
			char ch = (char)int.Parse(text.ToString(_begin, 4), System.Globalization.NumberStyles.HexNumber);
			text.Length = _begin;
			text.Append(ch);
		}
	|	'\\' {setText("\\"); }
	;

fragment
HEXDIGIT
	:	[a-fA-F0-9]
	;
