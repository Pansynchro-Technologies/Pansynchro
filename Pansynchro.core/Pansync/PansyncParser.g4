parser grammar PansyncParser;

options {
	tokenVocab = PansyncLexer;
}

file : statement+ EOF;

statement : (command | data_list) EOL?;

command :
	IDENTIFIER expression_list command_result?
	(COLON NEWLINE INDENT block)?;

block : statement+ (DEDENT | EOF);

expression_list :
		(	expression
			(	COMMA expression
			)*
		)?
	;

command_result: AS expression_list;

data_list: LBRACK (expression (COMMA expression)*)? RBRACK NEWLINE;

expression : name | string | named_list | kv_list | INTEGER;

name : IDENTIFIER;

string: SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING;

title : name | string;

named_list: title LPAREN expression_list RPAREN;

kv_list: LBRACE (kv_pair (COMMA kv_pair)*)? RBRACE;

kv_pair: string COLON expression;