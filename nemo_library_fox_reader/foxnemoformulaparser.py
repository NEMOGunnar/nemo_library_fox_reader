# expression          : '(' expression ')'                                    #parenthesisExp
#                     | NAME '(' expression (',' expression)* ')'             #functionCallExp
#                     | <assoc=right>  expression POWER expression            #powerExp
#                     | expression MULTOP expression                          #mulDivExp
#                     | expression ADDOP expression                           #addSubExp
#                     | ADDOP expression                                      #unaryExp
#                     | expression COMPARE expression                         #compareExp
#                     | expression IN '(' expression (',' expression)* ')'    #containedInExp
#                     | CONCAT '(' expression (',' expression)* ')'           #concatExp
#                     | expression (AND | OR) expression                      #logicExp                    
#                     | IF '(' expression ',' expression ',' expression ')'   #ifExp
#                     | NUMBER                                                #numericAtomExp
#                     | RESERVEDSYMBOL                                        #reservedSymbol
#                     | NAME                                                  #columnAtomExp
#                     | QUOTE                                                 #quote
#                     | UDF '(' UDF_NAME (',' expression)* ')'                #udfExp
#                     ;


# /* | NOT expression                              #unaryLogicExp */

# /*
#  * Lexer Rules
#  */

# fragment LOWERCASE  : [a-z] ;
# fragment DIGIT      : [0-9] ;
# fragment UPPERCASE  : [A-Z] ;

# fragment ASTERISK            : '*' ;
# fragment SLASH               : '/' ;
# fragment PLUS                : '+' ;
# fragment MINUS               : '-' ;

# fragment EQUALS              : '==' ;
# fragment NOTEQUALS           : '!=' ;
# fragment LESSEQUAL           : '<=' ;
# fragment LESS                : '<' ;
# fragment GREATEREQUAL        : '>=' ;
# fragment GREATER             : '>' ;

# fragment IN_1                : 'IN' ;
# fragment IN_2                : 'in' ;

# fragment UDF_1                : 'UDF' ;
# fragment UDF_2                : 'udf' ;

# fragment CONCAT_1            : 'CONCAT' ;
# fragment CONCAT_2            : 'concat' ;

# fragment AND_1               : 'AND' ;
# fragment AND_2               : 'and' ;
# fragment OR_1                : 'OR' ;
# fragment OR_2                : 'or' ;
# fragment NOT_1               : 'NOT' ;
# fragment NOT_2               : 'not' ;

# fragment IF_1                : 'IF' ;
# fragment IF_2                : 'if' ;

# fragment NOW                 : 'NOW' ;
# fragment TODAY               : 'TODAY' ;

# AND                 : (AND_1 | AND_2) ;
# OR                  : (OR_1 | OR_2) ;
# NOT                 : (NOT_1 | NOT_2) ;

# IF                  : (IF_1 | IF_2) ;

# IN                  : (IN_1 | IN_2) ;

# CONCAT              : (CONCAT_1 | CONCAT_2) ;

# UDF                 : (UDF_1 | UDF_2)  ;

# QUOTE               : ('"' ~('"')* '"' | '\'' ~('\'')* '\'') ;

# RESERVEDSYMBOL      : (NOW | TODAY) ;

# COMPARE             : (EQUALS | NOTEQUALS | LESSEQUAL | LESS | GREATEREQUAL | GREATER) ;

# MULTOP              : (ASTERISK | SLASH) ;

# ADDOP               : (PLUS | MINUS) ;

# POWER               : '^' ;

# NUMBER              : DIGIT+ ('.' DIGIT+)? ;

# NAME                : LOWERCASE (LOWERCASE | '_' | DIGIT)+ ;

# UDF_NAME            : UPPERCASE (UPPERCASE | '_' | DIGIT)+ ;

# WHITESPACE          : ' ' -> skip ;

# ESCAPE_SEQUENCE     : ('\\r' | '\r' | '\\n' | '\n' | '\\t' | '\t') -> skip ;
