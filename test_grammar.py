from lark import Lark, exceptions

# Define the extended grammar
extended_grammar = r"""
?start: expr

?expr: or_expr

?or_expr: and_expr
        | or_expr ( "or"i ) and_expr        -> or_op

?and_expr: comparison
         | and_expr ( "and"i ) comparison  -> and_op

?comparison: sum
           | sum "=" sum                   -> eq
           | sum "!=" sum                  -> neq
           | sum "<>" sum                  -> neq
           | sum "<=" sum                  -> le
           | sum "<" sum                   -> lt
           | sum ">=" sum                  -> ge
           | sum ">" sum                   -> gt

?sum: term
     | sum "+" term                        -> add
     | sum "-" term                        -> sub

?term: factor
      | term "*" factor                    -> mul
      | term "/" factor                    -> div
      | term "%" factor                    -> mod

?factor: atom
        | "-" factor                       -> neg
        | "+" factor                       -> pos
        | "(" expr ")"

?atom: varref
     | func_call
     | if_expr
     | CNAME                             -> identifier
     | SIGNED_NUMBER                    -> number
     | ESCAPED_STRING                   -> string
     | /'(\\.|[^'\\])*'/                -> string_singlequoted

?if_expr: ( "IF"i "(" expr "," expr "," expr ")" )         -> if_expr
        | ( "IF"i "(" expr ";" expr ";" expr ")" )         -> if_expr_semicolon

?func_call: CNAME "(" [arg_list] ")"                         -> function

arg_list: expr ("," expr)*

varref: "[" "X" INT "]"                                      -> varref

%import common.CNAME
%import common.INT
%import common.SIGNED_NUMBER
%import common.ESCAPED_STRING
%import common.WS
%ignore WS
"""

# A small subset of your expressions for testing
sample_expressions = [
    'TRUNC((TODAY() - [X0]) / 365.25)',
    'IF([X0] > 150000 or [X1] > 150000,"A-Kunde",IF([X0] > 35000 or [X1] > 35000,"B-Kunde",IF([X0] = 0 and [X1] = 0,"X-Kunde","C-Kunde")))',
    '[X0] + " " + [X1] + " " + [X2]',
    'RIGHT([X0],4)',
    'QUARTER([X0])',
    'CalendarWeek([X0])',
    '([X0] - [X1]) / [X1] * 100',
    'IF(YEAR([X0]) = YEAR(TODAY()) AND MONTH([X0]) = MONTH(TODAY()),[X1],"")',
    'MONTH([X0]) = MONTH(TODAY())',
    'mod([X0],60)',
    'trunc([X0] / 60)',
    'IF(FIND("/",[X0],2,1) = 0,[X0],LEFT([X0],FIND("/",[X0],2,1) - 1))',
    '"~|Map|http://maps.google.de/maps?&q=" + [X0] + ", " + [X1] + "|"',
    'IF([X0] = "P" OR [X0] = "p",20,"")',
    'DATE(YEAR([X0]),2,29) != ""',
    'IF([X0] = "Sa" OR [X0] = "So","weekend","weekday")',
    'if(matches([X0],"[0-9]"),"yes","no")',
    'if(right([X0],2) != left([X1],2),"yes","no")'
]

# Create the parser
parser = Lark(extended_grammar, parser="lalr", start="start")

# Parse and test expressions
for expr in sample_expressions:
    try:
        parser.parse(expr)
        print(f"✅ OK: {expr}")
    except exceptions.LarkError as e:
        print(f"❌ FAIL: {expr}\n   ↳ {str(e).splitlines()[0]}")

print("Test run ready!")