from lark import Lark, UnexpectedInput, Tree

class FoxFormulaParser:
    _grammar = r"""
    ?start: expr

    ?expr: or_expr

    ?or_expr: and_expr
            | or_expr "or" and_expr                -> or_op
            | or_expr "OR" and_expr                -> or_op

    ?and_expr: comparison
             | and_expr "and" comparison           -> and_op
             | and_expr "AND" comparison           -> and_op

    ?comparison: sum
               | sum "=" sum                       -> eq
               | sum "!=" sum                      -> neq
               | sum "<" sum                       -> lt
               | sum "<=" sum                      -> le
               | sum ">" sum                       -> gt
               | sum ">=" sum                      -> ge

    ?sum: term
        | sum "+" term                             -> add
        | sum "-" term                             -> sub

    ?term: factor
         | term "*" factor                         -> mul
         | term "/" factor                         -> div

    ?factor: atom
           | "-" factor                            -> neg
           | "(" expr ")"

    ?atom: var
         | func
         | if_expr
         | string
         | SIGNED_NUMBER                           -> number

    ?if_expr: "IF" "(" expr "," expr "," expr ")"          -> if_expr
            | "IF" "(" expr ";" expr ";" expr ")"          -> if_expr_semicolon

    string: ESCAPED_STRING
          | /'(\\.|[^'\\])*'/

    var: "[" "X" INT "]"                            -> varref

    func: CNAME "(" [args] ")"                     -> function

    args: expr ("," expr)*

    %import common.CNAME
    %import common.INT
    %import common.SIGNED_NUMBER
    %import common.ESCAPED_STRING
    %import common.WS
    %ignore WS
    """

    def __init__(self):
        self._parser = Lark(self._grammar, parser="lalr", start="start")

    def parse(self, formula: str) -> Tree:
        try:
            return self._parser.parse(formula)
        except UnexpectedInput as e:
            raise ValueError(f"Failed to parse formula: {formula}") from e

    def is_valid(self, formula: str) -> bool:
        try:
            self._parser.parse(formula)
            return True
        except UnexpectedInput:
            return False