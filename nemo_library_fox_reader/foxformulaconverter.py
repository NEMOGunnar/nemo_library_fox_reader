import logging
from lark import Lark, UnexpectedInput, Tree

from nemo_library_fox_reader.foxformulaparser import FoxFormulaParser
from nemo_library_fox_reader.foxattribute import FoxAttribute
from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo
from nemo_library_fox_reader.foxstatisticsinfo import IssueType


class FoxFormulaConverter:
    """
    class for converting formulas and expressions from InfoZoom syntax to Nemo syntax.
    """

    def __init__(self, attr: FoxAttribute, all_attributes: list[FoxAttribute], foxReaderInfo: FOXReaderInfo):
        self.attr = attr
        self.all_attributes = all_attributes
        self.foxReaderInfo = foxReaderInfo


    def get_nemo_formula_from_infozoom_expression(self, expression: str) -> str:
        """
        Converts an expressions with InfoZoom syntax to a formula in Nemo syntax.
        Uses Lark to create an abstract syntax tree (AST) from a given expression
        and traverses the tree composing a Nemo formula
        Args:
            expression (str): Expression to convert into Nemo formula.
        """
        # logging.info(f"get_nemo_formula_from_infozoom_expresion   expression={expression}")

        self.parser = FoxFormulaParser() 
        tree = self.parser.parse(expression)

        if not self.parser.is_valid:
            raise ValueError(f"FoxFormulaConverter parser error  INVALID EXPRESSION:'{expression}'")

        if expression=='if([X0] < 3,"#1#klein",if([X0] < 10,"#2#mittel","#3#gross"))':
            delasap = 42

        formula = self.inspect_tree(tree, "")

        # logging.info(f"get_nemo_formula_from_infozoom_expresion   formula='{formula}'  expression='{expression}'")
        return formula

    def inspect_tree(self, tree: Tree, formula_part: str) -> str:
        """
        Converts recusively a (sub-) tree depending on the token of the (sub-) tree root
        Depending on the token and the children of the tree the fuction calls itself with a subtree
        Args:
            tree (Tree): The (sub-) tree to convert into a Nemo formula
            formula_part (str): The formula already composed from more upper tree elements.
        """

        # logging.info(f"inspect_tree  data={tree.data}  left={formula_part}")

        match tree.data:
            case  "if_expr":
                formula_part = self.match_if_expr(tree, formula_part)

            case "eq":
                formula_part = self.match_comparison(tree, "==", formula_part)

            case "neq":
                formula_part = self.match_comparison(tree, "!=", formula_part)

            case "lt":
                formula_part = self.match_comparison(tree, "<", formula_part)

            case "le":
                formula_part = self.match_comparison(tree, "<=", formula_part)

            case "gt":
                formula_part = self.match_comparison(tree, ">", formula_part)

            case "ge":
                formula_part = self.match_comparison(tree, ">=", formula_part)

            case "varref":
                formula_part = self.match_varref(tree, formula_part)

            case "mul":
                formula_part = self.match_factor(tree, "*", formula_part)

            case "div":
                formula_part = self.match_factor(tree, "/", formula_part)

            case "add":
                formula_part = self.match_factor(tree, "+", formula_part)

            case "sub":
                formula_part = self.match_factor(tree, "-", formula_part)

            case "neg":
                formula_part = self.match_neg(tree, formula_part)

            case "or_op":
                formula_part = self.match_or_op(tree, formula_part)

            case "and_op":
                formula_part = self.match_and_op(tree, formula_part)

            case "function":
                formula_part = self.match_function(tree, formula_part)
                
            case "args":
                formula_part = self.match_args(tree, formula_part)
                
            case "string": #"ESCAPED_STRING":
                value_str = self.get_token(tree.children[0])
                if value_str == '""' or len(value_str) == 0:
                    value_str = "'NULL'"
                formula_part = formula_part + f'{value_str}'
                # logging.info(f"inspect_tree  <<string>> value_str={value_str}")

            case "number": #"SIGNED_NUMBER":
                value_str = self.get_token(tree.children[0])
                if value_str == "" or len(value_str) == 0:
                    value_str = "'NULL'"
                formula_part = formula_part + f'{value_str}'
                # logging.info(f"inspect_tree  <<number>> value_str={value_str}")

            case _:
                logging.info(f"inspect_tree  UNKNOWN TOKEN={tree.data}")
                raise ValueError(f"FoxFormulaConverter inspect_tree  UNKNOWN TOKEN={tree.data}")

        return formula_part

    def match_if_expr(self, tree: Tree, formula_part: str) -> str:
        try:
            formula_part = formula_part + ' IF ('
            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + ' , '
            formula_part = self.inspect_tree(tree.children[1], formula_part)
            formula_part = formula_part + ' , '
            formula_part = self.inspect_tree(tree.children[2], formula_part)
            formula_part = formula_part + ") "
             # logging.info(f"inspect_tree  <<IF>> left={formula_part}")

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_if_expr: {e}")

        return formula_part


    def match_function(self, tree: Tree, formula_part: str) -> str:
        try:
            token = f"{self.get_token(tree.children[0])}"
            token = token.upper()
            formula_part = formula_part + f"{token} ("

            if tree.children[1]:
                child_args = tree.children[1].children[0]
                formula_part = self.inspect_tree(child_args, formula_part)
  
            formula_part = formula_part + ') '

            if self.foxReaderInfo:
                self.foxReaderInfo.add_issue(IssueType.FUNCTIONCALL, self.attr.attribute_name, None, self.attr.format, extra_info=token)

            #logging.info(f"inspect_tree  <<FUNCTION>> {tree.data} left={formula_part}")

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_function: {e}")

        return formula_part

    def match_args(self, tree: Tree, formula_part: str) -> str:
        try:
            formula_part = self.inspect_tree(tree.children[0], formula_part)

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_args: {e}")

        return formula_part

    def match_comparison(self, tree: Tree, operation: str, formula_part: str) -> str:
        try:
            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + f" {operation} " 
            formula_part = self.inspect_tree(tree.children[1], formula_part)

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_eq: {e}")
        return formula_part
    
    def match_factor(self, tree: Tree, factor: str, formula_part: str) -> str:
        try:
            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + f" {factor} "
            formula_part = self.inspect_tree(tree.children[1], formula_part)

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_factor: {e}")
        return formula_part
    
    def match_neg(self, tree: Tree, formula_part: str) -> str:
        try:
            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + ' -'
            formula_part = self.inspect_tree(tree.children[1], formula_part)
 
        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_neg: {e}")
        return formula_part
    
    def match_and_op(self, tree: Tree, formula_part: str) -> str:
        try:
            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + ' AND '
            formula_part = self.inspect_tree(tree.children[1], formula_part)
 
        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_and_op: {e}")
        return formula_part
    
    def match_or_op(self, tree: Tree, formula_part: str) -> str:
        try:
            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + ' OR '
            formula_part = self.inspect_tree(tree.children[1], formula_part)

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_or_op: {e}")
        return formula_part
    
    def match_varref(self, tree: Tree, formula_part: str) -> str:
        try:
            id_token = int(self.get_token(tree.children[0]))
            # logging.info(f"inspect_tree  <<VAR>> id_token={id_token}")

            referenced_attribute = self.get_referenced_attribute(id_token)
            if referenced_attribute:
                formula_part = formula_part + f"({referenced_attribute.get_nemo_name()})"
            else:
                formula_part = formula_part + f' Attribute with ID={id} not found '

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_varref: {e}")

        return formula_part


    # def convert_expression(self, tree: Tree):
    #     logging.info(f"convert_expression  data={tree.data}")

    #     if tree.data == "if_expr":
    #         yield f' IF '
    #     if tree.data == "eq":
    #         yield f' == '
    #     if tree.data == "varref":
    #         referenced_attribute = self.get_referenced_attribute(3)
    #         if referenced_attribute:
    #             yield f"({referenced_attribute.get_nemo_name()})"
    #         else:
    #             yield f' Attribute with ID={id} not found '

    #     if len(tree.children) == 1 and not isinstance(tree.children[0], Tree):
    #         first_child = tree.children[0]
    #         yield f'\t{first_child}\n'
    #     else:
    #         # yield '\n'
    #         for child in tree.children:
    #             if isinstance(child, Tree):
    #                 yield from tree.convert_expression(child)
    #             else:
    #                 yield f'{child}\n'


    # def _pretty_label(self):
    #     return self.data

    # def _pretty(self, level, indent_str):
    #     yield f'{indent_str*level}{self._pretty_label()}'
    #     if len(self.children) == 1 and not isinstance(self.children[0], Tree):
    #         yield f'\t{self.children[0]}\n'
    #     else:
    #         yield '\n'
    #         for n in self.children:
    #             if isinstance(n, Tree):
    #                 yield from n._pretty(level+1, indent_str)
    #             else:
    #                 yield f'{indent_str*(level+1)}{n}\n'


    # def _pretty(self, tree: Tree):
    #     yield f'{tree.data}'
    #     if len(tree.children) == 1 and not isinstance(tree.children[0], Tree):
    #         yield f'\t{tree.children[0]}\n'
    #     else:
    #         yield '\n'
    #         for child in tree.children:
    #             if isinstance(child, Tree):
    #                 yield from tree._pretty(child)
    #             else:
    #                 yield f'{child}\n'


    def get_token(self, token: str) -> int:
        #Token('ESCAPED_STRING', '"KW 48"')
        #Token('INT', '1')
        #Token('SIGNED_NUMBER', '15921774.63')
        token = token.removeprefix("Token('CNAME',")
        token = token.removeprefix("Token('RULE',")
        token = token.removeprefix("Token('INT',")
        token = token.removeprefix("Token('ESCAPED_STRING',")
        token = token.removeprefix("Token('SIGNED_NUMBER',")
        token = token.removesuffix("')")

        return token

    def get_referenced_attribute(self, id: int) -> FoxAttribute:
        id = int(self.attr.referenced_attributes[id])
        referenced_attribute = next(
            (
                a
                for a in self.all_attributes
                if a.attribute_id == id
            ),
            None,
        )
        return referenced_attribute