import logging
from lark import Lark, UnexpectedInput, Tree

from nemo_library_fox_reader.foxformulaparser import FoxFormulaParser
from nemo_library_fox_reader.foxattribute import FoxAttribute
from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo
from nemo_library_fox_reader.foxstatisticsinfo import IssueType
from nemo_library_fox_reader.foxutils import FOXAttributeType


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


        if not isinstance(tree, Tree):
            formula_part = self.match_args(tree, formula_part)

            value_str = self.get_token(tree)
            if value_str == "" or len(value_str) == 0:
                value_str = "'NULL'"
            formula_delasap = formula_part + f'{value_str}'

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
                
            # case "args":
            case "arg_list":
                formula_part = self.match_args(tree, formula_part)

            case "paren_expr":
                formula_part = self.match_paren_expr(tree, formula_part)

            case "string": #"ESCAPED_STRING":
                value_str = self.get_token(tree.children[0])
                if value_str == '""' or len(value_str) == 0:
                    value_str = "'NULL'"
                if value_str == '"yes"' or value_str == "'yes'":
                    value_str = "'true'"
                if value_str == '"no"' or value_str == "'no'":
                    value_str = "'false'"
                formula_part = formula_part + f'{value_str}'
                self.last_used_string_token = value_str
                # logging.info(f"inspect_tree  <<string>> value_str={value_str}")

            case "number": #"SIGNED_NUMBER":
                value_str = self.get_token(tree.children[0])
                if value_str == "" or len(value_str) == 0:
                    value_str = "'NULL'"
                formula_part = formula_part + f'{value_str}'
                # logging.info(f"inspect_tree  <<number>> value_str={value_str}")

            case "string_singlequoted": #"SINGLEQUOTED_STRING":
                value_str = self.get_token(tree.children[0])
                if value_str == "''" or len(value_str) == 0:
                    value_str = "'NULL'"
                formula_part = formula_part + f'{value_str}'
                self.last_used_string_token = value_str
                
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

    def match_paren_expr(self, tree: Tree, formula_part: str) -> str:
        try:
            formula_part = formula_part + '('
            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + ")"

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_if_expr: {e}")

        return formula_part

    def match_function(self, tree: Tree, formula_part: str) -> str:
        try:
            token = f"{self.get_token(tree.children[0])}"
            token = token.lower()

            # Convert some function names from InfoZoom to Nemo equivalents
            if token == "replacematch":
                token = "regex_replace"
            if token == "matches":
                token = "regex_matches"
            if token == "concatenate":
                token = "concate"
            if token == "len":
                token = "length"

            # logging.info(f"Function call: '{delasap}'=>'{token}'")
    
            without_paratheses = token in ["today","now"]
            if without_paratheses:  
                token = token.upper()

            formula_part = formula_part + f"{token}"

            self.attr.attribute_name = f"<{token}> {self.attr.attribute_name}" #delasap

            if not without_paratheses:
                formula_part = formula_part + ' ('

            if tree.children[1]:
                child_args = tree.children[1].children[0]
                # formula_delasap = self.inspect_tree(child_args, formula_part)

                formula_part = self.inspect_tree(tree.children[1], formula_part)

                # # child_args can be a Tree (with .children) or a plain list/tuple or
                # # even a single node. Normalize to an iterable list of argument nodes
                # # so we can safely calculate the number of items with len(args).
                # if isinstance(child_args, Tree) and hasattr(child_args, 'children'):
                #     args = child_args #.children
                # elif isinstance(child_args, (list, tuple)):
                #     args = child_args
                # else:
                #     args = [child_args]

                # # iterate and append commas correctly (append before each but the first)
                # for i, child_arg in enumerate(args):
                #     if i > 0:
                #         formula_part = " , " + formula_part
                #     formula_part = self.inspect_tree(child_arg, formula_part)

            if not without_paratheses:
                formula_part = formula_part + ') '

            if token == "contains":
                formula_part = f"{formula_part} == 'True'"

            if self.foxReaderInfo:
                self.foxReaderInfo.add_issue(IssueType.FUNCTIONCALL, self.attr.attribute_name, None, self.attr.format, extra_info=token)

            #logging.info(f"inspect_tree  <<FUNCTION>> {tree.data} left={formula_part}")

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_function: {e}")

        return formula_part

    def match_args(self, tree: Tree, formula_part: str) -> str:
        try:
            for i, child in enumerate(tree.children):
                if i > 0:
                    formula_part += " , "
                formula_part = self.inspect_tree(child, formula_part)

            # if tree.children[1]:
            #     for i, child_arg in enumerate(tree.children[1]):
            #         if i > 0:
            #             formula_part = " , " + formula_part
            #         formula_part = self.inspect_tree(child_arg, formula_part)


            # formula_part = self.inspect_tree(tree.children[0], formula_part)

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
            self.last_referenced_attribute = None
            self.last_used_string_token = None

            formula_part = self.inspect_tree(tree.children[0], formula_part)
            formula_part = formula_part + f" {factor} "
            formula_part = self.inspect_tree(tree.children[1], formula_part)

            if self.last_referenced_attribute and self.last_referenced_attribute.nemo_data_type in ['integer','float'] and self.last_used_string_token:
                if self.foxReaderInfo:
                    self.foxReaderInfo.add_issue(IssueType.NUMBERSTRINGCONCATENATION, self.last_referenced_attribute.attribute_name, formula_part, None, extra_info=self.last_used_string_token)


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
                formula_part = formula_part + f"{referenced_attribute.get_nemo_name()}"
            else:
                formula_part = formula_part + f' Attribute with ID={id} not found '

        except Exception as e:
            logging.warning(f"Exception FoxFormulaConverter.match_varref: {e}")

        return formula_part


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

        name_of_attr = self.attr.attribute_name if self.attr else "NOT FOUND"

        while referenced_attribute and referenced_attribute.attribute_type == FOXAttributeType.Link:
            # For link attributes, resolve to the original attribute they point to
            original_attr_index = referenced_attribute.original_attribute_index
            if original_attr_index is not None and original_attr_index < len(self.all_attributes):
                # logging.info(f"Resolving link attribute '{referenced_attribute.attribute_name}' to original attribute index {original_attr_index} {self.attr.attribute_name}")
                name = referenced_attribute.attribute_name
                dataType = referenced_attribute.nemo_data_type
                if referenced_attribute.nemo_data_type not in ["datetime", "date"]:
                    logging.info(f"Recursively resolving link {original_attr_index}   from {name_of_attr} to {referenced_attribute.attribute_name}  {dataType}=>{referenced_attribute.nemo_data_type}")
                    # referenced_attribute = self.all_attributes[original_attr_index]
                    return referenced_attribute
                else:
                    logging.info(f"Not resolving link {original_attr_index}   from {name_of_attr} to {referenced_attribute.attribute_name}  {dataType}=>{referenced_attribute.nemo_data_type}")

        self.last_referenced_attribute = referenced_attribute
        return referenced_attribute