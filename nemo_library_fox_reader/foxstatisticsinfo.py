import logging
from enum import Enum, IntEnum


class FOXStatisticsInfo:
    """
    class for storing statistic information about the implementation of InfoZoom features.
    """

    file_name: str = ""
    file_path: str = ""
    fox_version: str = ""
    success: bool = True
    exception: str = ""
    attribute: str = ""
    issue: str = ""
    formula: str = ""
    format: str = ""
    extra_info: str = ""


# IssueType = {
#     1: "EXCEPTION",
#     2: "MULTIPLEVALUES",
#     3: "SUMMARY",
#     4: "EXPRESSION",
#     5: "CLASSIFICATION",
#     6: "CASEDESCRIMINATION", 
#     7: "UNSUPPORTEDFORMAT",
#     8: "IMAGESSHOWN",
#     9: "HTMLLINKSUSED",
#     10: "VALUEFREQUENCYCOLORING",
#     11: "EXPLICITCOLORING",
#     12: "TAGHANDLING",
#     13: "COMMENT",
# }

class IssueType(Enum):
    EXCEPTION = "exception"
    MULTIPLEVALUES = "multiple_values"
    SUMMARY = "summary_function"
    EXPRESSION = "expression"
    CLASSIFICATION = "classification"
    CASEDESCRIMINATION = "case_discrimination"
    FUNCTIONCALL = "function_call"
    AGGREGATIONWITHANALYSISGROUP = "aggregation_with_analysis_group"
    NUMBERSTRINGCONCATENATION = "number_string_concatenation"
    UNSUPPORTEDFORMAT = "unsupported_format"
    IMAGESSHOWN = "images_shown"
    HTMLLINKSUSED = "html_links_used"
    SORTORDERSUSED = "sort_order_used"
    VALUEFREQUENCYCOLORING = "value_frequency_coloring"
    EXPLICITCOLORING = "explicit_coloring"
    TAGHANDLING = "tag_handling"
    COMMENT = "comment"
    OLAPTREEITEMS = "olap_tree_items"
    DATABASEASSISTANTUSED = "database_assistant_used"
    COLUMNNUMBER = "column_number"