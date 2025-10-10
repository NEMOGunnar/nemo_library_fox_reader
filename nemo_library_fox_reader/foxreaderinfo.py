import logging
from nemo_library_fox_reader.foxstatisticsinfo import FOXStatisticsInfo, IssueType

class FOXReaderInfo:
    """
    class for storing statistics information about the implementation of InfoZoom features.
    """

    current_file_name: str = ""
    current_file_path: str = ""
    current_fox_version: str = ""

    statistics_infos: list[FOXStatisticsInfo] = []

    def add_exception(self, exception: Exception):
        info = FOXStatisticsInfo()
        info.issue = "EXCEPTION"
        info.exception = exception
        info.file_name = self.current_file_name
        info.file_path = self.current_file_path.replace("\\", "\\\\")
        info.fox_version = self.current_fox_version
        self.statistics_infos.append(info)


    def add_issue(self, issue: str, attribute: str, formula: str = "", format: str = "", extra_info: str=""):
        info = FOXStatisticsInfo()
        info.issue = f"{issue}" # IssueType.get(issue, None)
        info.issue = info.issue.replace("IssueType.", "")
        info.formula = formula
        info.attribute = attribute
        info.format = format
        info.extra_info = extra_info
        info.file_name = self.current_file_name
        info.file_path = self.current_file_path.replace("\\", "\\\\")
        info.fox_version = self.current_fox_version
        self.statistics_infos.append(info)


    attributes_with_multiple_values: list[str] = []
    attributes_with_summary_function: list[str] = []
    attributes_with_expression: list[str] = []
    attributes_with_classification: list[str] = []
    attributes_with_case_discrimination: list[str] = []
    attributes_with_unsupported_format: list[str] = []
    attributes_with_images_shown: list[str] = []
    attributes_with_sort_order_used: list[str] = []
    attributes_with_html_links_used: list[str] = []
    