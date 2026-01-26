"""
FOXReader module for reading and importing FOX files into NEMO projects.
"""

import logging
import re
from typing import TypeVar
import attr
import attr
import pandas as pd
from dateutil import parser as dateutil_parser

from nemo_library_fox_reader.foxattribute import FoxAttribute
from nemo_library_fox_reader.foxbinaryreader import FoxBinaryReader
from nemo_library_fox_reader.foxglobal import FoxGlobal
from nemo_library_fox_reader.foxutils import FOXAttributeType

from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo
from nemo_library_fox_reader.foxstatisticsinfo import IssueType

MINIMUM_FOX_VERSION = "FOX2006/11/08"
MAXIMUM_FOX_VERSION = "FOX2025/07/23"


T = TypeVar("T")


class FOXFile:
    """
    Class for reading FOX files and importing their data and metadata into NEMO projects.
    """

    def __init__(self, file_path: str, foxReaderInfo: FOXReaderInfo | None = None):
        """
        Initialize FOXReader with the given file path.
        Args:
            file_path (str): Path to the FOX file to be read.
            foxReaderInfo: Stores statistics information about the implementation of InfoZoom features.
        """
        self.file_path = file_path
        self.file = open(file_path, "rb")
        self.binary_reader = FoxBinaryReader(self.file, foxReaderInfo=foxReaderInfo)
        self.global_information = None
        self.attributes = None
        self.data_frame = pd.DataFrame()
        self.foxReaderInfo = foxReaderInfo
        # logging.info(f"FOXFile __init__ foxReaderInfo={self.foxReaderInfo}")


    def read(self) -> pd.DataFrame:
        """
        Reads the FOX file, parses the header and attributes, and returns a DataFrame with the data.
        Returns:
            pd.DataFrame: DataFrame containing the FOX file data.
        Raises:
            ValueError: If the file format is unsupported or does not use Unicode.
        """
        self.global_information = self._read_global_part_1()

        if self.foxReaderInfo:
            self.foxReaderInfo.current_fox_version = self.global_information.version_short

        self.attributes = self._read_attributes(self.global_information)
        self.global_information = self._read_global_part_2(self.global_information)

        #self._set_values_for_datetime_attributes_depending_on_format(self.attributes)
        logging.info("Removed 2026-01-22: _set_values_for_datetime_attributes_depending_on_format(self.attributes)")

        self.data_frame = self._create_dataframe(
            self.global_information, self.attributes
        )
        return self.data_frame

    def _create_dataframe(
        self, header: FoxGlobal, attributes: list[FoxAttribute]
    ) -> pd.DataFrame:
        """
        Creates a pandas DataFrame from the FOX file attributes.
        Args:
            header (dict): FOX file header information.
            attributes (dict): FOX file attributes.
        Returns:
            pd.DataFrame: DataFrame containing the FOX file data.
        """
        # Create DataFrame from read attributes
        if attributes:
            # Only attributes with values (e.g., not headers or formula attributes)
            columns = []
            values = []
            for attr in attributes:
#                if attr.attribute_type in [FOXAttributeType.Normal, FOXAttributeType.Expression]:
                # if attr.attribute_type not in [FOXAttributeType.Header, FOXAttributeType.Link]:
                if attr.attribute_type not in [FOXAttributeType.Header, FOXAttributeType.Link, FOXAttributeType.Expression]:
                # if attr.attribute_type in [FOXAttributeType.Normal]:
                    columns.append(attr.get_nemo_name())
                    values.append(attr.values)

            # Transpose rows/columns: zip(*values) converts list-of-columns to list-of-rows
            df = pd.DataFrame(data=list(zip(*values)), columns=columns)

            # df_columns = df.columns.tolist()
            # print("C rows =", len(df))
            # print("C cols  =", df.shape[1])
            # print("C shape =", df.shape)
            # print("C columns =", df_columns)
            # print(df.head())


            # data type conversions
            for attr in attributes:
                if attr.attribute_type == FOXAttributeType.Normal:
                # if attr.attribute_type in [FOXAttributeType.Normal, FOXAttributeType.Expression]:
                    try:
                        attribute_nemo_name = attr.get_nemo_name()

                        match attr.nemo_data_type:
                            case "date":
                                # Parse dates. Keep as full timestamps (datetime64) so exports
                                # preserve the time component (previously we converted to python
                                # date objects via .dt.date which caused CSV exports to drop time).
                                df[attribute_nemo_name] = self._parse_datetime_series_fuzzy(
                                    df[attribute_nemo_name],
                                    fmt=attr.nemo_pandas_conversion_format,
                                    date_only=False,
                                )
                                # If you truly want pure date objects later, call .dt.date when exporting
                                delasap1 = df[attribute_nemo_name].to_list()
                                delasap2 = delasap1

                            case "datetime":

                                df[attribute_nemo_name] = self._parse_datetime_series_fuzzy(
                                    df[attribute_nemo_name],
                                    fmt=attr.nemo_pandas_conversion_format,
                                    date_only=False,
                                )
                                delasap1 = df[attribute_nemo_name].to_list()
                                delasap2 = delasap1

                            case "integer" | "float":

                                # remove thousands separator and replace decimal point with a "."
                                if attr.nemo_numeric_separator:
                                    df[attribute_nemo_name] = (
                                        df[attribute_nemo_name  ]
                                        .astype(str)
                                        .str.replace(
                                            attr.nemo_numeric_separator, "", regex=False
                                        )
                                    )
                                if (
                                    attr.nemo_decimal_point
                                    and attr.nemo_decimal_point != "."
                                ):
                                    df[attribute_nemo_name] = (
                                        df[attribute_nemo_name ]
                                        .astype(str)
                                        .str.replace(
                                            attr.nemo_decimal_point, ".", regex=False
                                        )
                                    )
                                df[attribute_nemo_name] = pd.to_numeric(
                                    df[attribute_nemo_name], errors="coerce"
                                ).astype(
                                    "Int64"
                                    if attr.nemo_data_type == "integer"
                                    else "float64"
                                )

                        # if attribute_nemo_name in ["bestelldatum_4_2f1fd773_6caf_455c_b31c_2ef2245f3fc1"]:
                        #     # for val in attr.values:
                        #     #     if val is not None and val != "":
                        #     #         xxx = val
                        #     attr.nemo_data_type = "date"

                    except Exception as e:
                        logging.warning(
                            f"Could not convert attribute '{attr.get_nemo_name()}' to type '{attr.nemo_data_type}'  format={attr.format}  nemo_pandas_conversion_format={attr.nemo_pandas_conversion_format}: {e}"
                        )
                        attr.nemo_data_type = "string"
                        # print("C rows =", len(df))
                        # print("C cols  =", df.shape[1])
                        # print("C shape =", df.shape)
                        # print("C columns =", df.columns.tolist())
                        # print(df.head())
                        pass


        else:
            df = pd.DataFrame()

        return df


    def _set_values_for_datetime_attributes_depending_on_format(self, attributes: list[FoxAttribute]) -> None:
        for attr in attributes:
            if attr.nemo_data_type == "datetime" or attr.nemo_data_type == "date":
                if attr.nemo_pandas_conversion_format:
                    # Convert FOX format string to pandas/strftime format
                    pandas_format = attr.nemo_pandas_conversion_format #self._convert_fox_format_to_strftime(attr.format)
                    has_complete_date_part = self._check_format_has_complete_date_part(pandas_format)
                    has_complete_time_part = self._check_format_has_complete_time_part(pandas_format)

                    if not has_complete_date_part and not has_complete_time_part:
                        attr.nemo_data_type = "string"
                        for i in range(len(attr.values)):
                            if attr.values[i] is not None and attr.values[i] != "":
                                # try:
                                delasap5 = str(attr.values[i])
                                delasap6 = delasap5
                                #     # Convert value to datetime if not already
                                #     if isinstance(attr.values[i], str):
                                #         dt_value = pd.to_datetime(attr.values[i])
                                #     else:
                                #         dt_value = pd.to_datetime(attr.values[i])
                                #     # Format using strftime
                                #     attr.values[i] = dt_value.strftime(pandas_format)
                                # except (ValueError, TypeError, AttributeError):
                                #     # If conversion fails, keep original value
                                #     attr.values[i] = str(attr.values[i])


    def _check_format_has_complete_date_part(self, pandas_format: str) -> bool:
        """
        Check if the given pandas/strftime format string contains complete date components (year, month, day).
        
        Args:
            pandas_format: pandas/strftime format string
        """
        
        if pandas_format is None:
            return False

        # Check for presence of year, month, and day components
        has_year = "%Y" in pandas_format or "%y" in pandas_format
        has_month = "%m" in pandas_format
        has_day = "%d" in pandas_format

        return has_year and has_month and has_day


    def _check_format_has_complete_time_part(self, pandas_format: str) -> bool:

        if pandas_format is None:
            return False

        has_hour = "%H" in pandas_format or "%I" in pandas_format
        has_minute = "%M" in pandas_format

        return has_hour and has_minute


    def _convert_fox_format_to_strftime(self, fox_format: str) -> str | None:
        """
        Convert FOX format string (e.g., "tt.mm.", "%d%m") to Python strftime format.
        
        Args:
            fox_format: Format string from FOX file
            
        Returns:
            strftime format string or None if conversion is not applicable
        """
        if not fox_format:
            return None
        
        # Already a strftime format (contains %), use as-is
        if "%" in fox_format:
            return fox_format
        
        # Convert FOX date/time format to strftime
        pandas_string = fox_format
        
        # Deal with time components
        pandas_string = pandas_string.replace("hhh:", "%H:" if "AM" not in fox_format else "%I")
        pandas_string = pandas_string.replace("hh:", "%H:" if "AM" not in fox_format else "%I")
        pandas_string = pandas_string.replace("h:", "%H:" if "AM" not in fox_format else "%I")
        pandas_string = pandas_string.replace("sss:", "%H:" if "AM" not in fox_format else "%I")
        pandas_string = pandas_string.replace("ss:", "%H:" if "AM" not in fox_format else "%I")
        pandas_string = pandas_string.replace("s:", "%H:" if "AM" not in fox_format else "%I")
        pandas_string = pandas_string.replace(":mmm", ":%M")
        pandas_string = pandas_string.replace(":mm", ":%M")
        pandas_string = pandas_string.replace(":m", ":%M")
        pandas_string = pandas_string.replace(":ss", ":%S")
        pandas_string = pandas_string.replace(":s", ":%S")
        
        # Deal with date components
        pandas_string = pandas_string.replace("jjjj", "%Y")
        pandas_string = pandas_string.replace("yyyy", "%Y")
        pandas_string = pandas_string.replace("aaaa", "%Y")
        pandas_string = pandas_string.replace("jj", "%y")
        pandas_string = pandas_string.replace("yy", "%y")
        pandas_string = pandas_string.replace("aa", "%y")
        pandas_string = pandas_string.replace("mm", "%m")
        pandas_string = pandas_string.replace("dddd", "%d")
        pandas_string = pandas_string.replace("dd", "%d")
        pandas_string = pandas_string.replace("tttt", "%d")
        pandas_string = pandas_string.replace("tt", "%d")
        pandas_string = pandas_string.replace("t", "%d")
        
        # Handle AM/PM
        if "AM" in pandas_string:
            pandas_string = pandas_string.replace("AM", "%p")
        
        return pandas_string if "%" in pandas_string else None


    def _detect_number_format(
        self, attr: FoxAttribute
    ) -> tuple[str, str | None, str | None]:
        """
        Detect number format type, thousand separator, and decimal separator from InfoZoom pattern.

        Args:
            pattern (str): Format string like "#.###,00" or "####."

        Returns:
            tuple: (type, thousands_separator, decimal_separator)
        """
        num_type = "string"
        thousands_sep = None
        decimal_sep = None
        unique_thousands = [" ", "’", "'"]

        if attr.format == "####":
            return ("integer", None, None)

        if attr.format == "####,####":
            return ("float", None, ",")

        # remove + and - first, we don't need it
        pattern_clean = attr.format.replace("+", "").replace("-", "")

        # easiest case: thousands? we just have to find out the decimal point then
        if any(token in pattern_clean for token in unique_thousands):
            thousands_sep = next(
                (token in pattern_clean for token in unique_thousands), None
            )
            if "." in pattern_clean:
                decimal_sep = "."
                num_type = "float"
            elif "," in pattern_clean:
                decimal_sep = ","
                num_type = "float"
            else:
                num_type = "integer"
        else:

            # Expectation: there are #0,. left only. We now have to find out, which of them is the decimal point and which is the separator

            if not (re.match(r"^[#.,0]*$", pattern_clean)):
                logging.warning(
                    f"could not resolve numeric format '{attr.format}' of attribute {attr.attribute_name}"
                )
                return ("string", None, None)

            # we now have 3 cases
            # 1: there is still a thounsand separator and a decimal point
            # 2: there is only one of them
            # 3: there is none of them
            if all(token in pattern_clean for token in [",", "."]):
                # case 1 - both are in the pattern, the first one is the separator, the second one the decimal point
                num_type = "float"
                thousands_sep = (
                    "." if pattern_clean.find(".") < pattern_clean.find(",") else ","
                )
                decimal_sep = "," if thousands_sep == "." else "."
            elif any(token in pattern_clean for token in [",", "."]):
                # case 2 - only one is in. This is the tricky one...
                for token in [",", "."]:
                    if (
                        pattern_clean.endswith(token)
                        or pattern_clean.endswith(f"{token}0")
                        or pattern_clean.endswith(f"{token}#")
                        or pattern_clean.endswith(f"{token}00")
                        or pattern_clean.endswith(f"{token}##")
                    ):
                        decimal_sep = token
                        thousands_sep = "," if token == "." else "."
                        num_type = "float"
                    elif pattern_clean.endswith(f"{token}###"):
                        thousands_sep = token
                        decimal_sep = "," if token == "." else "."
                        num_type = (
                            "float" if decimal_sep in pattern_clean else "integer"
                        )

            else:
                # case 3 - no token is in the string -> we have an integer
                num_type = "integer"

        return (num_type, thousands_sep, decimal_sep)


    def _detect_date_format(self, attr: FoxAttribute) -> tuple[str, str | None]:
        """
        Detect date type and conversion format from InfoZoom pattern.

        Args:
            pattern (str): Format string like "tt.mm.jj"

        Returns:
            tuple: (type, conversion format)
        """
        data_type = "string"
        pandas_string = None


        # IZ supports
        # variants of d/m/y (t,m,j)
        # variants of hh:mm:ss
        # and the combination
        # NEMO supports
        # date and datetime, but not "time"
        # so we have to find out whether we have a datetime (d,m,y PLUS h,m,s) or a date (just d,m,y) or a string (mm:ss) or an integer (yyyy)

        # we have do deal with strange things, e.g. "m" is not unique, can stand for month as well as for minute, s for "Sekunde"...

        # deal with the time first - this removes these issues
        pandas_string = attr.format
        pandas_string = pandas_string.replace(
            "hhh:", "%H:" if not "AM" in attr.format else "%I"
        )
        pandas_string = pandas_string.replace(
            "hh:", "%H:" if not "AM" in attr.format else "%I"
        )
        pandas_string = pandas_string.replace(
            "h:", "%H:" if not "AM" in attr.format else "%I"
        )
        pandas_string = pandas_string.replace(
            "sss:", "%H:" if not "AM" in attr.format else "%I"
        )
        pandas_string = pandas_string.replace(
            "ss:", "%H:" if not "AM" in attr.format else "%I"
        )
        pandas_string = pandas_string.replace(
            "s:", "%H:" if not "AM" in attr.format else "%I"
        )
        pandas_string = pandas_string.replace(":mmm", ":%M") #USI
        pandas_string = pandas_string.replace(":mm", ":%M")
        pandas_string = pandas_string.replace(":m", ":%M")
        pandas_string = pandas_string.replace(":ss", ":%S")
        pandas_string = pandas_string.replace(":s", ":%S")

        # deal with date now
        pandas_string = pandas_string.replace("jjjj", "%Y")
        pandas_string = pandas_string.replace("yyyy", "%Y")
        pandas_string = pandas_string.replace("aaaa", "%Y")
        pandas_string = pandas_string.replace("jj", "%y")
        pandas_string = pandas_string.replace("yy", "%y")
        pandas_string = pandas_string.replace("aa", "%y")
        pandas_string = pandas_string.replace("mm", "%m")
        # pandas_string = pandas_string.replace("m", "%m") #USI
        pandas_string = pandas_string.replace("dd", "%d")
        # USI pandas_string = pandas_string.replace("d", "%d")
        pandas_string = pandas_string.replace("dddd", "%D") #USI
        pandas_string = pandas_string.replace("tttt", "%D") #USI
        pandas_string = pandas_string.replace("tt", "%d")
        pandas_string = pandas_string.replace("t", "%d")

        # logging.info(f"_detect_date_format('{attr.format}') => '{pandas_string}'  {attr.attribute_name}")

        # now guess the data type
        if any(token in pandas_string for token in ["%H", "%I", "%M", "%S"]) and any(
            token in pandas_string for token in ["%Y", "%y", "%m", "%d"]
        ):
            data_type = "datetime"
        elif not any(token in pandas_string for token in ["%H", "%M", "%S"]):
            data_type = "date"
        elif sum(token in pandas_string for token in ["%Y", "%y", "%m", "%d"]):
            data_type = "integer"
            pandas_string = None
        else:
            data_type = "string"
            pandas_string = None

        return (data_type, pandas_string)


    def _convert_date_format_to_hana_date_format(self, attr: FoxAttribute) -> tuple[str, str | None]:
        """
        Detect date type and conversion format from InfoZoom pattern into HANA format.

        Args:
            pattern (str): Format string like "tt.mm.jj"

        Returns:
            tuple: (type, conversion format)
        """
        data_type = "string"
        format_string = None

        if not attr.format:
            return (data_type, format_string)
    
        format_string = attr.format.lower()
        format_string = format_string.replace(
            "hhh:", "HH24:" if not "AM" in attr.format else "HH12:"
        )
        format_string = format_string.replace(
            "hh:", "HH24:" if not "AM" in attr.format else "HH12:"
        )
        format_string = format_string.replace(
            "h:", "HH24:" if not "AM" in attr.format else "HH12:"
        )
        format_string = format_string.replace(
            "sss:", "HH24:" if not "AM" in attr.format else "HH12:"
        )
        format_string = format_string.replace(
            "ss:", "HH24:" if not "AM" in attr.format else "HH12:"
        )
        format_string = format_string.replace(
            "s:", "HH24:" if not "AM" in attr.format else "HH12:"
        )
        format_string = format_string.replace(":mmm", ":MI") #check
        format_string = format_string.replace(":mm", ":MI")
        format_string = format_string.replace(":m", ":MI")
        format_string = format_string.replace(":ss", ":SS")
        format_string = format_string.replace(":s", ":SS")

        # deal with date now
        format_string = format_string.replace("jjjj", "YYYY")
        format_string = format_string.replace("yyyy", "YYYY")
        format_string = format_string.replace("aaaa", "YYYY ")
        format_string = format_string.replace("jj", "YY")
        format_string = format_string.replace("yy", "YY")
        format_string = format_string.replace("aa", "YY")
        format_string = format_string.replace("mm", "MM")
        format_string = format_string.replace("dddd", "D")
        format_string = format_string.replace("ddd", "DAY")
        format_string = format_string.replace("dd", "DD")
        format_string = format_string.replace("tttt", "D")
        format_string = format_string.replace("ttt", "DAY")
        format_string = format_string.replace("tt", "DD")
        format_string = format_string.replace("t", "DD")

        data_type = "string"

        # now guess the data type
        has_year_part = format_string.find("YY") != -1
        has_month_part = format_string.find("MM") != -1
        has_day_part = format_string.find("DD") != -1
        has_hour_part = format_string.find("HH") != -1
        has_minute_part = format_string.find("MI") != -1

        if has_year_part and has_month_part and has_day_part:
            if has_hour_part and has_minute_part:
                data_type = "datetime"
            else:
                data_type = "date"

        logging.info(f"_convert_date_format_to_hana_date_format('{attr.format}') => '{format_string}'  '{data_type}'  '{attr.attribute_name}'")

        return (data_type, format_string)


    def _parse_datetime_series_fuzzy(self, series: pd.Series, fmt: str | None = None, date_only: bool = False) -> pd.Series:
        """Parse a Series to datetimes using a fast vectorized pass with `pd.to_datetime(format=...)` and
        fallback to dateutil.parser.parse(..., fuzzy=True) for entries that remain NaT.

        Generated by CoPilot

        Args:
            series: pd.Series with the raw string values
            fmt: optional pandas strftime format string to try first
            date_only: if True return python dates (.dt.date), else Timestamp
        Returns:
            pd.Series with parsed datetimes (or python date objects if date_only=True)
        """
        # First try fast vectorized parsing with provided format (if any)
        try:
            if fmt:
                parsed = pd.to_datetime(series, errors="coerce", format=fmt)
            else:
                parsed = pd.to_datetime(series, errors="coerce")
        except Exception:
            # if format is invalid or parsing with format failed, try without format
            parsed = pd.to_datetime(series, errors="coerce")

        mask = parsed.isna()
        if mask.any():
            # parse remaining values with dateutil (fuzzy)
            idxs = parsed.index[mask]
            parsed_values = []
            for v in series.loc[idxs].astype(str):
                if v is None or v == "" or v.lower() == "nan":
                    parsed_values.append(pd.NaT)
                    continue
                try:
                    dt = dateutil_parser.parse(v, fuzzy=True)
                    parsed_values.append(pd.Timestamp(dt))
                except Exception:
                    parsed_values.append(pd.NaT)
            parsed.loc[idxs] = parsed_values

        if date_only:
            return parsed.dt.date
        return parsed

    def _read_global_part_1(self) -> FoxGlobal:
        """
        Reads and parses the FOX file header using the binary reader.
        Returns:
            FoxHeader: Parsed header object with all metadata fields.
        Raises:
            ValueError: If the file version is unsupported or not Unicode-based.
        """
        reader = self.binary_reader

        self.isReadingProalpha = False   
        version_full = reader.read_bytes(52).decode("ascii")
        version_short = version_full[38:51]
        if version_short.startswith("="):
            version_short = version_full[39:52]
            self.isReadingProalpha = True
            unused = reader.read_bytes(1)

        if version_short.startswith("="):
            version_short = version_full
            logging.warning('Version string started with character "=" -> removed!')

        if version_short < MINIMUM_FOX_VERSION:
            raise ValueError(
                f"Unsupported FOX version: {version_short} in file {self.file_path}. Minimum required is {MINIMUM_FOX_VERSION}."
            )
        
        if version_short > MAXIMUM_FOX_VERSION:
            raise ValueError(
                f"Unsupported FOX version: {version_short} in file {self.file_path}. Maximum required is {MAXIMUM_FOX_VERSION}."
            )

        global_information = FoxGlobal(
            version_full=version_full,
            version_short=version_short,
        )

        document_format_uses_unicode = reader.read_bool()
        if not document_format_uses_unicode:
            raise ValueError("FOX file does not use Unicode.")

        if version_short >= "FOX2006/10/25":
            global_information.locale_id = reader.read_int()
        if version_short >= "FOX2011/06/17":
            global_information.show_edit_bar = reader.read_bool()
        if version_short >= "FOX2021/05/19":
            global_information.automatic_evaluation = reader.read_bool()
        if version_short >= "FOX2007/06/01":
            global_information.sort_order_stored_in_file = reader.read_bool()
            if global_information.sort_order_stored_in_file:
                reader.read_sorted_characters()

        global_information.table_name = reader.read_CString()
        global_information.left_header = reader.read_CString()
        global_information.center_header = reader.read_CString()
        global_information.right_header = reader.read_CString()
        global_information.left_footer = reader.read_CString()
        global_information.center_footer = reader.read_CString()
        global_information.right_footer = reader.read_CString()

        global_information.left_margin_mm = reader.read_int()
        global_information.top_margin_mm = reader.read_int()
        global_information.right_margin_mm = reader.read_int()
        global_information.bottom_margin_mm = reader.read_int()

        global_information.percent_of_full = reader.read_int()
        global_information.one_page = reader.read_bool()
        global_information.repeat_headers = reader.read_bool()

        global_information.num_records = reader.read_int()
        global_information.num_attributes = reader.read_int()
        global_information.num_diff_attributes = reader.read_int()

        if (
            (global_information.num_records > 1000000000)
            or (global_information.num_attributes > 1000000)
            or (global_information.num_diff_attributes > 1000000)
        ):
            raise ValueError(
                f"Invalid FOX file: too many records ({global_information.num_records}), attributes ({global_information.num_attributes}), or differing attributes ({global_information.num_diff_attributes})"
            )

        global_information.inplace_frame_size_x = reader.read_int()
        global_information.inplace_frame_size_y = reader.read_int()

        global_information.object_name_dative_plural = reader.read_CString()
        global_information.template_path = reader.read_CString()
        if version_short >= "FOX2010/08/12":
            global_information.parent_fox_file = reader.read_CString()

        font_block_size = 92 if document_format_uses_unicode else 60
        reader.read_bytes(font_block_size)
        reader.read_bytes(font_block_size)
        reader.read_bytes(font_block_size)

        if version_short >= "FOX2011/04/21":
            global_information.font_modified_by_user = reader.read_bool()

        global_information.column_header_height = reader.read_int()
        global_information.row_header_width = reader.read_int()
        global_information.grid_width = reader.read_int()

        global_information.use_own_color_scheme = reader.read_bool()
        global_information.color_scheme = reader.read_color_scheme()

        global_information.use_own_reload_settings = reader.read_bool()
        global_information.use_import_names = reader.read_bool()
        global_information.check_formats_at_import = reader.read_bool()
        global_information.append_new_attributes = reader.read_bool()
        global_information.replace_inserted_records = reader.read_bool()
        global_information.delete_old_records = reader.read_bool()
        global_information.append_new_records = reader.read_bool()
        global_information.suppress_compression = reader.read_bool()

        if version_short >= "FOX2009/05/28":
            global_information.hidden = reader.read_bool()
            if version_short >= "FOX2009/10/05":
                global_information.data_hidden = reader.read_bool()
            if version_short >= "FOX2012/06/11":
                global_information.report_menubars_hidden = reader.read_bool()

        global_information.import_date = reader.read_compressed_string()
        global_information.import_time = reader.read_compressed_string()
        global_information.imported_file = reader.read_compressed_string()

        global_information.num_imported_attributes = reader.read_int()
        global_information.database_table_name = reader.read_compressed_string()
        global_information.crystal_report_path = reader.read_compressed_string()
        global_information.cube_name = reader.read_compressed_string()

        global_information.olap_source = reader.read_int()
        global_information.not_empty = reader.read_bool()

        global_information.num_olap_tree_items = reader.read_int()
        if global_information.num_olap_tree_items > 0:
            if self.foxReaderInfo:
                self.foxReaderInfo.add_issue(IssueType.OLAPTREEITEMS)
            else:
                raise NotImplementedError("NoOfOlapTreeItems>0 is not yet supported")

        global_information.default_connect_string = reader.read_compressed_string()

        global_information.store_import_password = reader.read_bool()
        global_information.import_orientation_known = reader.read_bool()
        global_information.import_transposed = reader.read_bool()
        global_information.import_delimiter = reader.read_int()
        global_information.import_quote = reader.read_tchar()

        if version_short >= "FOX2008/08/14":
            global_information.transform_vertical_bars = reader.read_bool()

        global_information.import_with_fixed_column_width = reader.read_bool()
        if global_information.import_with_fixed_column_width:
            if self.foxReaderInfo:
                self.foxReaderInfo.add_issue(IssueType.FIXEDCOLUMNWIDTH)

        global_information.num_columns = reader.read_int()
        if global_information.num_columns > 0:
            if self.foxReaderInfo:
                self.foxReaderInfo.add_issue(IssueType.COLUMNWIDTHUSED)
            for _ in range(global_information.num_columns):
                reader.read_int()  # lColumnWidths (unused)

        global_information.import_attribute_names = reader.read_bool()
        global_information.import_attribute_line = reader.read_int()
        global_information.import_data_line = reader.read_int()

        if version_short >= "FOX2010/05/04":
            global_information.import_max_records = reader.read_int()

        if version_short >= "FOX2018/04/05":
            global_information.import_from_url = reader.read_bool()
            global_information.imported_url = reader.read_CString()
        else:
            global_information.import_from_url = False
            global_information.imported_url = ""

        datasource_unused = reader.read_int()  # lDatasource (unused)

        # read nemo specific data (unused)
        if version_short >= "FOX2025/02/25":
            global_information.nemo_project_name = reader.read_CString()
            global_information.nemo_environment = reader.read_CString()
            global_information.nemo_tenant = reader.read_CString()
            global_information.nemo_is_new_project = reader.read_bool()


        importEncoding_unused = reader.read_int()  # iImportEncoding (unused)
        importCodePage_unused = reader.read_int()  # iImportCodePage (unused)

        if version_short >= "FOX2008/06/04":
            if version_short >= "FOX2010/05/14":
                global_information.serialized_database_join_info = reader.read_CString()
            else:
                global_information.serialized_database_join_info = (
                    reader.read_compressed_string()
                )
            # if global_information.serialized_database_join_info:
            #     if self.foxReaderInfo:
            #         self.foxReaderInfo.add_issue(IssueType.DATABASEASSISTANTUSED, extra_info=f"length serialized_database_join_info={len(global_information.serialized_database_join_info)}")

        if version_short >= "FOX2014/10/08":
            global_information.excel_file = reader.read_compressed_string()
            global_information.excel_table = reader.read_compressed_string()
            global_information.excel_records_in_rows = reader.read_bool()
            global_information.excel_range = reader.read_compressed_string()
            global_information.all_excel_records = reader.read_bool()
            global_information.num_selected_columns = reader.read_int()
            global_information.selected_columns = [
                reader.read_int()
                for _ in range(global_information.num_selected_columns)
            ]

        if version_short >= "FOX2018/04/16":
            global_information.ignore_empty_excel_column_titles = reader.read_bool()
        if version_short >= "FOX2014/11/10":
            global_information.hide_empty_attributes = reader.read_bool()

        global_information.last_report_file = reader.read_compressed_string()
        global_information.last_report_directory = reader.read_compressed_string()
        global_information.default_report_directory = reader.read_compressed_string()
        global_information.default_data_source_directory = (
            reader.read_compressed_string()
        )

        global_information.max_report_index = reader.read_int()
        global_information.num_formats = reader.read_int()
        global_information.formats = [
            reader.read_CString() for _ in range(global_information.num_formats)
        ]

        global_information.allow_reload = reader.read_bool()
        global_information.allow_insert_file = reader.read_bool()
        global_information.allow_insert_database = reader.read_bool()
        if version_short >= "FOX2017/07/24":
            global_information.allow_change_connection_data = reader.read_bool()
        else:
            global_information.allow_change_connection_data = (
                global_information.allow_insert_database
            )

        global_information.allow_join = reader.read_bool()
        global_information.allow_link = reader.read_bool()
        global_information.allow_print = reader.read_bool()
        global_information.allow_save_fox = reader.read_bool()
        global_information.allow_save_objects_as_fox = reader.read_bool()
        global_information.allow_save_as_text = reader.read_bool()
        global_information.allow_move_attributes = reader.read_bool()
        global_information.allow_change_format = reader.read_bool()
        global_information.allow_edit_values = reader.read_bool()
        global_information.allow_insert_objects = reader.read_bool()
        global_information.allow_insert_attributes = reader.read_bool()
        global_information.allow_delete_attributes = reader.read_bool()
        global_information.allow_insert_link = reader.read_bool()
        global_information.allow_create_queries = reader.read_bool()
        global_information.allow_define_color_coding = reader.read_bool()
        global_information.allow_determine_column_width = reader.read_bool()
        global_information.allow_insert_derived_attributes = reader.read_bool()
        global_information.allow_redefine_derived_attributes = reader.read_bool()
        if version_short >= "FOX2012/06/01":
            global_information.obsolete_flag = reader.read_bool()
        global_information.allow_define_cube = reader.read_bool()
        global_information.allow_create_diagram = reader.read_bool()
        global_information.allow_create_report = reader.read_bool()
        global_information.allow_create_crystal_reports = reader.read_bool()
        global_information.allow_excel_export_wizard = reader.read_bool()
        if version_short >= "FOX2016/09/26":
            global_information.allow_list_label_designer = reader.read_bool()
            global_information.allow_list_label_print = reader.read_bool()
        global_information.allow_advanced_report_designer = reader.read_bool()
        global_information.allow_advanced_report_print = reader.read_bool()
        if version_short >= "FOX2010/06/16":
            global_information.allow_edit_comment = reader.read_bool()

        global_information.encoded_table_password = reader.read_double()
        global_information.encoded_protection_password = reader.read_double()
        global_information.password_protect_table = reader.read_bool()
        global_information.password_protect_dialog = reader.read_bool()

        return global_information

    def _read_attributes(self, global_information: FoxGlobal) -> list[FoxAttribute]:
        """
        Reads and parses all attribute metadata and values from the FOX file.
        Args:
            header (FoxHeader): Parsed FOX header.
        Returns:
            dict[int, FoxAttribute]: Dictionary of attributes keyed by attribute ID.
        """
        attributes = []
        reader = self.binary_reader

        for _ in range(global_information.num_attributes):
            attribute_name = reader.read_CString()

            display_children = False
            if len(attribute_name) > 1 and attribute_name[0] == "+":
                attribute_name = attribute_name[
                    1:
                ]  # remove leading '+' - in attribute groups an indicator that the group is expanded
                display_children = True

            attribute_id = reader.read_int()
            attr = FoxAttribute(
                attribute_name=attribute_name,
                attribute_id=attribute_id,
            )

            attr.level = reader.read_int()

            if global_information.version_short >= "FOX2011/05/26":
                attr.uuid = reader.read_CString()

            # logging.info(f"Reading attribute: {attribute_id} {attr.uuid} {attribute_name}")

            if global_information.version_short >= "FOX2012/08/08":
                attr.number_of_tags = reader.read_int()
                if attr.number_of_tags > 0:
                    attr.nemo_not_supported = True
                    if self.foxReaderInfo:
                        self.foxReaderInfo.add_issue(IssueType.TAGHANDLING, attr.attribute_name, attr.expression_string, attr.format)
                    else:
                        raise NotImplementedError("Tag handling not implemented yet")
            elif global_information.version_short >= "FOX2012/05/08":
                    attr.nemo_not_supported = True
                    if self.foxReaderInfo:
                        self.foxReaderInfo.add_issue(IssueType.TAGHANDLING, attr.attribute_name, attr.expression_string, attr.format)
                    else: 
                        raise NotImplementedError("Tag handling not implemented yet")

            if global_information.version_short >= "FOX2008/01/31":
                attr.comment = reader.read_compressed_string()
                if len(attr.comment) > 0:
                    if self.foxReaderInfo:
                        self.foxReaderInfo.add_issue(IssueType.COMMENT, attr.attribute_name, attr.expression_string, attr.format, extra_info=attr.comment)

            attr.format = reader.read_compressed_string()
            attr.unclear_format = reader.read_int()
            attr.maximum = reader.read_double()
            attr.minimum = reader.read_double()
            attr.differing = reader.read_int()
            attr.shown = reader.read_bool()
            attr.multiple_values = reader.read_bool()
            attr.max_multiple_values = reader.read_int()
            attr.is_header = reader.read_bool()
            if attr.is_header:
                attr.display_children = display_children
                # logging.info(f"Attribute '{attr.attribute_name}' is a header attribute. Display children: {attr.display_children} ")

            attr.is_link = reader.read_bool()

            attr.can_drill_down = reader.read_bool()
            attr.chosen_hierarchy_index = reader.read_int()

            attr.olap_hierarchy = reader.read_compressed_string()
            attr.drilldown_attribute_index = reader.read_int()
            attr.olap_measure_aggregate = reader.read_int()
            attr.defining_column_width = reader.read_bool()
            attr.defining_all_colors = reader.read_bool()

            if global_information.version_short >= "FOX2012/05/09":
                attr.defining_group_colors = reader.read_bool()

            attr.absolute_average = reader.read_double()
            attr.low_mark = reader.read_double()
            attr.high_mark = reader.read_double()
            attr.yellow_percent = reader.read_int()

            for _ in range(9):
                reader.read_bytes(4)  # ignore 9 colors

            attr.dynamic_color_ranges = reader.read_bool()
            attr.color_threshold = reader.read_double()
            attr.ampel_color_coding = reader.read_bool()
            if attr.ampel_color_coding:
                if self.foxReaderInfo:
                    self.foxReaderInfo.add_issue(IssueType.AMPELCOLORCODING, attr.attribute_name, "", "")

            attr.bold_values = reader.read_bool()
            attr.suppress_compression = reader.read_bool()
            attr.constant_low_high_mark = reader.read_bool()

            if global_information.version_short >= "FOX2019/07/25":
                attr.individual_background_color = reader.read_bool()
                reader.read_bytes(4)  # ignore 4 bytes
            if self.isReadingProalpha:
                reader.read_int()  # ignore 4 bytes

            try:
                attr.attribute_type = FOXAttributeType(reader.read_int())
            except ValueError as e:
                raise ValueError(f"Unsupported attribute type in FOX file: {e}")
            attr.formula_returns_string = reader.read_bool()
            attr.summary = reader.read_bool()
            if attr.summary:
                attr.attribute_type = FOXAttributeType.Summary
            attr.combination = reader.read_bool()
            attr.expression = reader.read_bool()
            if attr.expression and attr.attribute_type != FOXAttributeType.Link:
                attr.attribute_type = FOXAttributeType.Expression

            if global_information.version_short >= "FOX2008/01/09":
                attr.use_unique_value = reader.read_bool()

            attr.regard_undefined_as_zero = reader.read_bool()
            attr.eval_mode = reader.read_int()
            attr.outdated = reader.read_bool()
            attr.invert_direction = reader.read_bool()
            attr.dynamic = reader.read_bool()

            if attr.attribute_type == FOXAttributeType.Summary or attr.combination:
                attr.attribute1_index = reader.read_int()
                attr.attribute2_index = reader.read_int()
                attr.function = reader.read_int()
                attr.marginal_value = reader.read_int()
                attr.combined_format = reader.read_compressed_string()

            if attr.attribute_type == FOXAttributeType.Expression:
                attr.expression_string = reader.read_compressed_string()
                attr.num_referenced_attributes = reader.read_int()
                for _ in range(attr.num_referenced_attributes):
                    referencedAttribute = reader.read_CString()
                    attr.referenced_attributes.append(referencedAttribute)
                    # logging.info(f"FOXAttributeType.Expression attr={attr.attribute_name}  referencedAttribute={referencedAttribute}")

            if attr.attribute_type == FOXAttributeType.Link:
                attr.original_attribute_index = reader.read_int()
                if global_information.version_short >= "FOX2016/01/19":
                    attr.drilldown_group_index = reader.read_int()

            if attr.attribute_type == FOXAttributeType.Classification:
                attr.classified_attribute_index = reader.read_int()
                attr.num_value_ranges = reader.read_int()
                expression = ""
                # collect per-range metadata so we can later build a nested IF expression
                for i in range(attr.num_value_ranges):
                    op = reader.read_int()
                    threshold = reader.read_double()
                    s_threshold = reader.read_CString()
                    s_class_value = reader.read_CString()

                    if global_information.version_short >= "FOX2010/05/17":
                        unmodified = reader.read_bool() #.read_int()
                    num_values = reader.read_int()
                    
                    # logging.info(f"Classification attribute detected '{attr.attribute_name}': range {i}, op={op}, threshold={threshold}, s_threshold={s_threshold}, s_class_value={s_class_value}, num_values={num_values}")

                    # read the optional discrete values for IN operator and also store
                    values_list = []
                    for j in range(num_values):
                        key = f"range_{i}_value_{j}"
                        val = reader.read_compressed_value(attr.attribute_name, [], "", 1)
                        attr.classification_values[key] = val
                        values_list.append(val)

                    # store range metadata for later expression building
                    attr.classification_ranges.append({
                        "op": op,
                        "threshold": threshold,
                        "s_threshold": s_threshold,
                        "s_class_value": s_class_value,
                        "values": values_list,
                    })
                attr.user_defined_order = reader.read_bool()

            if attr.attribute_type == FOXAttributeType.CaseDiscrimination:
                attr.num_cases = reader.read_int()
                for i in range(attr.num_cases):
                    cond = reader.read_CString()
                    class_val = reader.read_CString()
                    if global_information.version_short >= "FOX2021/07/07":
                        if reader.read_int():
                            class_attr_index = reader.read_int()
                            attr.cases[f"case_{i}_class_attribute_index"] = (
                                class_attr_index
                            )
                    num_refs = reader.read_int()
                    refs = [reader.read_CString() for _ in range(num_refs)]
                    attr.cases[f"case_{i}"] = {
                        "condition": cond,
                        "class_value": class_val,
                        "refattrs": refs,
                    }
                attr.user_defined_order = bool(reader.read_int())

            if global_information.version_short >= "FOX2007/09/11":
                attr.type_error = reader.read_bool()
                attr.syntax_error = reader.read_bool()

            attr.import_name = reader.read_compressed_string()
            attr.import_index = reader.read_int()
            attr.extra_import_format = reader.read_bool()
            attr.import_format_string = reader.read_CString()
            attr.report_index = reader.read_int()

            attr.allow_edit = reader.read_int()
            attr.allow_rename = reader.read_int()
            attr.allow_delete = reader.read_int()
            attr.allow_change_format = reader.read_int()
            attr.allow_move = reader.read_int()
            attr.allow_redefine = reader.read_int()

            if global_information.version_short >= "FOX2012/06/12":
                attr.allow_inspect_definition = reader.read_int()

            attr.permanently_hidden = reader.read_int()

            if attr.permanently_hidden != 0 or not attr.shown:
                if self.foxReaderInfo:
                    if attr.permanently_hidden != 0:
                        self.foxReaderInfo.list_of_ids_permanently_hidden_columns.append(attr.uuid)
                        self.foxReaderInfo.add_issue(IssueType.ATTRIBUTEPERMANENTLYHIDDEN, attr.attribute_name, "", attr.format, extra_info="")
                        logging.info(f"Attribute '{attr.attribute_name}' is permanently hidden.")
                    else:
                        self.foxReaderInfo.add_issue(IssueType.ATTRIBUTETEMPORARYHIDDEN, attr.attribute_name, "", attr.format, extra_info="")
                        logging.info(f"Attribute '{attr.attribute_name}' is temporary hidden.")


            if global_information.version_short >= "FOX2010/06/16":
                attr.allow_edit_comment = reader.read_int()

            attr.coupled = reader.read_bool()
            if attr.coupled:
                # logging.info(f"Attribute '{attr.attribute_name}' '{attr.uuid}' is coupled.")
                attr.coupling_extras = [reader.read_int() for _ in range(4)]

            # Read values if applicable
            if attr.attribute_type != FOXAttributeType.Header:
                attr.num_values = reader.read_int()
                if attr.num_values > 500000000:
                    raise ValueError(
                        f"Invalid FOX file: too many values ({attr.num_values}) for attribute {attr.attribute_name}"
                    )
                if attr.num_values <= 255:
                    bytes_per_index = 1
                elif attr.num_values <= 32767:
                    bytes_per_index = 2
                else:
                    bytes_per_index = 4

                attr.index_of_first_multiple_value = reader.read_int()
                value_store = []
                last_value = ""
                for _ in range(attr.num_values):
                    # if _ % 100000 == 0:
                    #     logging.info(
                    #         f"Reading values for attribute {attr.attribute_name} ({_:,}/{attr.num_values:,})"
                    #     )
                    last_value = reader.read_compressed_value(
                        attr.attribute_name, value_store, last_value, bytes_per_index
                    )
                    value_store.append(last_value)

                num_records = global_information.num_records
                raw_index_array = reader.read_bytes(num_records * bytes_per_index)
                index_array = reader.unpack_n_byte_values(
                    raw_index_array, bytes_per_index
                )

                attr.values = [value_store[i] for i in index_array]
                for i in range(len(attr.values)):
                    if len(attr.values[i]) > 500:
                        logging.warning(f"Value too long in attribute '{attr.attribute_name}': '{attr.values[i][:100]}...{attr.values[i][-100:]}'")
                        attr.values[i] = attr.values[i][:500]

                attr.value_frequency_coloring = reader.read_bool()
                if attr.value_frequency_coloring:
                    if self.foxReaderInfo:
                        self.foxReaderInfo.add_issue(IssueType.VALUEFREQUENCYCOLORING, attr.attribute_name, "", attr.format)
                    else:
                        raise NotImplementedError("Value frequency coloring not implemented")
                    nr_of_frequency_values = reader.read_int()
                    for _ in range(nr_of_frequency_values):
                        freq_value = reader.read_compressed_value(
                            attr.attribute_name, [], "", 1
                        )

                attr.explicit_colors = reader.read_bool()
                if attr.explicit_colors:
                    num_colors = reader.read_int()
                    for _ in range(num_colors):
                        value = reader.read_CString()
                        color = reader.read_bytes(4)  # ignore color

                    if self.foxReaderInfo:
                        self.foxReaderInfo.add_issue(IssueType.EXPLICITCOLORING, attr.attribute_name, "", attr.format)
                    else:
                       raise NotImplementedError("Explicit coloring not implemented")

            # guess data conversion information
            if attr.attribute_type == FOXAttributeType.Normal:
            # if attr.attribute_type != FOXAttributeType.Header:
                self._guess_data_conversion(attr)
                # logging.info(f"Regular attribute '{attr.format}'   '{attr.nemo_data_type}'  '{attr.get_nemo_name()}'")

            if attr.attribute_type == FOXAttributeType.Expression:
                self._guess_data_conversion(attr)
                # logging.info(f"Expression attribute '{attr.format}'   '{attr.nemo_data_type}'  '{attr.get_nemo_name()}'")

            if attr.attribute_type == FOXAttributeType.CaseDiscrimination:
                self._guess_data_conversion(attr)
                # logging.info(f"CaseDiscrimination attribute '{attr.format}'  '{attr.nemo_data_type}'  '{attr.get_nemo_name()}'")

            if attr.attribute_type == FOXAttributeType.Classification:
                self._guess_data_conversion(attr)
                # logging.info(f"Classification attribute '{attr.format}'   '{attr.nemo_data_type}'  '{attr.get_nemo_name()}'")

            if attr.attribute_type == FOXAttributeType.Summary:
                # self._guess_data_conversion(attr) this call results in 0 records :-(
                attr.nemo_data_type = "string"
                # logging.info(f"Summary attribute '{attr.attribute_name}'  '{attr.format}'   '{attr.nemo_data_type}'")

            attributes.append(attr)

        if self.foxReaderInfo is not None:
            self.foxReaderInfo.coupled_attributes_in_fox_file = []

        if self.global_information.version_short >= "FOX2004/05/19":
            num_coupled_groups = reader.read_int()
            logging.info(f"Number of coupled attribute groups: {num_coupled_groups}")
            # if self.foxReaderInfo is not None:
            #     self.foxReaderInfo.couple_attributes_in_fox_file.

            for _ in range(num_coupled_groups):
                attributes_in_coupled_group = []
                last_attribute_leads = reader.read_bool()
                sorted_reverse = reader.read_bool()
                num_attributes_in_group = reader.read_int()

                for _ in range(num_attributes_in_group):
                    is_part_of_query = reader.read_bool()
                    attribute_index_in_file = reader.read_int()
                    if attribute_index_in_file != 4294967295 and not is_part_of_query:
                        try:
                            attr = attributes[attribute_index_in_file]
                            if attr is not None:
                                attributes_in_coupled_group.append(attr)
                        except IndexError:
                            pass

                if self.foxReaderInfo is not None and len(attributes_in_coupled_group) > 1:
                    self.foxReaderInfo.coupled_attributes_in_fox_file.append(attributes_in_coupled_group)
                    logging.info(f"Coupled attribute group: {[a for a in attributes_in_coupled_group]}, last_attribute_leads={last_attribute_leads}, sorted_reverse={sorted_reverse}")

        # ignore id of attribute and overwrite it with the index
        for idx, attr in enumerate(attributes):
            attr.attribute_id = idx

        return attributes


    def _read_global_part_2(self, global_information: FoxGlobal) -> FoxGlobal:
        """
        Reads the second part of the FOX file global information.
        Args:
            header (FoxGlobal): The first part of the global information.
        Returns:
            FoxGlobal: The updated global information with additional fields.
        """
        reader = self.binary_reader

        global_information.num_coupled_attributes = reader.read_int()

        return global_information

    def _guess_data_conversion(self, attr: FoxAttribute) -> None:
        # guess data type and unit
        # clean up the format string
        format_clean = attr.format.strip().lower()

        # detect any special characters that might indicate a specific unit for nemo
        if any(currency in format_clean for currency in ["€", "$"]):
            attr.nemo_unit = "currency"
            format_clean = format_clean.replace("€", "").replace("$", "")
        elif "%" in format_clean:
            attr.nemo_unit = "percent"
            format_clean = format_clean.replace("%", "")

        # detect data type and conversion information from format. It's quite tricky...
        if "#" in format_clean:
            # "#" in format? we have a number
            (
                attr.nemo_data_type,
                attr.nemo_numeric_separator,
                attr.nemo_decimal_point,
            ) = self._detect_number_format(attr)

        elif any(token in format_clean for token in ["d", "m", "y", "t", "s", "h"]):
            # we have a candidate for a date(time)
            hana_data_type, attr.hana_conversion_format = (
                self._convert_date_format_to_hana_date_format(attr)
            )
            
            attr.nemo_data_type, attr.nemo_pandas_conversion_format = (
                self._detect_date_format(attr)
            )

        # logging.info(
        #     f"attribute: {attr.attribute_name} with format '{attr.format}'. Guessed data type: '{attr.nemo_data_type}', unit: '{attr.nemo_unit}', numsep: '{attr.nemo_numeric_separator}', decp: '{attr.nemo_decimal_point}', pandas conversion: '{attr.nemo_pandas_conversion_format}'"
        # )

    def close(self):
        """
        Closes the FOX file stream.
        """
        self.file.close()
