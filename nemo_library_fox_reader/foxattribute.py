from dataclasses import asdict, dataclass, field
from typing import Optional, List, Dict, Any

from nemo_library.utils.utils import (
    get_internal_name
)


@dataclass
class FoxAttribute:
    # Core metadata
    attribute_name: str
    attribute_id: int
    level: int = 0
    parent_index: Optional[int] = None
    uuid: Optional[str] = None

    # Optional tag handling (unimplemented)
    number_of_tags: Optional[int] = None

    # Additional metadata
    comment: Optional[str] = None
    format: str = ""
    unclear_format: int = 0
    maximum: float = 0.0
    minimum: float = 0.0
    differing: int = 0
    shown: bool = False
    multiple_values: bool = False
    max_multiple_values: int = 0
    is_header: bool = False
    is_link: bool = False

    # Drill-down / hierarchy support
    can_drill_down: Optional[bool] = None
    chosen_hierarchy_index: Optional[int] = None
    olap_hierarchy: str = ""
    drilldown_attribute_index: int = 0
    olap_measure_aggregate: int = 0

    # Visual formatting
    defining_column_width: bool = False
    defining_all_colors: bool = False
    defining_group_colors: Optional[bool] = None
    absolute_average: float = 0.0
    low_mark: float = 0.0
    high_mark: float = 0.0
    yellow_percent: int = 0

    # Dynamic color / formatting
    dynamic_color_ranges: bool = False
    color_threshold: float = 0.0
    ampel_color_coding: bool = False
    bold_values: bool = False
    suppress_compression: bool = False
    constant_low_high_mark: bool = False
    individual_background_color: Optional[bool] = None

    # Typing / evaluation
    attribute_type: int = 0
    formula_returns_string: bool = False
    summary: bool = False
    combination: bool = False
    expression: bool = False
    use_unique_value: Optional[bool] = None
    regard_undefined_as_zero: bool = False
    eval_mode: int = 0
    outdated: bool = False
    invert_direction: bool = False
    dynamic: bool = False

    # Combined attributes
    attribute1_index: Optional[int] = None
    attribute2_index: Optional[int] = None
    function: Optional[int] = None
    marginal_value: Optional[int] = None
    combined_format: Optional[str] = None

    # Expression attribute
    expression_string: Optional[str] = None
    num_referenced_attributes: Optional[int] = None
    referenced_attributes: List[str] = field(default_factory=list)

    # Link attribute
    original_attribute_index: Optional[int] = None
    drilldown_group_index: Optional[int] = None

    # Classification attribute
    classified_attribute_index: Optional[int] = None
    num_value_ranges: Optional[int] = None
    # Detailed per-range metadata for classification attributes
    classification_ranges: List[Dict[str, Any]] = field(default_factory=list)
    classification_values: Dict[str, Any] = field(default_factory=dict)
    user_defined_order: Optional[bool] = None

    # Case discrimination
    num_cases: Optional[int] = None
    cases: Dict[str, Any] = field(default_factory=dict)

    # Validation flags
    type_error: Optional[bool] = None
    syntax_error: Optional[bool] = None

    # Import metadata
    import_name: str = ""
    import_index: int = 0
    extra_import_format: bool = False
    import_format_string: str = ""
    report_index: int = 0

    allow_edit: int = 0
    allow_rename: int = 0
    allow_delete: int = 0
    allow_change_format: int = 0
    allow_move: int = 0
    allow_redefine: int = 0
    allow_inspect_definition: Optional[int] = None
    permanently_hidden: int = 0
    allow_edit_comment: Optional[int] = None

    # Coupling
    coupled: bool = False
    coupling_extras: List[int] = field(default_factory=list)

    # Value data
    num_values: Optional[int] = None
    index_of_first_multiple_value: Optional[int] = None
    values: Optional[List[str]] = None
    value_frequency_coloring: Optional[bool] = None
    explicit_colors: Optional[bool] = None

    # Internal extensions / dynamic keys
    extra_fields: Dict[str, Any] = field(default_factory=dict)

    # additonal fields, not part of the file
    nemo_data_type: str = "string"
    nemo_pandas_conversion_format: str = ""
    nemo_decimal_point: str = ""
    nemo_numeric_separator: str = ""
    nemo_unit: str = ""
    nemo_not_supported: bool = False

    def to_dict(self):
        """
        Converts the Diagram instance to a dictionary.

        Returns:
            dict: A dictionary representation of the Diagram instance.
        """
        return asdict(self)

    def get_nemo_name(self) -> str:
        """
        Returns the Nemo name for the attribute, which is a sanitized version of the display name.

        Returns:
            str: The sanitized Nemo name.
        """
        return get_internal_name(
            f"{self.attribute_name}_{self.attribute_id}_{self.uuid}"
        )
