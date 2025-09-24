from dataclasses import asdict, dataclass, field
from typing import Optional, List, Dict

@dataclass
class FoxGlobal:
    # Version information
    version_full: str
    version_short: str

    # Locale and version-specific flags
    document_format_uses_unicode: bool = True
    locale_id: Optional[int] = None
    show_edit_bar: Optional[bool] = None
    automatic_evaluation: Optional[bool] = None

    # Sorting configuration
    sort_order_stored_in_file: Optional[bool] = None

    # Page header/footer content
    table_name: str = ""
    left_header: str = ""
    center_header: str = ""
    right_header: str = ""
    left_footer: str = ""
    center_footer: str = ""
    right_footer: str = ""

    # Page margins
    top_margin_mm: int = 0
    left_margin_mm: int = 0
    right_margin_mm: int = 0
    bottom_margin_mm: int = 0

    # Layout options
    percent_of_full: int = 0
    one_page: bool = False
    repeat_headers: bool = False

    # Attribute and record counts
    num_records: int = 0
    num_attributes: int = 0
    num_diff_attributes: int = 0

    # Embedded frame dimensions
    inplace_frame_size_x: int = 0
    inplace_frame_size_y: int = 0

    # Metadata
    object_name_dative_plural: str = ""
    template_path: str = ""
    parent_fox_file: Optional[str] = None

    # Font block skipping
    font_blocks_skipped: int = 3

    # Layout details
    font_modified_by_user: Optional[bool] = None
    column_header_height: int = 0
    row_header_width: int = 0
    grid_width: int = 0

    # Color scheme
    use_own_color_scheme: bool = False
    color_scheme: Dict[str, object] = field(default_factory=dict)

    # Import and reload flags
    use_own_reload_settings: bool = False
    use_import_names: bool = False
    check_formats_at_import: bool = False
    append_new_attributes: bool = False
    replace_inserted_records: bool = False
    delete_old_records: bool = False
    append_new_records: bool = False
    suppress_compression: bool = False

    # Visibility flags
    hidden: Optional[bool] = None
    data_hidden: Optional[bool] = None
    report_menubars_hidden: Optional[bool] = None

    # Import metadata
    import_date: str = ""
    import_time: str = ""
    imported_file: str = ""

    # Import attribute info
    num_imported_attributes: int = 0
    database_table_name: str = ""
    crystal_report_path: str = ""
    cube_name: str = ""
    olap_source: int = 0
    not_empty: bool = False

    # OLAP tree information
    num_olap_tree_items: int = 0
    default_connect_string: str = ""

    # Import behavior settings
    store_import_password: bool = False
    import_orientation_known: bool = False
    import_transposed: bool = False

    import_delimiter: int = 0
    import_quote: str = ""

    transform_vertical_bars: Optional[bool] = None
    import_with_fixed_column_width: bool = False

    # Column definitions
    num_columns: int = 0
    import_attribute_names: bool = False
    import_attribute_line: int = 0
    import_data_line: int = 0
    import_max_records: Optional[int] = None

    # Excel import options
    import_from_url: bool = False
    imported_url: str = ""
    serialized_database_join_info: Optional[str] = None

    excel_file: Optional[str] = None
    excel_table: Optional[str] = None
    excel_records_in_rows: Optional[bool] = None
    excel_range: Optional[str] = None
    all_excel_records: Optional[bool] = None
    num_selected_columns: Optional[int] = None
    selected_columns: Optional[List[int]] = None
    ignore_empty_excel_column_titles: Optional[bool] = None
    hide_empty_attributes: Optional[bool] = None

    # Report paths
    last_report_file: str = ""
    last_report_directory: str = ""
    default_report_directory: str = ""
    default_data_source_directory: str = ""

    max_report_index: int = 0
    num_formats: int = 0
    formats: List[str] = field(default_factory=list)

    # Permission flags
    allow_reload: bool = False
    allow_insert_file: bool = False
    allow_insert_database: bool = False
    allow_change_connection_data: bool = False
    allow_join: bool = False
    allow_link: bool = False
    allow_print: bool = False
    allow_save_fox: bool = False
    allow_save_objects_as_fox: bool = False
    allow_save_as_text: bool = False
    allow_move_attributes: bool = False
    allow_change_format: bool = False
    allow_edit_values: bool = False
    allow_insert_objects: bool = False
    allow_insert_attributes: bool = False
    allow_delete_attributes: bool = False
    allow_insert_link: bool = False
    allow_create_queries: bool = False
    allow_define_color_coding: bool = False
    allow_determine_column_width: bool = False
    allow_insert_derived_attributes: bool = False
    allow_redefine_derived_attributes: bool = False
    obsolete_flag: Optional[bool] = None
    allow_define_cube: bool = False
    allow_create_diagram: bool = False
    allow_create_report: bool = False
    allow_create_crystal_reports: bool = False
    allow_excel_export_wizard: bool = False
    allow_list_label_designer: Optional[bool] = None
    allow_list_label_print: Optional[bool] = None
    allow_advanced_report_designer: bool = False
    allow_advanced_report_print: bool = False
    allow_edit_comment: Optional[bool] = None

    # Passwords and protection
    encoded_table_password: float = 0.0
    encoded_protection_password: float = 0.0
    password_protect_table: bool = False
    password_protect_dialog: bool = False
    
    # coupled attributes
    num_coupled_attributes: Optional[int] = None
    
    
    def to_dict(self):
        """
        Converts the Diagram instance to a dictionary.

        Returns:
            dict: A dictionary representation of the Diagram instance.
        """
        return asdict(self)
    