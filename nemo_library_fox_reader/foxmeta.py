from dataclasses import fields, is_dataclass
from typing import TypeVar
import logging
import math

from nemo_library.features.focus import focusMoveAttributeBefore
from nemo_library_fox_reader.foxfile import FOXFile
from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo
from nemo_library_fox_reader.foxstatisticsinfo import IssueType
from nemo_library_fox_reader.foxnemo_persistence_api import (
    createAttributeGroups,
    createAttributeLinks,
    createColumns,
    createProjects,
    deleteAttributeGroups,
    deleteAttributeLinks,
    deleteColumns,
    getAttributeGroups,
    getAttributeLinks,
    getColumns,
    getProjectID,
)
from nemo_library_fox_reader.foxnemo_persistence_api import coupleAttributes
from nemo_library.model.attribute_group import AttributeGroup
from nemo_library.model.attribute_link import AttributeLink
from nemo_library.model.column import Column
from nemo_library_fox_reader.foxattribute import FoxAttribute
from nemo_library_fox_reader.foxformulaconverter import FoxFormulaConverter
from nemo_library_fox_reader.models.couple_attributes_request import CoupleAttributesRequest
from nemo_library.model.project import Project
from nemo_library.utils.config import Config

from nemo_library.utils.utils import (
    get_display_name,
)

from nemo_library_fox_reader.foxutils import (
    SUMMARY_FUNCTIONS,
    SUMMARY_FUNCTIONS_ALL,
    FOXAttributeType,
    get_aggregation_function
)

T = TypeVar("T")


class FOXMeta:
    """
    class for reconcile metadata of FOX file with NEMO project.
    """

    def __init__(self, fox: FOXFile, foxReaderInfo: FOXReaderInfo | None = None):
        self.global_information = fox.global_information
        self.attributes = fox.attributes
        self.foxReaderInfo = foxReaderInfo
        # logging.info(f"FOXMeta __init__ foxReaderInfo={self.foxReaderInfo}")

    def reconcile_metadata(
        self,
        config: Config,
        projectname: str,
        statistics_only: bool = False
    ) -> None:
        """
        Reconciles the NEMO project metadata with the FOX file attributes.
        Adds, updates, or deletes columns in the NEMO project to match the FOX file.
        Args:
            config (Config): NEMO configuration object.
            projectname (str): Name of the NEMO project to reconcile.
            statistics_only (bool, optional): If True, statistics are collected and no ingestion is performed
        """

        # adjust parent relationships
      ##  logging.info("Setting parent relationships for FOX attributes...")
        self._set_parent_relationships()

        # read meatadata from FOX file
        # sequence is important here, because we do not support all attribute types and formuale yet
        # during the processing, we will log unsupported attribute types
        # thus: links needs to be the last one to be processed, if they point to an unsupported attribute type,
        # the link will be skipped and not imported into NEMO project
        attributegroups_fox = self._get_fox_columns_HEADER()
        columns_fox = self._get_fox_columns_NORMAL()
        columns_fox.extend(self._get_fox_columns_SUMMARY())
        columns_fox.extend(self._get_fox_columns_EXPRESSION())
        columns_fox.extend(self._get_fox_columns_LINKS_with_DATEDETAILS())
        columns_fox.extend(self._get_fox_columns_CLASSIFICATION())
        columns_fox.extend(self._get_fox_columns_CASEDISCRIMINATION())
        # attributegroups_fox = self._get_fox_columns_HEADER()
        attributelinks_fox = self._get_fox_columns_LINK()

        if self.foxReaderInfo:
            self.foxReaderInfo.dictionary_internal_names_to_data_types = {}
            for col in columns_fox:
                self.foxReaderInfo.dictionary_internal_names_to_data_types[col.internalName] = col.dataType     

        # log unsupported attribute types
        # TODO: implement support for these attribute types
        for attr in self.attributes:
            if attr.nemo_not_supported:
                logging.warning(
                    f"Attribute {attr.attribute_name} (ID: {attr.attribute_id}) ist not unsupported. It will not be imported."
                )

        # if parameter statistics_only is True, then no ingestion takes place
        if statistics_only:
            logging.info("statistics_only is True => no ingestion takes place")
            return
        
        # create a project if it does not exist
        if not getProjectID(config=config, projectname=projectname):
            createProjects(
                config=config,
                projects=[
                    Project(
                        displayName=projectname,
                        description=f"Project created from FOX file {self.global_information.table_name}",
                    )
                ],
            )

        # split columns in ExportedColumns and DefindedColumns
        exported_columns_fox = [col for col in columns_fox if col.columnType == "ExportedColumn"]
        defined_columns_fox = [col for col in columns_fox if col.columnType == "DefinedColumn"]
        other_columns_fox = [col for col in columns_fox if (col.columnType != "DefinedColumn" and col.columnType != "ExportedColumn")]

        # for c in attributegroups_fox:
        #     logging.info(f"Group: '{c.internalName}' {c.attributeGroupType}  '{c.parentAttributeGroupInternalName}'")
        # for c in attributelinks_fox:
        #     logging.info(f"Link: '{c.internalName}' source='{c.sourceAttributeInternalName}'  '{c.parentAttributeGroupInternalName}'")
        # for c in exported_columns_fox:
        #     logging.info(f"Exported: '{c.internalName}'  '{c.parentAttributeGroupInternalName}'  {c.dataType}")
        # for c in defined_columns_fox:
        #     logging.info(f"Defined: '{c.internalName}'  '{c.parentAttributeGroupInternalName}'  {c.dataType}")
        # for c in other_columns_fox:
        #     logging.info(f"Other: '{c.internalName}' {c.columnType}  '{c.parentAttributeGroupInternalName}'  {c.dataType}")

        # load current metadata from NEMO project
        nemo_lists = {}
        methods = {
            "columns": getColumns,
            "attributegroups": getAttributeGroups,
            "attributelinks": getAttributeLinks,
        }
        for name, method in methods.items():
            nemo_lists[name] = method(
                config=config,
                projectname=projectname,
            )

        # compare objects from FOX file with NEMO project
        logging.info(
            f"Comparing FOX file metadata with NEMO project '{projectname}'..."
        )

        def _find_deletions(model_list: list[T], nemo_list: list[T]) -> list[T]:
            model_keys = {obj.internalName for obj in model_list}
            return [obj for obj in nemo_list if obj.internalName not in model_keys]

        def _find_updates(model_list: list[T], nemo_list: list[T]) -> list[T]:
            updates = []
            nemo_dict = {getattr(obj, "internalName"): obj for obj in nemo_list}
            for model_obj in model_list:
                key = getattr(model_obj, "internalName")
                if key in nemo_dict:
                    nemo_obj = nemo_dict[key]
                    differences = {}
                    if is_dataclass(model_obj) and is_dataclass(nemo_obj):
                        differences = {
                            attr.name: (
                                getattr(model_obj, attr.name),
                                getattr(nemo_obj, attr.name),
                            )
                            for attr in fields(model_obj)
                            if getattr(model_obj, attr.name)
                            != getattr(nemo_obj, attr.name)
                            and not attr.name
                            in ["isCustom", "tenant", "order", "id", "projectId"]
                        }

                    if differences:
                        for attrname, (new_value, old_value) in differences.items():
                            logging.info(f"Difference: {attrname}: {old_value} --> {new_value}")
                        updates.append(model_obj)

            return updates

        def _find_new_objects(model_list: list[T], nemo_list: list[T]) -> list[T]:
            nemo_keys = {getattr(obj, "internalName") for obj in nemo_list}
            return [
                obj
                for obj in model_list
                if getattr(obj, "internalName") not in nemo_keys
            ]

        deletions: dict[str, list[T]] = {}
        updates: dict[str, list[T]] = {}
        creates: dict[str, list[T]] = {}

        for key, model_list, nemo_list in [
            ("columns", columns_fox, nemo_lists["columns"]),
            ("attributegroups", attributegroups_fox, nemo_lists["attributegroups"]),
            ("attributelinks", attributelinks_fox, nemo_lists["attributelinks"]),
        ]:
            deletions[key] = _find_deletions(model_list, nemo_list)
            updates[key] = _find_updates(model_list, nemo_list)
            creates[key] = _find_new_objects(model_list, nemo_list)

            logging.info(
                f"Found  {key} {len(deletions[key])} deletions, {len(updates[key])} updates, and {len(creates[key])} new {key} in FOX file."
            )

        # Start with deletions
        logging.info(f"start deletions")
        delete_functions = {
            "attributelinks": deleteAttributeLinks,
            "attributegroups": deleteAttributeGroups,
            "columns": deleteColumns,
            # "exported_columns": deleteColumns,
            # "defined_columns": deleteColumns,
            # "other_columns": deleteColumns,
        }

        for key, delete_function in delete_functions.items():
            if deletions[key]:
                objects_to_delete = [data_nemo.id for data_nemo in deletions[key]]
                delete_function(config=config, **{key: objects_to_delete})

        # Now do updates and creates in a reverse  order
        logging.info(f"start creates and updates")
        create_functions = {
            "attributegroups": createAttributeGroups,
            "columns": createColumns,
            "attributelinks": createAttributeLinks,
        }

        for key, create_function in create_functions.items():
            # create new objects first
            if creates[key]:
                create_function(
                    config=config, projectname=projectname, **{key: creates[key]}
                )
            # now the changes
            if updates[key]:
                create_function(
                    config=config, projectname=projectname, **{key: updates[key]}
                )

        if self.foxReaderInfo:
            self._fill_dictionary_internal_names_to_attribute_ids(config=config, projectname=projectname)

        # finally, adjust order of objects in NEMO project
        logging.info("Adjusting order of objects in NEMO project...")
        try:
            self._adjust_order(config=config, projectname=projectname)
        except Exception as e:
            logging.warning(f"Failed to adjust order of attributes: {e}")

        try:
            self._couple_attributes(config=config, projectname=projectname, attributes=self.attributes)
        except Exception as e:
            logging.warning(f"Failed to couple attributes: {e}")


    def _fill_dictionary_internal_names_to_attribute_ids(self,config: Config, projectname: str) -> None:
        """
        Fills the foxReaderInfo dictionary mapping internal names to attribute IDs.
        Args:
            config (Config): NEMO configuration object.
            projectname (str): Name of the NEMO project.
        """

        if self.foxReaderInfo is None:
            return

        self.foxReaderInfo.dictionary_internal_names_to_attribute_ids = {}

        # load current metadata from NEMO project
        columns = getColumns(config=config,projectname=projectname)
        links = getAttributeLinks(config=config,projectname=projectname)
        groups = getAttributeGroups(config=config,projectname=projectname)

        for col in columns:
            self.foxReaderInfo.dictionary_internal_names_to_attribute_ids[col.internalName] = col.id
        for link in links:
            self.foxReaderInfo.dictionary_internal_names_to_attribute_ids[link.internalName] = link.id
        for group in groups:
            self.foxReaderInfo.dictionary_internal_names_to_attribute_ids[group.internalName] = group.id
        delasap4 = self.foxReaderInfo.dictionary_internal_names_to_attribute_ids

    def _set_parent_relationships(self) -> None:
        """
        Set the parent attribute for each attribute in the list based on level hierarchy.
        Assumes that attributes are ordered correctly (e.g., top-down).
        Includes detailed debugging output for troubleshooting.
        """

        level_stack = {}

        logging.info("Starting parent-child relationship assignment...")

        for index, attr in enumerate(self.attributes):
            current_level = attr.level
            logging.debug(
                f"Processing attribute #{index}: {attr.attribute_name} (level {current_level})"
            )

            # Show current state of the level stack
            stack_snapshot = {lvl: a.attribute_name for lvl, a in level_stack.items()}
            logging.debug(f"Current level stack before processing: {stack_snapshot}")

            # Determine and set parent
            if current_level > 0 and (current_level - 1) in level_stack:
                parent_attr = level_stack[current_level - 1]
                attr.parent_index = parent_attr.attribute_id
                # logging.info(
                #     f"Setting parent of '{attr.attribute_name}' (level {current_level}) to "
                #     f"'{parent_attr.attribute_name}' (level {current_level - 1})"
                # )
            else:
                attr.parent_index = None
                # logging.info(
                #     f"No parent found for '{attr.attribute_name}' (level {current_level})"
                # )

            # Update the stack for the current level
            level_stack[current_level] = attr

            # Clean up stack levels deeper than the current one
            deeper_levels = [lvl for lvl in level_stack if lvl > current_level]
            if deeper_levels:
                logging.debug(f"Cleaning up deeper levels in stack: {deeper_levels}")
            for lvl in deeper_levels:
                del level_stack[lvl]

            # Final state of stack after processing
            updated_stack = {lvl: a.attribute_name for lvl, a in level_stack.items()}
            logging.debug(f"Updated level stack after processing: {updated_stack}")

        logging.info("Finished assigning parent-child relationships.")

    def _get_parent_internal_name(self, attr: FoxAttribute) -> str:
        """
        Returns the internal name of the parent attribute based on the attribute's level.
        If the attribute is at level 0, it has no parent.
        """
        if attr.level == 0:
            return None
        parent_attr = next(
            (a for a in self.attributes if a.attribute_id == attr.parent_index),
            None,
        )
        if parent_attr:
            return parent_attr.get_nemo_name()
        return None

    def _get_parent_uuid(self, attr: FoxAttribute) -> str:
        """
        Returns the id of the parent attribute based on the attribute's level.
        If the attribute is at level 0, it has no parent.
        """
        if attr.level == 0:
            return None
        parent_attr = next(
            (a for a in self.attributes if a.attribute_id == attr.parent_index),
            None,
        )
        if parent_attr:
            return parent_attr.uuid
        return None


    def _couple_attributes(self, config: Config, projectname: str, attributes: list[FoxAttribute]) -> None:
        """
        Couples attributes in the NEMO project based on their FOX file relationships.
        Args:
            config (Config): NEMO configuration object.
            projectname (str): Name of the NEMO project.
            attributes (list[FoxAttribute]): List of FoxAttribute objects to couple.
        """

        # Prepare couple requests
        if self.foxReaderInfo is not None:
            self.foxReaderInfo.couple_attributes_requests = []
        attribute_ids = []
        previous_element_id = None
        last_attribute_is_coupled = False
        containing_group_internal_name = None
        for attr in attributes:
            if attr.coupled and attr.attribute_type != FOXAttributeType.Header:
                if not last_attribute_is_coupled:
                    containing_group_internal_name = self._get_parent_internal_name(attr)
                attribute_ids.append(attr.get_nemo_name())
                
            else:   
                if last_attribute_is_coupled and attribute_ids:
                    couple_request = CoupleAttributesRequest(
                        attributeIds=attribute_ids,
                        previousElementId=previous_element_id,
                        containingGroupInternalName=containing_group_internal_name
                    )
                    if self.foxReaderInfo:
                        self.foxReaderInfo.couple_attributes_requests.append(couple_request)
                    attribute_ids = []
                previous_element_id = attr.get_nemo_name()
            last_attribute_is_coupled = attr.coupled and attr.attribute_type != FOXAttributeType.Header


    def _adjust_order(
        self,
        config: Config,
        projectname: str,
        start_attr: FoxAttribute = None,
    ) -> None:
        """
        Adjusts the order of attributes in the FOX file based on their level and parent-child relationships.
        This is a placeholder for future implementation.
        """
        # iterate all attributes that have the start_attr as parent
        last_attr = None
        for attr in reversed(self.attributes):
            if (attr != start_attr) and (
                attr.parent_index == (start_attr.attribute_id if start_attr else None)
            ):

                # TODO: remove special handling of objects that are not supported yet
                if attr.attribute_type == FOXAttributeType.Link and attr.original_attribute_index is not None:
                    # search for the referenced attribute
                    referenced_attr = self._get_referenced_attribute(attr.original_attribute_index)
                    if referenced_attr.nemo_not_supported:

                        logging.warning(
                            f"Link {attr.attribute_name} references attribute {referenced_attr.attribute_name} which has an unsupported attribute type {referenced_attr.attribute_type}. Link is skipped."
                        )
                        continue

                if not attr.nemo_not_supported:

                    # logging.info(
                    #     f"move attribute {attr.attribute_name} (Type {attr.attribute_type}) before {last_attr.attribute_name if last_attr else 'None'} (Parent: {self._get_parent_internal_name(attr)})"
                    # )
                    focusMoveAttributeBefore(
                        config=config,
                        projectname=projectname,
                        sourceInternalName=attr.get_nemo_name(),
                        targetInternalName=(
                            last_attr.get_nemo_name() if last_attr else None
                        ),
                        groupInternalName=self._get_parent_internal_name(attr),
                    )
                    last_attr = attr

                # recursively adjust order for child attributes
                if attr.attribute_type == FOXAttributeType.Header:
                    # logging.info(
                    #     f"Recursively adjusting order for child attributes of {attr.attribute_name} (Type {attr.attribute_type})"
                    # )
                    self._adjust_order(
                        config=config, projectname=projectname, start_attr=attr
                    )

    def _get_fox_columns_NORMAL(self) -> list[Column]:
        """
        Returns a list of ImportedColumn objects from the FOX file attributes.
        """
        imported_columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Normal:
                imported_columns.append(
                    Column(
                        displayName=get_display_name(attr.attribute_name),
                        importName=attr.get_nemo_name(),
                        internalName=attr.get_nemo_name(),
                        dataType=attr.nemo_data_type,
                        parentAttributeGroupInternalName=self._get_parent_internal_name(
                            attr
                        ),
                        columnType="ExportedColumn",
                        unit=attr.nemo_unit,
                    )
                )
        return imported_columns

    def _get_fox_columns_HEADER(self) -> list[AttributeGroup]:
        """
        Returns a list of AttributeGroup objects from the FOX file attributes.
        """
        attribute_groups = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Header:
                attribute_groups.append(
                    AttributeGroup(
                        displayName=get_display_name(attr.attribute_name),
                        internalName=attr.get_nemo_name(),
                        parentAttributeGroupInternalName=self._get_parent_internal_name(
                            attr
                        ),
                        display_children = attr.display_children,
                        attributeGroupType=self._get_attribute_group_type(attr),
                    )
                )
        return attribute_groups

    def _get_fox_columns_SUMMARY(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes.
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Summary:
                attribute1 = self._get_referenced_attribute(attr.attribute1_index)

                attribute2 = None
                if attr.attribute2_index is not None and attr.attribute2_index != 4294967295:
                    attribute2 = self._get_referenced_attribute(attr.attribute2_index)

                if attr.attribute2_index > 0 and not attr.attribute2_index == 4294967295 and not attribute2:
                    logging.info(f"::::::::::::::::::::::::::::::: attr.attribute2_index {attr.attribute2_index} > 0 and not attribute2")

                function_not_supported = False
                function = SUMMARY_FUNCTIONS.get(attr.function,None)
                
                # if we do not support this function, we have to convert as a normal attribute
                if not function:
                    logging.warning(f"Summary function {attr.function} not supported for attribute {attr.attribute_name}. Attribute {attr.get_nemo_name()} will be used as normal attribute")

                    function_not_supported = True

                    if self.foxReaderInfo:
                        function_name = SUMMARY_FUNCTIONS_ALL.get(attr.function,None)
                        expression = f"{function_name}({attribute1.attribute_name}"
                        if (attribute2):
                            expression = f"{expression}, {attribute2.attribute_name})"
                        else:
                            expression = f"{expression})"
                        self.foxReaderInfo.add_issue(IssueType.SUMMARY, attr.attribute_name, expression, attr.format)

                if attribute2 and self._get_attribute_group_type(attribute2) == "Analysis":
                    function_not_supported = True # mark as not supported until supported in NEMO
                    if self.foxReaderInfo:
                        self.foxReaderInfo.add_issue(IssueType.AGGREGATIONWITHANALYSISGROUP, attr.attribute_name, SUMMARY_FUNCTIONS_ALL.get(attr.function,None), attr.format, extra_info=f"'{attribute1.attribute_name}' '{attribute2.attribute_name}'")
                    
                if not function_not_supported:

                    dataType = attr.nemo_data_type
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
                        # (
                        #     attr.nemo_data_type,
                        #     attr.nemo_numeric_separator,
                        #     attr.nemo_decimal_point,
                        # ) = self._detect_number_format(attr)
                        if format_clean == "####":
                           dataType = "integer"
                        else:
                           dataType = "float"

                    elif any(token in format_clean for token in ["d", "m", "y", "t", "s", "h"]):
                        # we have a candidate for a date(time)
                        dataType, nemo_pandas_conversion_format = (
                            FOXFile._detect_date_format(None, attr)
                        )

                    logging.info(f"Summary attribute {attr.get_nemo_name()} function {function} dataType='{dataType}'  format='{attr.format}' nemo_data_type='{attr.nemo_data_type}'")

                    aggregation_function = get_aggregation_function(function)
                    column = Column(
                                displayName=get_display_name(attr.attribute_name),
                                importName=attr.get_nemo_name(),
                                internalName=attr.get_nemo_name(),
                                dataType=dataType,
                                parentAttributeGroupInternalName=self._get_parent_internal_name(
                                    attr
                                ),
                                columnType="FocusAggregationColumn",
                                focusAggregationFunction = aggregation_function,
                                focusAggregationSourceColumnInternalName=attribute1.get_nemo_name(),
                                focusAggregationGroupByTargetType="Column" if attribute2 else "NotApplicable",
                                focusGroupByTargetInternalName=attribute2.get_nemo_name() if attribute2 else None,
                                groupByColumnInternalName=attribute2.get_nemo_name() if attribute2 else None,
                                stringSize = 0,
                                unit=attr.nemo_unit,
                            )
                    columns.append(column)
                else:
                    column = Column(
                        displayName=get_display_name(attr.attribute_name),
                        importName=attr.get_nemo_name(),
                        internalName=attr.get_nemo_name(),
                        dataType=attr.nemo_data_type,
                        parentAttributeGroupInternalName=self._get_parent_internal_name(
                            attr
                        ),
                        columnType="ExportedColumn",
                        unit=attr.nemo_unit,
                    )
                    columns.append(column)

        return columns

    def _get_fox_columns_EXPRESSION(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes.
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Expression:
                attr.nemo_not_supported = False
                formula = attr.expression_string
                is_valid_formula_parsed = False

                try:
                    fox_formula_converter = FoxFormulaConverter(attr, self.attributes, self.foxReaderInfo)
                    formula = fox_formula_converter.get_nemo_formula_from_infozoom_expression(formula)

                    is_valid_formula_parsed = True
                except Exception as exception:
                    self.foxReaderInfo.add_exception(exception)
                    pass

                nemo_data_type = attr.nemo_data_type
                # nemo_data_type = "string"

                # try:
                #     for i in range(attr.num_referenced_attributes):
                #         refId = int(attr.referenced_attributes[i])
                #         referenced_attribute = next(
                #             (
                #                 a
                #                 for a in self.attributes
                #                 if a.attribute_id == refId
                #             ),
                #             None,
                #         )
                #         if referenced_attribute:
                #             # set the data_type of the column to the data type of the data type of the first referenced attribute
                #             if nemo_data_type == "string":
                #                 nemo_data_type = referenced_attribute.nemo_data_type


                # except Exception as e:
                #     logging.info(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Exception loop referenced_attributes  num_referenced_attributes={attr.num_referenced_attributes}  i={i}")
                #     pass

                if not nemo_data_type:
                    nemo_data_type = attr.nemo_data_type

                column = Column(
                            displayName=get_display_name(attr.attribute_name),
                            importName=attr.get_nemo_name(),
                            internalName=attr.get_nemo_name(),
                            dataType=nemo_data_type,
                            parentAttributeGroupInternalName=self._get_parent_internal_name(
                                attr
                            ),
                            columnType="DefinedColumn",
                            formula=formula,
                            unit=attr.nemo_unit,
                        )
                columns.append(column)
                # logging.info(f"Expression: '{attr.attribute_name}'  format='{attr.format}' => '{nemo_data_type}'  formula='{formula}'")
                
                # if self.foxReaderInfo:
                #     self.foxReaderInfo.add_issue(IssueType.EXPRESSION, attr.attribute_name, attr.expression_string, attr.format, extra_info=formula)
        return columns

    def _get_fox_columns_LINKS_with_DATEDETAILS(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes. Detects links to a date/datetime attribute with a format for parts of a datetime like in datedetail groups
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Link:
                referenced_attr = self._get_referenced_attribute(attr.original_attribute_index)

                if referenced_attr.nemo_not_supported:
                    logging.warning(
                        f"Link {attr.attribute_name} references attribute {referenced_attr.attribute_name} which has an unsupported attribute type {referenced_attr.attribute_type}. Link is skipped."
                    )
                    continue

                is_link_to_date_with_date_format = False
                nemo_pandas_conversion_format = None
                if referenced_attr.nemo_data_type in ["date", "datetime"]:
                    nemo_data_type, nemo_pandas_conversion_format = (FOXFile._detect_date_format(None, attr))
                    if nemo_data_type in ["date", "datetime"]:
                        is_link_to_date_with_date_format = True
                        # logging.info(f"Link to date detected {attr.attribute_name} {referenced_attr.attribute_name} {nemo_data_type} {attr.format} {referenced_attr.format} {nemo_pandas_conversion_format}")

                if is_link_to_date_with_date_format:
                    # the attribute link references a date/datetime attribute and has a format of a part of datetime. 
                    # such a scenario appears in datedetails attribute groups
                    formula = None
                    referenced_attribute_link = referenced_attr.get_nemo_name()

                    # for known formats the link attribute is replaced by an expression attribute using a formula
                    if nemo_pandas_conversion_format == "%Y":
                        formula = f"year({referenced_attribute_link})"
                        nemo_data_type = "integer"
                    if nemo_pandas_conversion_format == "%m" or nemo_pandas_conversion_format == "m":
                        formula = f"month({referenced_attribute_link})"
                        nemo_data_type = "integer"
                    if nemo_pandas_conversion_format == "%d" or nemo_pandas_conversion_format == "d":
                        formula = f"day({referenced_attribute_link})"
                        nemo_data_type = "integer"
                    if nemo_pandas_conversion_format == "%H":
                        formula = f"hour({referenced_attribute_link})"
                        nemo_data_type = "integer"
                    if nemo_pandas_conversion_format == "%M":
                        formula = f"minute({referenced_attribute_link})"
                        nemo_data_type = "integer"
                    if nemo_pandas_conversion_format == "%S":
                        formula = f"second({referenced_attribute_link})"
                        nemo_data_type = "integer"
                    if nemo_pandas_conversion_format == "%D": #USI
                        formula = f"weekday({referenced_attribute_link})"
                        nemo_data_type = "string"
                        
                    if formula is not None:
                        # the expresssion attribute is a DefinedColumn in Nemo
                        column = Column(
                                    displayName=get_display_name(attr.attribute_name),
                                    importName=attr.get_nemo_name(),
                                    internalName=attr.get_nemo_name(),
                                    dataType=nemo_data_type,
                                    parentAttributeGroupInternalName=self._get_parent_internal_name(
                                        attr
                                    ),
                                    columnType="DefinedColumn",
                                    formula=formula,
                                    unit=attr.nemo_unit,
                                )
                        columns.append(column)
                    else:
                        if self.foxReaderInfo:
                            self.foxReaderInfo.add_issue(IssueType.UNSUPPORTEDFORMAT, attr.attribute_name, attr.expression_string, attr.format)

        return columns

    def _get_fox_columns_CLASSIFICATION(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes.
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Classification:
                # Build an expression from classification ranges (nested IFs)

                referenced_attribute = self._get_referenced_attribute(attr.classified_attribute_index)

                if not referenced_attribute:
                    logging.info(f"Classification detected but referenced attribute with ID {attr.classified_attribute_index} not found for '{attr.attribute_name}' - importing as ExportedColumn")
                    column = Column(
                        displayName=get_display_name(attr.attribute_name),
                        importName=attr.get_nemo_name(),
                        internalName=attr.get_nemo_name(),
                        dataType=attr.nemo_data_type,
                        parentAttributeGroupInternalName=self._get_parent_internal_name(
                            attr
                        ),
                        columnType="ExportedColumn",
                        unit=attr.nemo_unit,
                    )
                    columns.append(column)
                    continue

                # helper to quote class values/numeric thresholds
                def _quote_if_needed_for_formula(val: str) -> str:
                    if val is None:
                        return "'NULL'"
                    s = str(val).strip()
                    if s == "" or s.upper() == 'NULL':
                        return "'NULL'"
                    # already quoted
                    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
                        return s
                    # numeric?
                    try:
                        float(s)
                        return s
                    except Exception:
                        pass
                    # escape and quote
                    return f"'{s.replace("'", "''")}'"

                # Build nested IFs: start with default 'NULL' as the final else
                nested = "'NULL'"
                maximum_threshold = None
                classification_ranges = getattr(attr, 'classification_ranges', [])
                for r in classification_ranges:
                    threshold = r.get('threshold')
                    if threshold is not None and not math.isnan(threshold):
                        if (maximum_threshold is None):
                            maximum_threshold = threshold
                        else:
                            maximum_threshold = max(maximum_threshold, threshold)

                # iterate ranges in reverse so first range becomes outermost IF
                for r in reversed(classification_ranges):
                    op = r.get('op')
                    threshold = r.get('threshold')
                    s_threshold = r.get('s_threshold')
                    s_class_value = r.get('s_class_value')
                    values = r.get('values', []) or []

                    colname = referenced_attribute.get_nemo_name()

                    cond = None
                    # map operator codes to conditions
                    if op == 0:  # OPERATOR_LESS_EQUAL
                        cond = f"({colname} <= {threshold})"
                    elif op == 1:  # OPERATOR_LESS
                        cond = f"({colname} < {threshold})"
                    elif op == 2:  # OPERATOR_EQUAL
                        # use numeric threshold if available, otherwise compare to s_threshold
                        try:
                            float(threshold)
                            cond = f"({colname} == {threshold})"
                        except Exception:
                            cond = f"({colname} == {_quote_if_needed_for_formula(s_threshold)})"
                    elif op == 3:  # OPERATOR_IN
                        # build OR of equality checks
                        parts = []
                        for v in values:
                            q = _quote_if_needed_for_formula(v)
                            parts.append(f"({colname} == {q})")
                        cond = f"({' OR '.join(parts)})" if parts else "(False)"
                    elif op == 4:  # OPERATOR_LEGAL
                        delasap = values
                        if maximum_threshold is not None:
                            cond = f"({colname} > {maximum_threshold})"
                        else:
                            cond = "(1==1)"
                    elif op == 6:  # OPERATOR_UNDEFINED
                        cond = f"({colname} == '')"
                    elif op == 7:  # OPERATOR_ELSE
                        cond = "(1==1)"
                    else:
                        # Unknown/unsupported operator (LEGAL/ILLEGAL)
                        logging.warning(f"Unsupported classification operator {op} for attribute {attr.attribute_name}")
                        cond = "(1==1)" # in InfoZoom the value is "Greater"

                    then_val = _quote_if_needed_for_formula(s_class_value)
                    nested = f"IF ({cond} , {then_val} , {nested})"

                expression_string = nested
                if self.foxReaderInfo:
                    self.foxReaderInfo.add_issue(IssueType.CLASSIFICATION, attr.attribute_name, expression_string, attr.format)

                # create a DefinedColumn using the built formula
                column = Column(
                    displayName=get_display_name(attr.attribute_name),
                    importName=attr.get_nemo_name(),
                    internalName=attr.get_nemo_name(),
                    dataType=attr.nemo_data_type or "string",
                    parentAttributeGroupInternalName=self._get_parent_internal_name(
                        attr
                    ),
                    # columnType="ExportedColumn",
                    columnType="DefinedColumn",
                    formula=expression_string,
                    unit=attr.nemo_unit,
                )
                columns.append(column)
        return columns

    def _get_fox_columns_CASEDISCRIMINATION(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes.
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.CaseDiscrimination:

                expression_string = ""
                case_expression_string = ""

                try:
                    # Build a CASE expression from the case discrimination structure.
                    # attr.cases is a dict which may contain entries like:
                    #  - 'case_0': { 'condition': cond, 'class_value': class_val, 'refattrs': refs }
                    #  - 'case_0_class_attribute_index': <int>
                    # We only want the numbered 'case_X' entries (the dicts), not the
                    # auxiliary keys that end with '_class_attribute_index'.
                    case_expression_string = "CASE "
                    expression_string = ""

                    for case_key, case_val in attr.cases.items():
                        # only handle case entries that are dicts with a 'condition'
                        if not isinstance(case_val, dict):
                            continue
                        condition = case_val.get("condition", "")
                        if "=" in condition and ">=" not in condition and "<=" not in condition and "!=" not in condition:
                            condition = condition.replace("=", "==")  # NEMO uses double '==' for equality
                        condition = condition.replace("<>", "!=")  # NEMO uses '!=' instead of '<>'
                        if '""' in condition:
                            delasap = 42
                        condition = condition.replace('""', "'NULL'") # replace empty string with 'NULL'

                        class_value = case_val.get("class_value", "")

                        refs = case_val.get("refattrs", []) or []

                        # Replace [X0], [X1], ... placeholders with referenced attribute internal names
                        for i, ref in enumerate(refs):
                            try:
                                ref_id = int(ref)
                            except Exception:
                                ref_id = None

                            referenced_attribute = self._get_referenced_attribute(ref_id)
                            # fallback: try to match by attribute name
                            if not referenced_attribute:
                                referenced_attribute = self._get_referenced_attribute(ref)

                            if referenced_attribute:
                                placeholder = f"[X{str(i)}]"
                                replacement = f"({referenced_attribute.get_nemo_name()})"
                                condition = condition.replace(placeholder, replacement)
                                class_value = class_value.replace(placeholder, replacement)

                        # Quote non-numeric class values if not already quoted
                        def _quote_if_needed(val: str) -> str:
                            if val is None:
                                return "'NULL'"
                            s = str(val)
                            s_strip = s.strip()
                            if s_strip == "" or s_strip.upper() == 'NULL':
                                return "'NULL'"
                            if (s_strip.startswith("'") and s_strip.endswith("'")) or (
                                s_strip.startswith('"') and s_strip.endswith('"')
                            ):
                                return s_strip
                            # numeric?
                            try:
                                float(s_strip)
                                return s_strip
                            except Exception:
                                pass
                            # escape single quotes
                            s_escaped = s_strip.replace("'", "''")
                            return f"'{s_escaped}'"

                        class_value_quoted = _quote_if_needed(class_value)

                        case_expression_string += f"WHEN {condition} THEN {class_value_quoted} "
                        expression_string += f"IF (({condition}) , {class_value_quoted} , "

                    expression_string += "'NULL'"
                    for i in range(attr.num_cases):
                        expression_string += ")" 

                except Exception as e:
                    logging.info(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Exception while processing case_values num_cases={attr.num_cases} exception={e}")
                    pass

                if self.foxReaderInfo:
                    self.foxReaderInfo.add_issue(IssueType.CASEDESCRIMINATION, attr.attribute_name, f"num_cases={attr.num_cases}", attr.format, extra_info=expression_string)

                column = Column(
                    displayName=get_display_name(attr.attribute_name),
                    importName=attr.get_nemo_name(),
                    internalName=attr.get_nemo_name(),
                    dataType=attr.nemo_data_type,
                    parentAttributeGroupInternalName=self._get_parent_internal_name(
                        attr
                    ),
                    # columnType="ExportedColumn",
                    columnType="DefinedColumn",
                    formula=expression_string,
                    unit=attr.nemo_unit,
                )
                columns.append(column)

        return columns

    def _get_fox_columns_LINK(self) -> list[AttributeLink]:
        """
        Returns a list of AttributeLink objects from the FOX file attributes.
        """
        attribute_links = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Link:

                # search for the referenced attribute
                referenced_attr = self._get_referenced_attribute(attr.original_attribute_index)

                if referenced_attr.nemo_not_supported:
                    logging.warning(
                        f"Link {attr.attribute_name} references attribute {referenced_attr.attribute_name} which has an unsupported attribute type {referenced_attr.attribute_type}. Link is skipped."
                    )
                    continue

                is_link_to_date_with_date_format = False
                nemo_pandas_conversion_format = None
                if referenced_attr.nemo_data_type in ["date", "datetime"]:
                    nemo_data_type, nemo_pandas_conversion_format = (FOXFile._detect_date_format(None, attr))
                    if nemo_data_type in ["date", "datetime"]:
                        if nemo_pandas_conversion_format in ["%Y", "%y", "%m", "m", "%d", "d", "%D", "%H", "%M", "%S"]:
                            is_link_to_date_with_date_format = True
                            # logging.info(f"Link to date detected {attr.attribute_name} {referenced_attr.attribute_name} {nemo_data_type} {attr.format} {referenced_attr.format} {nemo_pandas_conversion_format}")

                if not is_link_to_date_with_date_format:
#                    logging.info(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Link detected {attr.attribute_name} {referenced_attr.attribute_name}")
                    attribute_links.append(
                        AttributeLink(
                            displayName=get_display_name(attr.attribute_name),
                            internalName=attr.get_nemo_name(),
                            sourceAttributeInternalName=referenced_attr.get_nemo_name(),
                            parentAttributeGroupInternalName=self._get_parent_internal_name(
                                attr
                            ),
                            sourceMetadataType="column",
                        )
                    )
        return attribute_links
        
    def _get_attribute_group_type(self, attr: FoxAttribute) -> str:
        """
        Returns the type of the attribute group based on the FOX attribute type.
        """

        # an "Analysis" group is defined as a group where at least one attribute is of type "Summary"
        # and the second summary attribute is a header

        for child_attr in self.attributes:
            if child_attr.parent_index == attr.attribute_id:
                if child_attr.attribute_type == FOXAttributeType.Summary:
                    # check if the referenced attribute in attribute2_index is of type Header
                    for ref_attr in self.attributes:
                        if ref_attr.attribute_id == child_attr.attribute2_index:
                            if ref_attr.attribute_type == FOXAttributeType.Header:
                                return "Analysis"

        return "Standard"  # default type


    def _get_referenced_attribute(self, original_attribute_index: int) -> FoxAttribute:
        """return the referenced attribute for a link attribute. Resolve nested links if necessary.

        Args:
            attr (FoxAttribute): _description_

        Raises:
            ValueError: _description_

        Returns:
            FoxAttribute: _description_
        """
        referenced_attribute = next(
            (
                a
                for a in self.attributes
                if a.attribute_id == original_attribute_index
            ),
            None,
        )
        if not referenced_attribute:
            raise ValueError(
                f"Referenced attribute with ID {original_attribute_index} not found for link attribute."
            )

        if referenced_attribute.attribute_type == FOXAttributeType.Link and referenced_attribute.original_attribute_index is not None:
            referenced_attribute = self._get_referenced_attribute(referenced_attribute.original_attribute_index)

        return referenced_attribute

