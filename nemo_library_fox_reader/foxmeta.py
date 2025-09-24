from dataclasses import fields, is_dataclass
from typing import TypeVar
import logging

from nemo_library.features.focus import focusMoveAttributeBefore
from nemo_library.features.foxfile import FOXFile
from nemo_library.features.foxformulaparser import FoxFormulaParser
from nemo_library.features.nemo_persistence_api import (
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
from nemo_library.model.attribute_group import AttributeGroup
from nemo_library.model.attribute_link import AttributeLink
from nemo_library.model.column import Column
from nemo_library.model.foxattribute import FoxAttribute
from nemo_library.model.project import Project
from nemo_library.utils.config import Config
from nemo_library.utils.utils import (
    SUMMARY_FUNCTIONS,
    FOXAttributeType,
    get_display_name,
)

T = TypeVar("T")


class FOXMeta:
    """
    class for reconcile metadata of FOX file with NEMO project.
    """

    def __init__(self, fox: FOXFile):
        self.global_information = fox.global_information
        self.attributes = fox.attributes

    def reconcile_metadata(
        self,
        config: Config,
        projectname: str,
    ) -> None:
        """
        Reconciles the NEMO project metadata with the FOX file attributes.
        Adds, updates, or deletes columns in the NEMO project to match the FOX file.
        Args:
            config (Config): NEMO configuration object.
            projectname (str): Name of the NEMO project to reconcile.
        """

        # adjust parent relationships
        logging.info("Setting parent relationships for FOX attributes...")
        self._set_parent_relationships()

        # read meatadata from FOX file
        # sequence is important here, because we do not support all attribute types and formuale yet
        # during the processing, we will log unsupported attribute types
        # thus: links needs to be the last one to be processed, if they point to an unsupported attribute type,
        # the link will be skipped and not imported into NEMO project
        columns_fox = self._get_fox_columns_NORMAL()
        columns_fox.extend(self._get_fox_columns_SUMMARY())
        columns_fox.extend(self._get_fox_columns_EXPRESSION())
        columns_fox.extend(self._get_fox_columns_CLASSIFICATION())
        columns_fox.extend(self._get_fox_columns_CASEDISCRIMINATION())
        attributegroups_fox = self._get_fox_columns_HEADER()
        attributelinks_fox = self._get_fox_columns_LINK()

        # log unnsupported attribute types
        # TODO: implement support for these attribute types
        for attr in self.attributes:
            if attr.nemo_not_supported:
                logging.warning(
                    f"Attribute {attr.attribute_name} (ID: {attr.attribute_id}) ist not unsupported. It will not be imported."
                )

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
                            logging.info(f"{attrname}: {old_value} --> {new_value}")
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
                f"Found {len(deletions[key])} deletions, {len(updates[key])} updates, and {len(creates[key])} new {key} in FOX file."
            )

        # Start with deletions
        logging.info(f"start deletions")
        delete_functions = {
            "attributelinks": deleteAttributeLinks,
            "attributegroups": deleteAttributeGroups,
            "columns": deleteColumns,
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

        # finally, adjust order of objects in NEMO project
        logging.info("Adjusting order of objects in NEMO project...")
        self._adjust_order(config=config, projectname=projectname)

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
                logging.info(
                    f"Setting parent of '{attr.attribute_name}' (level {current_level}) to "
                    f"'{parent_attr.attribute_name}' (level {current_level - 1})"
                )
            else:
                attr.parent_index = None
                logging.info(
                    f"No parent found for '{attr.attribute_name}' (level {current_level})"
                )

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
                if attr.attribute_type == FOXAttributeType.Link:
                    # search for the referenced attribute
                    referenced_attr = self._get_referenced_attribute(attr)
                    if referenced_attr.nemo_not_supported:

                        logging.warning(
                            f"Link {attr.attribute_name} references attribute {referenced_attr.attribute_name} which has an unsupported attribute type {referenced_attr.attribute_type}. Link is skipped."
                        )
                        continue

                if not attr.nemo_not_supported:

                    logging.info(
                        f"move attribute {attr.attribute_name} (Type {attr.attribute_type}) before {last_attr.attribute_name if last_attr else 'None'} (Parent: {self._get_parent_internal_name(attr)})"
                    )
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
                    logging.info(
                        f"Recursively adjusting order for child attributes of {attr.attribute_name} (Type {attr.attribute_type})"
                    )
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
                # attr.attribute1_index = reader.read_int()
                # attr.attribute2_index = reader.read_int()
                # attr.function = reader.read_int()
                # attr.marginal_value = reader.read_int()
                # attr.combined_format = reader.read_compressed_string()
                attribute1 = next(
                    (
                        a
                        for a in self.attributes
                        if a.attribute_id == attr.attribute1_index
                    ),
                    None,
                )
                attribute2 = next(
                    (
                        a
                        for a in self.attributes
                        if a.attribute_id == attr.attribute2_index
                    ),
                    None,
                )
                function = SUMMARY_FUNCTIONS.get(attr.function,None)
                # if we do not support this function, we have to ignore this attribute
                if not function:
                    logging.warning(f"Summary function {attr.function} not supported for attribute {attr.attribute_name}. Attribute will be ignored")
                    attr.nemo_not_supported = True  # mark as not supported for now
                logging.info(
                    f"Processing summary attribute {attr.attribute_name} with attribute1 {attribute1.attribute_name} and attribute2 {attribute2.attribute_name if attribute2 else None}, function {attr.function}, marginal value {attr.marginal_value}, combined format {attr.combined_format}."
                )
                attr.nemo_not_supported = True  # mark as not supported for now
        return columns

    def _get_fox_columns_EXPRESSION(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes.
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Expression:
                parser = FoxFormulaParser()
                tree = parser.parse(attr.expression_string)
                logging.info(tree.pretty())
                attr.nemo_not_supported = True  # mark as not supported for now
                
                attr.nemo_not_supported = True  # mark as not supported for now
        return columns

    def _get_fox_columns_CLASSIFICATION(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes.
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Classification:
                attr.nemo_not_supported = True  # mark as not supported for now
        return columns

    def _get_fox_columns_CASEDISCRIMINATION(self) -> list[Column]:
        """
        Returns a list of DefinedColumn objects from the FOX file attributes.
        """
        columns = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.CaseDiscrimination:
                attr.nemo_not_supported = True  # mark as not supported for now
        return columns

    def _get_fox_columns_LINK(self) -> list[AttributeLink]:
        """
        Returns a list of AttributeLink objects from the FOX file attributes.
        """
        attribute_links = []
        for attr in self.attributes:
            if attr.attribute_type == FOXAttributeType.Link:

                # search for the referenced attribute
                referenced_attr = self._get_referenced_attribute(attr)

                if referenced_attr.nemo_not_supported:
                    logging.warning(
                        f"Link {attr.attribute_name} references attribute {referenced_attr.attribute_name} which has an unsupported attribute type {referenced_attr.attribute_type}. Link is skipped."
                    )
                    continue

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


    def _get_referenced_attribute(self, attr: FoxAttribute) -> FoxAttribute:
        """return the referenced attribute for a link attribute. Resolve nested links if necessary.

        Args:
            attr (FoxAttribute): _description_

        Raises:
            ValueError: _description_

        Returns:
            FoxAttribute: _description_
        """
        referenced_attr = next(
            (
                a
                for a in self.attributes
                if a.attribute_id == attr.original_attribute_index
            ),
            None,
        )
        if not referenced_attr:
            raise ValueError(
                f"Referenced attribute with ID {attr.original_attribute_index} not found for link attribute {attr.attribute_name}."
            )

        if referenced_attr.attribute_type == FOXAttributeType.Link:
            logging.info(
                f"Link attribute {attr.attribute_name} references another link attribute {referenced_attr.attribute_name}. Resolving nested link."
            )
            referenced_attr = self._get_referenced_attribute(referenced_attr)

        return referenced_attr



