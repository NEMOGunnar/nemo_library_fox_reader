import json
import logging
import re
from typing import Any, Type, TypeVar, get_type_hints

import requests


from nemo_library.model.application import Application
from nemo_library.model.attribute_group import AttributeGroup
from nemo_library.model.attribute_link import AttributeLink
from nemo_library.model.column import Column
from nemo_library.model.dependency_tree import DependencyTree
from nemo_library.model.diagram import Diagram
from nemo_library.model.metric import Metric
from nemo_library.model.pages import Page
from nemo_library.model.project import Project
from nemo_library.model.report import Report
from nemo_library.model.rule import Rule
from nemo_library.model.subprocess import SubProcess
from nemo_library.model.tile import Tile
from nemo_library.model.variance import Variance
from nemo_library.utils.config import Config
from nemo_library.utils.utils import FilterType, FilterValue, log_error
from nemo_library_fox_reader.models.couple_attributes_request import CoupleAttributesRequest

T = TypeVar("T")


def _deserializeMetaDataObject(value: Any, target_type: Type) -> Any:
    """
    Recursively deserializes JSON data into a nested DataClass structure.
    """
    if isinstance(value, list):
        # Check if we expect a list of DataClasses
        if hasattr(target_type, "__origin__") and target_type.__origin__ is list:
            element_type = target_type.__args__[0]
            return [_deserializeMetaDataObject(v, element_type) for v in value]
        return value  # Regular list without DataClasses
    elif isinstance(value, dict):
        # Check if the target type is a DataClass
        if hasattr(target_type, "__annotations__"):
            field_types = get_type_hints(target_type)
            return target_type(
                **{
                    key: _deserializeMetaDataObject(value[key], field_types[key])
                    for key in value
                    if key in field_types
                }
            )
        return value  # Regular dictionary
    return value  # Primitive values


def _generic_metadata_create_or_update(
    config: Config,
    projectname: str,
    objects: list[T],
    endpoint: str,
    get_existing_func,
) -> None:
    """
    Generic function to create or update metadata entries.

    :param config: Configuration containing connection details
    :param projectname: Name of the project
    :param objects: List of objects to create or update
    :param endpoint: API endpoint (e.g., "Tiles" or "Pages")
    :param get_existing_func: Function to check if an object already exists
    """

    # Initialize request
    headers = config.connection_get_headers()
    project_id = getProjectID(config, projectname)
    params = {"translationHandling": "UseAuxiliaryTranslationFields"}
    for obj in objects:
        # logging.info(
        #     f"Create/update {endpoint[:-1] if endpoint.endswith("s") else endpoint} '{obj.displayName if hasattr(obj, 'displayName') else obj.internalName}'"
        # )

        obj.tenant = config.get_tenant()
        obj.projectId = project_id

        # Check if the object already exists
        existing_object = get_existing_func(
            config=config,
            projectname=projectname,
            filter=obj.internalName,
            filter_type=FilterType.EQUAL,
            filter_value=FilterValue.INTERNALNAME,
        )

        if len(existing_object) == 1:
            # Update existing object
            obj.id = existing_object[0].id
            response = requests.put(
                f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}/{obj.id}",
                json=obj.to_dict(),
                headers=headers,
                params=params,
            )
            # if response.status_code != 200:
            #     log_error(
            #         f"PUT Request failed.\nURL: {f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}/{obj.id}"}\nobject: {json.dumps(obj.to_dict())}\nStatus: {response.status_code}, error: {response.text}"
            #     )

        else:
            # Create new object
            response = requests.post(
                f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}",
                json=obj.to_dict(),
                headers=headers,
                params=params,
            )
            # if response.status_code != 201:
            #     log_error(
            #         f"POST Request failed.\nURL: {f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}"}\nobject: {json.dumps(obj.to_dict())}\nStatus: {response.status_code}, error: {response.text}"
            #     )


def _generic_metadata_delete(config: Config, ids: list[str], endpoint: str) -> None:
    """
    Generic function to delete metadata entries.

    :param config: Configuration containing connection details
    :param ids: List of IDs to be deleted
    :param endpoint: API endpoint (e.g., "Metrics" or "Columns")
    """

    # Initialize request
    headers = config.connection_get_headers()

    for obj_id in ids:
        # logging.info(
        #     f"Deleting {endpoint[:-1] if endpoint.endswith("s") else endpoint} with ID {obj_id}"
        # )

        response = requests.delete(
            f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}/{obj_id}",
            headers=headers,
            params={"translationHandling": "UseAuxiliaryTranslationFields"},
        )

        if response.status_code != 204:
            log_error(
                f"DELETE Request failed.\nURL: {f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}/{obj_id}"}\nStatus: {response.status_code}, error: {response.text}"
            )


def _generic_metadata_get(
    config: Config,
    projectname: str,
    endpoint: str,
    endpoint_postfix: str,
    return_type: Type[T],
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[T]:
    """
    Generic method to fetch and filter metadata for different objects.

    :param config: Configuration containing connection details
    :param projectname: Name of the project
    :param endpoint: API endpoint (e.g., "Tiles" or "Pages")
    :param return_type: The class of the returned object (Tile or Page)
    :param filter: Filter value for searching
    :param filter_type: Type of filter (EQUAL, STARTSWITH, etc.)
    :param filter_value: The attribute to filter on (e.g., DISPLAYNAME)
    :return: A list of objects of the specified return_type
    """

    # Initialize request
    headers = config.connection_get_headers()
    project_id = getProjectID(config, projectname)
    params = {"translationHandling": "UseAuxiliaryTranslationFields"}

    response = requests.get(
        f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}/project/{project_id}{endpoint_postfix}",
        headers=headers,
        params=params,
    )

    if response.status_code != 200:
        log_error(
            f"GET Request failed.\nURL:{f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/{endpoint}/project/{project_id}{endpoint_postfix}"}\nStatus: {response.status_code}, error: {response.text}"
        )
        return []

    data = json.loads(response.text)

    def match_filter(value: str, filter: str, filter_type: FilterType) -> bool:
        """Applies the given filter to the value."""
        if filter == "*":
            return True
        elif filter_type == FilterType.EQUAL:
            return value == filter
        elif filter_type == FilterType.STARTSWITH:
            return value.startswith(filter)
        elif filter_type == FilterType.ENDSWITH:
            return value.endswith(filter)
        elif filter_type == FilterType.CONTAINS:
            return filter in value
        elif filter_type == FilterType.REGEX:
            return re.search(filter, value) is not None
        return False

    # Apply filter to the data
    filtered_data = [
        item
        for item in data
        if match_filter(item.get(filter_value.value, ""), filter, filter_type)
    ]

    return [_deserializeMetaDataObject(item, return_type) for item in filtered_data]


def getProjectID(
    config: Config,
    projectname: str,
) -> str:
    """
    Retrieves the unique project ID for a given project name.

    Args:
        config (Config): Configuration object containing connection details.
        projectname (str): The name of the project for which to retrieve the ID.

    Returns:
        str: The unique identifier (ID) of the specified project.

    Raises:
        ValueError: If the project name cannot be uniquely identified in the project list.

    Notes:
        - This function relies on the `getProjects` function to fetch the full project list.
        - If multiple or no entries match the given project name, an error is logged, and the first matching ID is returned.
    """
    projects = getProjects(
        config,
        filter=projectname,
        filter_type=FilterType.EQUAL,
        filter_value=FilterValue.DISPLAYNAME,
    )
    if len(projects) != 1:
        return None

    return projects[0].id


def getAttributeGroups(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[AttributeGroup]:
    """Fetches AttributeGroups metadata with the given filters."""
    return _generic_metadata_get(
        config,
        projectname,
        "AttributeGroup",
        "/attributegroups",
        AttributeGroup,
        filter,
        filter_type,
        filter_value,
    )


def getAttributeLinks(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[AttributeLink]:
    """Fetches AttributeLinks metadata with the given filters."""

    return _generic_metadata_get(
        config,
        projectname,
        "AttributeLink",
        "",
        AttributeLink,
        filter,
        filter_type,
        filter_value,
    )


def getMetrics(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Metric]:
    """Fetches Metrics metadata with the given filters."""
    return _generic_metadata_get(
        config, projectname, "Metrics", "", Metric, filter, filter_type, filter_value
    )


def getTiles(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Tile]:
    """Fetches Tiles metadata with the given filters."""
    return _generic_metadata_get(
        config, projectname, "Tiles", "", Tile, filter, filter_type, filter_value
    )


def getPages(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Page]:
    """Fetches Pages metadata with the given filters."""
    return _generic_metadata_get(
        config, projectname, "Pages", "", Page, filter, filter_type, filter_value
    )


def getApplications(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Application]:
    """Fetches Applications metadata with the given filters."""
    return _generic_metadata_get(
        config,
        projectname,
        "Applications",
        "",
        Application,
        filter,
        filter_type,
        filter_value,
    )


def getDiagrams(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Diagram]:
    """Fetches Diagrams metadata with the given filters."""
    return _generic_metadata_get(
        config,
        projectname,
        "Diagrams",
        "",
        Diagram,
        filter,
        filter_type,
        filter_value,
    )


def getColumns(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Column]:
    """Fetches columns metadata with the given filters."""
    return _generic_metadata_get(
        config,
        projectname,
        "Columns",
        "/all",
        Column,
        filter,
        filter_type,
        filter_value,
    )


def getReports(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Report]:
    """Fetches Reports metadata with the given filters."""
    return _generic_metadata_get(
        config,
        projectname,
        "Reports",
        "/reports",
        Report,
        filter,
        filter_type,
        filter_value,
    )


def getSubProcesses(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[SubProcess]:
    """Fetches SubProcesss metadata with the given filters."""
    return _generic_metadata_get(
        config,
        projectname,
        "SubProcess",
        "/subprocesses",
        SubProcess,
        filter,
        filter_type,
        filter_value,
    )


def getRules(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Rule]:
    """Fetches Rules metadata with the given filters."""
    return _generic_metadata_get(
        config, projectname, "Rule", "/rules", Rule, filter, filter_type, filter_value
    )


def getVariances(
    config: Config,
    projectname: str,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Variance]:
    """Fetches Variances metadata with the given filters."""
    return _generic_metadata_get(
        config,
        projectname,
        "Variance",
        "/variances",
        Variance,
        filter,
        filter_type,
        filter_value,
    )


def deleteColumns(config: Config, columns: list[str]) -> None:
    """Deletes a list of Columns by their IDs."""
    _generic_metadata_delete(config, columns, "Columns")


def deleteMetrics(config: Config, metrics: list[str]) -> None:
    """Deletes a list of Metrics by their IDs."""
    _generic_metadata_delete(config, metrics, "Metrics")


def deleteTiles(config: Config, tiles: list[str]) -> None:
    """Deletes a list of Tiles by their IDs."""
    _generic_metadata_delete(config, tiles, "Tiles")


def deleteAttributeGroups(config: Config, attributegroups: list[str]) -> None:
    """Deletes a list of AttributeGroups by their IDs."""
    _generic_metadata_delete(config, attributegroups, "AttributeGroup")


def deleteAttributeLinks(config: Config, attributelinks: list[str]) -> None:
    """Deletes a list of AttributeLinks by their IDs."""
    _generic_metadata_delete(config, attributelinks, "AttributeLink")


def deletePages(config: Config, pages: list[str]) -> None:
    """Deletes a list of Pages by their IDs."""
    _generic_metadata_delete(config, pages, "Pages")


def deleteApplications(config: Config, applications: list[str]) -> None:
    """Deletes a list of Pages by their IDs."""
    _generic_metadata_delete(config, applications, "Applications")


def deleteDiagrams(config: Config, diagrams: list[str]) -> None:
    """Deletes a list of Diagrams by their IDs."""
    _generic_metadata_delete(config, diagrams, "Diagrams")


def deleteSubprocesses(config: Config, subprocesses: list[str]) -> None:
    """Deletes a list of SubProcesses by their IDs."""
    _generic_metadata_delete(config, subprocesses, "SubProcess")


def deleteReports(config: Config, reports: list[str]) -> None:
    """Deletes a list of Reports by their IDs."""
    _generic_metadata_delete(config, reports, "Reports")


def deleteRules(config: Config, rules: list[str]) -> None:
    """Deletes a list of Rules by their IDs."""
    _generic_metadata_delete(config, rules, "Rule")


def deleteVariances(config: Config, variances: list[str]) -> None:
    """Deletes a list of Variances by their IDs."""
    _generic_metadata_delete(config, variances, "Variance")


def createColumns(config: Config, projectname: str, columns: list[Column]) -> None:
    """Creates or updates a list of columns."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=columns,
        endpoint="Columns",
        get_existing_func=getColumns,
    )


def createMetrics(config: Config, projectname: str, metrics: list[Metric]) -> None:
    """Creates or updates a list of Metrics."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=metrics,
        endpoint="Metrics",
        get_existing_func=getMetrics,
    )


def createTiles(config: Config, projectname: str, tiles: list[Tile]) -> None:
    """Creates or updates a list of Tiles."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=tiles,
        endpoint="Tiles",
        get_existing_func=getTiles,
    )


def createAttributeGroups(
    config: Config, projectname: str, attributegroups: list[AttributeGroup]
) -> None:
    """Creates or updates a list of AttributeGroups."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=attributegroups,
        endpoint="AttributeGroup",
        get_existing_func=getAttributeGroups,
    )


def createAttributeLinks(
    config: Config, projectname: str, attributelinks: list[AttributeLink]
) -> None:
    """Creates or updates a list of AttributeLinks."""

    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=attributelinks,
        endpoint="AttributeLink",
        get_existing_func=getAttributeLinks,
    )


def createPages(config: Config, projectname: str, pages: list[Page]) -> None:
    """Creates or updates a list of Pages."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=pages,
        endpoint="Pages",
        get_existing_func=getPages,
    )


def createApplications(
    config: Config, projectname: str, applications: list[Application]
) -> None:
    """Creates or updates a list of Applications."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=applications,
        endpoint="Applications",
        get_existing_func=getApplications,
    )


def createDiagrams(config: Config, projectname: str, diagrams: list[Diagram]) -> None:
    """Creates or updates a list of Diagrams."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=diagrams,
        endpoint="Diagrams",
        get_existing_func=getDiagrams,
    )


def createSubProcesses(
    config: Config, projectname: str, subprocesses: list[SubProcess]
) -> None:
    """Creates or updates a list of SubProcesses."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=subprocesses,
        endpoint="SubProcess",
        get_existing_func=getSubProcesses,
    )


def createReports(config: Config, projectname: str, reports: list[Report]) -> None:
    """Creates or updates a list of Reports."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=reports,
        endpoint="Reports",
        get_existing_func=getReports,
    )


def createRules(config: Config, projectname: str, rules: list[Rule]) -> None:
    """Creates or updates a list of Rules."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=rules,
        endpoint="Rule",
        get_existing_func=getRules,
    )


def createVariances(
    config: Config, projectname: str, variances: list[Variance]
) -> None:
    """Creates or updates a list of Variances."""
    _generic_metadata_create_or_update(
        config=config,
        projectname=projectname,
        objects=variances,
        endpoint="Variance",
        get_existing_func=getVariances,
    )


def getProjects(
    config: Config,
    filter: str = "*",
    filter_type: FilterType = FilterType.STARTSWITH,
    filter_value: FilterValue = FilterValue.DISPLAYNAME,
) -> list[Project]:
    """Fetches Projects metadata with the given filters."""

    # cannot use the generic meta data getter, since this is "above" the other object

    headers = config.connection_get_headers()

    response = requests.get(
        config.get_config_nemo_url() + "/api/nemo-projects/projects", headers=headers
    )
    if response.status_code != 200:
        log_error(
            f"request failed. Status: {response.status_code}, error: {response.text}"
        )
    data = json.loads(response.text)

    def match_filter(value: str, filter: str, filter_type: FilterType) -> bool:
        """Applies the given filter to the value."""
        if filter == "*":
            return True
        elif filter_type == FilterType.EQUAL:
            return value == filter
        elif filter_type == FilterType.STARTSWITH:
            return value.startswith(filter)
        elif filter_type == FilterType.ENDSWITH:
            return value.endswith(filter)
        elif filter_type == FilterType.CONTAINS:
            return filter in value
        elif filter_type == FilterType.REGEX:
            return re.search(filter, value) is not None
        return False

    # Apply filter to the data
    filtered_data = [
        item
        for item in data
        if match_filter(item.get(filter_value.value, ""), filter, filter_type)
    ]

    return [_deserializeMetaDataObject(item, Project) for item in filtered_data]


def deleteProjects(config: Config, projects: list[str]) -> None:
    """Deletes a list of projects by their IDs."""
    _generic_metadata_delete(config, projects, "Project")


def createProjects(config: Config, projects: list[Project]) -> None:
    """Creates or updates a list of Projects."""
    # Initialize request
    headers = config.connection_get_headers()

    for project in projects:
        logging.info(f"Create/update Project '{project.displayName}'")

        project.tenant = config.get_tenant()

        # Check if the object already exists
        existing_object = getProjects(
            config=config,
            filter=project.displayName,
            filter_type=FilterType.EQUAL,
            filter_value=FilterValue.DISPLAYNAME,
        )

        if len(existing_object) == 1:
            # Update existing object
            project.id = existing_object[0].id
            response = requests.put(
                f"{config.get_config_nemo_url()}/api/nemo-projects/projects/{project.id}",
                json=project.to_dict(),
                headers=headers,
            )
            if response.status_code != 200:
                log_error(
                    f"PUT Request failed.\nURL: {f"{config.get_config_nemo_url()}/api/nemo-projects/projects/{project.id}"}\nobject: {json.dumps(project.to_dict())}\nStatus: {response.status_code}, error: {response.text}"
                )

        else:
            # Create new object
            response = requests.post(
                f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/Project",
                json=project.to_dict(),
                headers=headers,
            )
            if response.status_code != 201:
                log_error(
                    f"POST Request failed.\nURL: {f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/Project"}\nobject: {json.dumps(project.to_dict())}\nStatus: {response.status_code}, error: {response.text}"
                )


def getDependencyTree(config: Config, id: str) -> DependencyTree:
    # Initialize request
    headers = config.connection_get_headers()
    data = {"id": id}

    response = requests.get(
        f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/Metrics/DependencyTree",
        headers=headers,
        params=data,
    )

    if response.status_code != 200:
        log_error(
            f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/Metrics/DependencyTree",
        )
        return None

    data = json.loads(response.text)
    return DependencyTree.from_dict(data)


def _couple_columns(
    config: Config,
    projectname: str,
    requestItems: list[CoupleAttributesRequest],
) -> None:
    """
    Generic function to couple attributes.

    :param config: Configuration containing connection details
    :param projectname: Name of the project
    :param requestItems: List of requests to couple attributes
    """

    # Get existing columns. Their ids are not the same as the attribute ids in FOX file.
    cols = getColumns(config, projectname)

    for request in requestItems:

        # Initialize request
        headers = config.connection_get_headers()
        project_id = getProjectID(config, projectname)
        params = {"translationHandling": "UseAuxiliaryTranslationFields"}

        request.tenant = config.get_tenant()
        request.projectId = project_id

        # Map attribute internal names to their IDs
        attr_ids = []
        for attr_nemo_name in request.attributeIds:
            attr_id = cols[next(i for i, c in enumerate(cols) if c.internalName == attr_nemo_name)].id
            attr_ids.append(attr_id)
        request.attributeIds = attr_ids

        attr_id = cols[next(i for i, c in enumerate(cols) if c.internalName == request.previousElementId)].id
        request.previousElementId = attr_id

        request_as_json = request.to_dict()
        url = f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/AttributeTree/projects/{project_id}/attributes/couple"
        # url = f"{config.get_config_nemo_url()}/api/nemo-persistence/metadata/Columns/projects/{project_id}/attributes/couple"
        # url = f"{config.get_config_nemo_url()}/api/nemo-focus/infoscape/projects/{project_id}/attributes/couple"


        response = requests.post(
            url,
            json=request_as_json,
            headers=headers,
            params=params,
        )

        status_code = response.status_code
        if response.status_code > 201:
            log_error(
                f"POST Request failed.\nURL: {url}\nObject: {request_as_json}\nStatus: {response.status_code}, error: {response.text}"
            )


def coupleAttributes(
    config: Config,
    projectname: str,
    request: list[CoupleAttributesRequest],
) -> None:
    
    _couple_columns(
        config=config,
        projectname=projectname,
        requestItems=request,
    )
