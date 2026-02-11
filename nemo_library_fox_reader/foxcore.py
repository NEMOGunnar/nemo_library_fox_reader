import pandas as pd
import logging

from nemo_library.features.deprecated import createOrUpdateReport, createOrUpdateRule
from nemo_library.features.metadata import (
    MetaDataHelperAutoResolveApplications,
    MetaDataCreate,
    MetaDataDelete,
    MetaDataHelperCleanParentAttributeGroupInternalNames,
    MetaDataHelperUpdateLinkTexts,
    MetaDataLoad,
)
from nemo_library.features.migman_precheck_files import MigManPrecheckFiles
from nemo_library_fox_reader.foxnemo_persistence_api import (
    createApplications,
    createAttributeGroups,
    createAttributeLinks,
    createColumns,
    createDiagrams,
    createMetrics,
    createPages,
    createReports,
    createRules,
    createTiles,
    deleteApplications,
    deleteAttributeGroups,
    deleteAttributeLinks,
    deleteColumns,
    deleteDiagrams,
    deleteMetrics,
    deletePages,
    deleteProjects,
    deleteReports,
    deleteRules,
    deleteTiles,
    getApplications,
    getAttributeGroups,
    getAttributeLinks,
    getColumns,
    getDiagrams,
    getMetrics,
    getPages,
    getProjectID,
    getProjects,
    getReports,
    getRules,
    getSubProcesses,
    getTiles,
    createVariances,
    deleteVariances,
    getVariances,
)
from nemo_library.features.nemo_persistence_api import createProjects
from nemo_library.features.nemo_report_api import (
    LoadReport,
)
from nemo_library.features.shared_api import (
    createExportConfiguration,
    deleteExportConfiguration,
    getExportConfiguration,
)
from nemo_library.model.application import Application
from nemo_library.model.attribute_group import AttributeGroup
from nemo_library.model.attribute_link import AttributeLink
from nemo_library.model.column import Column
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
from nemo_library.features.deficiency_mining import createOrUpdateRulesByConfigFile
from nemo_library_fox_reader.foxfileingestion import (
    ReUploadDataFrame,
    ReUploadFile,
    synchronizeCsvColsAndImportedColumns,
)
from nemo_library.features.focus import focusCoupleAttributes, focusMoveAttributeBefore
from nemo_library.features.hubspot_handler import FetchDealFromHubSpotAndUploadToNEMO
from nemo_library.features.import_configuration import ImportConfigurations
from nemo_library.features.migman_delete_projects import MigManDeleteProjects
from nemo_library.features.migman_export_data import MigManExportData
from nemo_library.features.migman_mapping_apply import MigManApplyMapping
from nemo_library.features.migman_database import (
    MigManDatabaseAddWebInformation,
    MigManDatabaseInit,
)
from nemo_library.features.migman_create_project_templates import (
    MigManCreateProjectTemplates,
)
from nemo_library.features.migman_load_data import MigManLoadData
from nemo_library.features.migman_mapping_create import MigManCreateMapping
from nemo_library.features.migman_mapping_load import MigManLoadMapping
from nemo_library.features.projects import (
    getProjectProperty,
    setProjectMetaData,
)
from nemo_library.utils.migmanutils import get_migman_project_list
from nemo_library.utils.utils import FilterType, FilterValue
from nemo_library.version import __version__
from nemo_library_fox_reader.foxprogressmanager import FOXProgressManager
from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo

from deprecated import deprecated


class FoxNemoLibrary:
    """
    A library for interacting with the NEMO system, providing methods for managing projects,
    reports, metadata, and other entities.

    Version: {__version__}

    Attributes:
        config (Config): Configuration object initialized with user-provided or default settings.
    """

    __version__ = __version__  # Add this line to expose the version attribute

    def __init__(
        self,
        config_file: str = "config.ini",
        environment: str = None,
        tenant: str = None,
        userid: str = None,
        password: str = None,
        migman_local_project_directory: str = None,
        migman_proALPHA_project_status_file: str = None,
        migman_projects: list[str] = None,
        migman_mapping_fields: list[str] = None,
        migman_additional_fields: dict[str, list[str]] = None,
        migman_multi_projects: dict[str, list[str]] = None,
        metadata_directory: str = None,
        etl_directory: str = None,
        hubspot_api_token: str = None,
        gedys_user_id: str = None,
        gedys_password: str = None,
        foxReaderInfo: FOXReaderInfo = None,
        foxreader_statistics_file : str | None = None,
    ):
        """
        Initializes the NemoLibrary instance with configuration settings.

        Args:
            config_file (str): Path to the configuration file. Defaults to "config.ini".
            environment (str, optional): Environment name (e.g., "dev", "prod"). Defaults to None.
            tenant (str, optional): Tenant name. Defaults to None.
            userid (str, optional): User ID for authentication. Defaults to None.
            password (str, optional): Password for authentication. Defaults to None.
            hubspot_api_token (str, optional): API token for HubSpot integration. Defaults to None.
            migman_local_project_directory (str, optional): Directory for local project files. Defaults to None.
            migman_proALPHA_project_status_file (str, optional): Path to the project status file. Defaults to None.
            migman_projects (list[str], optional): List of project names. Defaults to None.
            migman_mapping_fields (list[str], optional): List of mapping fields. Defaults to None.
            migman_additional_fields (dict[str, list[str]], optional): Additional fields for mapping. Defaults to None.
            migman_multi_projects (dict[str, list[str]], optional): Multi-project configurations. Defaults to None.
            metadata (str, optional): Metadata configuration. Defaults to None.
        """

        self.config = Config(
            config_file=config_file,
            environment=environment,
            tenant=tenant,
            userid=userid,
            password=password,
            migman_local_project_directory=migman_local_project_directory,
            migman_proALPHA_project_status_file=migman_proALPHA_project_status_file,
            migman_projects=migman_projects,
            migman_mapping_fields=migman_mapping_fields,
            migman_additional_fields=migman_additional_fields,
            migman_multi_projects=migman_multi_projects,
            metadata_directory=metadata_directory,
            hubspot_api_token=hubspot_api_token,
            foxreader_statistics_file=foxreader_statistics_file,
        )

        super().__init__()

    # def testLogin(self) -> None:
    #     """
    #     Tests the login credentials and returns the status.

    #     Returns:
    #         str: The status of the login attempt.

    #     Raises:
    #         RuntimeError: If the login attempt fails.

    #     Notes:
    #         - This function checks if the provided credentials are valid.
    #         - If successful, it returns a success message.
    #         - If unsuccessful, it raises a RuntimeError with an error message.
    #     """
    #     return self.config.testLogin()

    # def getProjectID(self, projectname: str) -> str:
    #     """
    #     Retrieves the unique project ID for a given project name.

    #     Args:
    #         projectname (str): The name of the project for which to retrieve the ID.

    #     Returns:
    #         str: The unique identifier (ID) of the specified project.

    #     Raises:
    #         ValueError: If the project name cannot be uniquely identified in the project list.

    #     Notes:
    #         - This function relies on the `getProjectList` function to fetch the full project list.
    #         - If multiple or no entries match the given project name, an error is logged, and the first matching ID is returned.
    #     """
    #     return getProjectID(self.config, projectname)

    # def getProjectProperty(self, projectname: str, propertyname: str) -> str:
    #     """
    #     Retrieves a specific property value of a given project from the server.

    #     Args:
    #         projectname (str): The name of the project for which the property is requested.
    #         propertyname (str): The name of the property to retrieve.

    #     Returns:
    #         str: The value of the specified property, with leading and trailing quotation marks removed.

    #     Raises:
    #         RuntimeError: If the request to fetch the project property fails (non-200 status code).

    #     Notes:
    #         - This function first fetches the project ID using the `getProjectID` function.
    #         - Constructs an endpoint URL using the project ID and property name.
    #         - Sends an HTTP GET request to fetch the property value.
    #         - Logs an error if the request fails and raises an exception.
    #     """
    #     return getProjectProperty(self.config, projectname, propertyname)

    # def LoadReport(
    #     self,
    #     projectname: str,
    #     report_guid: str = None,
    #     report_name: str = None,
    # ) -> pd.DataFrame:
    #     """
    #     Loads a report from a specified project and returns the data as a Pandas DataFrame.

    #     Args:
    #         projectname (str): The name of the project from which to load the report.
    #         report_guid (str): The unique identifier (GUID) of the report to be loaded.
    #         max_pages (int, optional): Reserved for future use to limit the number of pages in the report.

    #     Returns:
    #         pd.DataFrame: The report data as a Pandas DataFrame.

    #     Raises:
    #         RuntimeError: If the report initialization or data download fails.
    #         ValueError: If the downloaded data cannot be processed into a DataFrame.

    #     Notes:
    #         - Fetches the project ID using `getProjectID`.
    #         - Sends an HTTP POST request to initialize the report and retrieve a CSV download URL.
    #         - Downloads the CSV file and converts it into a Pandas DataFrame.
    #         - Removes the `_RECORD_COUNT` column if present in the dataset.
    #         - Logs errors and raises exceptions for failed requests or data processing issues.
    #     """
    #     return LoadReport(
    #         config=self.config,
    #         projectname=projectname,
    #         report_guid=report_guid,
    #         report_name=report_name,
    #     )

    # def setProjectMetaData(
    #     self,
    #     projectname: str,
    #     processid_column: str = None,
    #     processdate_column: str = None,
    #     corpcurr_value: str = None,
    # ) -> None:
    #     """
    #     Updates metadata for a specific project, including process ID, process date, and corporate currency value.

    #     Args:
    #         projectname (str): The name of the project to update metadata for.
    #         processid_column (str, optional): The column name representing the process ID.
    #         processdate_column (str, optional): The column name representing the process date.
    #         corpcurr_value (str, optional): The corporate currency value to set.

    #     Returns:
    #         None

    #     Raises:
    #         RuntimeError: If the HTTP PUT request to update the metadata fails (non-200 status code).

    #     Notes:
    #         - Fetches the project ID using `getProjectID`.
    #         - Constructs a metadata payload based on the provided parameters.
    #         - Sends an HTTP PUT request to update the project's business process metadata.
    #         - Logs an error if the request fails and raises an exception.
    #     """
    #     setProjectMetaData(
    #         self.config,
    #         projectname,
    #         processid_column,
    #         processdate_column,
    #         corpcurr_value,
    #     )

    # def MigManGetProjects(self) -> list[str] | None:
    #     """
    #     Retrieves a list of projects managed by the Migration Manager (MigMan).

    #     Returns:
    #         list[Project]: A list of Project objects representing MigMan projects.

    #     Notes:
    #         - This method fetches all projects that are currently managed by MigMan.
    #     """
    #     return get_migman_project_list(self.config)

    # def MigManInitDatabase(self) -> None:
    #     """
    #     Initializes the database for the Migration Manager (MigMan).

    #     Notes:
    #         - This method sets up the necessary database structure for MigMan operations.
    #     """
    #     MigManDatabaseInit()

    # def MigManDatabaseAddWebInformation(self) -> None:
    #     """
    #     Adds web-related information to the Migration Manager (MigMan) database.

    #     Notes:
    #         - This method populates the database with additional web-based metadata.
    #     """
    #     MigManDatabaseAddWebInformation()

    # def MigManPrecheckFiles(self) -> dict[str:str]:
    #     """
    #     Performs a pre-check on files required for the Migration Manager (MigMan).

    #     Returns:
    #         dict[str:str]: A dictionary containing the status of pre-checked files.

    #     Notes:
    #         - This method validates the presence and integrity of required files.
    #     """
    #     return MigManPrecheckFiles(self.config)

    # def MigManCreateProjectTemplates(self) -> None:
    #     """
    #     Creates project templates for the Migration Manager (MigMan).

    #     Notes:
    #         - This method generates reusable templates for MigMan projects.
    #     """
    #     MigManCreateProjectTemplates(self.config)

    # def MigManDeleteProjects(self) -> None:
    #     """
    #     Deletes projects managed by the Migration Manager (MigMan).

    #     Notes:
    #         - This method removes projects and their associated data from the system.
    #     """
    #     MigManDeleteProjects(self.config)

    # def MigManLoadData(
    #     self,
    #     deficiency_mining_only: bool = False,
    #     fuzzy_matching: bool = True,
    # ) -> None:
    #     """
    #     Loads data into the Migration Manager (MigMan) system.

    #     Notes:
    #         - This method imports data required for MigMan operations.
    #     """
    #     MigManLoadData(
    #         self.config,
    #         deficiency_mining_only=deficiency_mining_only,
    #         fuzzy_matching=fuzzy_matching,
    #     )

    # def MigManCreateMapping(self):
    #     """
    #     Creates a mapping configuration for the Migration Manager (MigMan).

    #     Notes:
    #         - This method defines relationships between source and target data fields.
    #     """
    #     MigManCreateMapping(self.config)

    # def MigManLoadMapping(self):
    #     """
    #     Loads an existing mapping configuration into the Migration Manager (MigMan).

    #     Notes:
    #         - This method retrieves and applies a predefined mapping configuration.
    #     """
    #     MigManLoadMapping(self.config)

    # def MigManApplyMapping(self):
    #     """
    #     Applies the mapping configuration in the Migration Manager (MigMan).

    #     Notes:
    #         - This method executes the mapping to transform data as per the configuration.
    #     """
    #     MigManApplyMapping(self.config)

    # def MigManExportData(self) -> None:
    #     """
    #     Exports data from the Migration Manager (MigMan) system.

    #     Notes:
    #         - This method extracts data for external use or backup purposes.
    #     """
    #     MigManExportData(self.config)

    def ReUploadDataFrame(
        self,
        projectname: str,
        df: pd.DataFrame,
        update_project_settings: bool = True,
        datasource_ids: list[dict] = None,
        global_fields_mapping: list[dict] = None,
        version: int = 2,
        trigger_only: bool = False,
        import_configuration: ImportConfigurations = None,
        format_data: bool = True,
    ) -> None:
        ReUploadDataFrame(
            self.config,
            projectname=projectname,
            df=df,
            update_project_settings=update_project_settings,
            datasource_ids=datasource_ids,
            global_fields_mapping=global_fields_mapping,
            version=version,
            trigger_only=trigger_only,
            import_configuration=import_configuration,
            format_data=format_data,
        )

    def ReUploadFile(
        self,
        projectname: str,
        filename: str,
        update_project_settings: bool = True,
        datasource_ids: list[dict] = None,
        global_fields_mapping: list[dict] = None,
        version: int = 2,
        trigger_only: bool = False,
        import_configuration: ImportConfigurations = None,
        format_data: bool = True,
        foxReaderInfo: FOXReaderInfo | None = None,
        statistics_only: bool = False
    ) -> None:
        """
        Re-uploads a file to a specified project in the NEMO system and triggers data ingestion.

        Args:
            projectname (str): The name of the project to which the file will be uploaded.
            filename (str): The path to the file to upload.
            update_project_settings (bool, optional): Whether to trigger the "analyze_table" task after ingestion (version 2 only). Defaults to True.
            datasource_ids (list[dict], optional): Data source identifiers for version 3 ingestion. Defaults to None.
            global_fields_mapping (list[dict], optional): Field mappings for version 3 ingestion. Defaults to None.
            version (int, optional): The ingestion version (2 or 3). Defaults to 2.
            trigger_only (bool, optional): If True, skips waiting for task completion. Defaults to False.
            statistics_only (bool, optional): If True, statistics are collected and no ingestion is performed

        Returns:
            None

        Raises:
            Exception: If any step of the file upload, data ingestion, or subsequent tasks fails.

        Notes:
            - Compresses the file into gzip format before uploading.
            - Retrieves temporary AWS S3 credentials from NEMO's Token Vendor and uploads the file to S3.
            - Sends a request to ingest the uploaded data and optionally waits for task completion.
            - Triggers "analyze_table" task if version 2 and `update_project_settings` is True.
            - Logs and raises exceptions for any errors encountered during the process.
        """

        # logging.info(f"ReUploadFile(core.py) statistics_only={statistics_only}")
        FOXProgressManager.info(f"Uploading file {filename} to project {projectname}")


        ReUploadFile(
            self.config,
            projectname=projectname,
            filename=filename,
            update_project_settings=update_project_settings,
            datasource_ids=datasource_ids,
            global_fields_mapping=global_fields_mapping,
            version=version,
            trigger_only=trigger_only,
            import_configuration=import_configuration,
            format_data=format_data,
            foxReaderInfo=foxReaderInfo,
            statistics_only = statistics_only
        )

    # @deprecated(reason="Please use 'createReports' API instead")
    # def createOrUpdateReport(
    #     self,
    #     projectname: str,
    #     displayName: str,
    #     querySyntax: str,
    #     internalName: str = None,
    #     description: str = None,
    # ) -> None:
    #     """
    #     Creates or updates a report in the specified project within the NEMO system.

    #     Args:
    #         projectname (str): The name of the project where the report will be created or updated.
    #         displayName (str): The display name of the report.
    #         querySyntax (str): The query syntax that defines the report's data.
    #         internalName (str, optional): The internal system name of the report. Defaults to a sanitized version of `displayName`.
    #         description (str, optional): A description of the report. Defaults to an empty string.

    #     Returns:
    #         None

    #     Raises:
    #         RuntimeError: If any HTTP request fails (non-200/201 status code).

    #     Notes:
    #         - Fetches the project ID using `getProjectID`.
    #         - Retrieves the list of existing reports in the project to check if the report already exists.
    #         - If the report exists, updates it with the new data using a PUT request.
    #         - If the report does not exist, creates a new report using a POST request.
    #         - Logs errors and raises exceptions for failed requests.
    #     """
    #     createOrUpdateReport(
    #         self.config,
    #         projectname,
    #         displayName,
    #         querySyntax,
    #         internalName,
    #         description,
    #     )

    # @deprecated(reason="Please use 'createRules' API instead")
    # def createOrUpdateRule(
    #     self,
    #     projectname: str,
    #     displayName: str,
    #     ruleSourceInternalName: str,
    #     internalName: str = None,
    #     ruleGroup: str = None,
    #     description: str = None,
    # ) -> None:
    #     """
    #     Creates or updates a rule in the specified project within the NEMO system.

    #     Args:
    #         projectname (str): The name of the project where the rule will be created or updated.
    #         displayName (str): The display name of the rule.
    #         ruleSourceInternalName (str): The internal name of the rule source that the rule depends on.
    #         internalName (str, optional): The internal system name of the rule. Defaults to a sanitized version of `displayName`.
    #         ruleGroup (str, optional): The group to which the rule belongs. Defaults to None.
    #         description (str, optional): A description of the rule. Defaults to an empty string.

    #     Returns:
    #         None

    #     Raises:
    #         RuntimeError: If any HTTP request fails (non-200/201 status code).

    #     Notes:
    #         - Fetches the project ID using `getProjectID`.
    #         - Retrieves the list of existing rules in the project to check if the rule already exists.
    #         - If the rule exists, updates it with the new data using a PUT request.
    #         - If the rule does not exist, creates a new rule using a POST request.
    #         - Logs errors and raises exceptions for failed requests.
    #     """
    #     createOrUpdateRule(
    #         self.config,
    #         projectname,
    #         displayName,
    #         ruleSourceInternalName,
    #         internalName,
    #         ruleGroup,
    #         description,
    #     )

    # def createOrUpdateRulesByConfigFile(
    #     self,
    #     filename: str,
    # ) -> None:
    #     createOrUpdateRulesByConfigFile(self.config, filename)

    # def synchronizeCsvColsAndImportedColumns(
    #     self,
    #     projectname: str,
    #     filename: str,
    # ) -> None:
    #     """
    #     Synchronizes the columns from a CSV file with the imported columns in a specified project.

    #     Args:
    #         projectname (str): The name of the project where the synchronization will occur.
    #         filename (str): The path to the CSV file to synchronize.

    #     Returns:
    #         None

    #     Raises:
    #         RuntimeError: If there are issues retrieving imported columns or reading the CSV file.

    #     Notes:
    #         - Retrieves the existing imported columns in the project using `getImportedColumns`.
    #         - Reads the first line of the CSV file to get column names.
    #         - Compares the column names from the CSV file with the imported columns.
    #         - Creates new imported columns in the project for any CSV column names not already present.
    #         - Uses utility functions `display_name`, `internal_name`, and `import_name` to format column names.
    #     """
    #     synchronizeCsvColsAndImportedColumns(self.config, projectname, filename)

    def focusMoveAttributeBefore(
        self,
        projectname: str,
        sourceDisplayName: str = None,
        sourceInternalName: str = None,
        targetDisplayName: str = None,
        targetInternalName: str = None,
        groupInternalName: str = None,
    ) -> None:
        """
        Moves an attribute in the focus tree of a specified project, positioning it before a target attribute.

        Args:
            projectname (str): The name of the project where the attribute will be moved.
            sourceDisplayName (str): The display name of the attribute to move.
            targetDisplayName (str, optional): The display name of the attribute before which the source will be positioned. Defaults to None.
            groupInternalName (str, optional): The internal name of the attribute group for grouping purposes. Defaults to None.

        Returns:
            None

        Raises:
            RuntimeError: If any HTTP request fails (non-200/204 status code) or if the source/target attributes are not found.

        Notes:
            - Fetches the project ID using `getProjectID`.
            - Retrieves the attribute tree for the project to locate the source and target attributes.
            - If the target display name is not provided, the source attribute is moved to the top of the group or tree.
            - Sends a PUT request to update the position of the source attribute in the attribute tree.
            - Logs errors and raises exceptions for failed requests or missing attributes.
        """

        focusMoveAttributeBefore(
            config=self.config,
            projectname=projectname,
            sourceDisplayName=sourceDisplayName,
            sourceInternalName=sourceInternalName,
            targetDisplayName=targetDisplayName,
            targetInternalName=targetInternalName,
            groupInternalName=groupInternalName,
        )

    def focusCoupleAttributes(
        self,
        projectname: str,
        attributenames: list[str],
        previous_attribute: str,
    ) -> None:
        focusCoupleAttributes(
            self.config,
            projectname=projectname,
            attributenames=attributenames,
            previous_attribute=previous_attribute,
        )

    # def FetchDealFromHubSpotAndUploadToNEMO(self, projectname: str) -> None:
    #     """
    #     Fetches deal data from HubSpot, processes it, and uploads the combined information to a specified NEMO project.

    #     Args:
    #         projectname (str): The name of the NEMO project where the deal data will be uploaded.

    #     Returns:
    #         None

    #     Raises:
    #         RuntimeError: If any step in the HubSpot data retrieval or NEMO upload process fails.

    #     Notes:
    #         - Authenticates with HubSpot using the provided configuration.
    #         - Retrieves deals, deal history, and deal activities from HubSpot.
    #         - Merges deal history and activities with deal details.
    #         - Resolves internal fields (e.g., `companyId`, `userId`) to human-readable information.
    #         - Processes the deal data to map deal stages and other fields.
    #         - Finally, uploads the processed deal data to the specified NEMO project using `upload_deals_to_NEMO`.
    #         - Includes optional debugging capability for saving/loading intermediate data as a pickle file.
    #     """
    #     FetchDealFromHubSpotAndUploadToNEMO(self.config, projectname)

    # def MetaDataLoad(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ):
    #     MetaDataLoad(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def MetaDataCreate(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ):
    #     MetaDataCreate(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def MetaDataHelperUpdateLinkTexts(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> None:
    #     MetaDataHelperUpdateLinkTexts(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def MetaDataHelperCleanParentAttributeGroupInternalNames(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> None:

    #     MetaDataHelperCleanParentAttributeGroupInternalNames(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def MetaDataHelperAutoResolveApplications(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ):
    #     MetaDataHelperAutoResolveApplications(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def MetaDataDelete(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ):
    #     MetaDataDelete(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    def getAttributeGroups(
        self,
        projectname: str,
        filter: str = "*",
        filter_type: FilterType = FilterType.STARTSWITH,
        filter_value: FilterValue = FilterValue.DISPLAYNAME,
    ) -> list[AttributeGroup]:
        """Fetches AttributeGroups metadata with the given filters."""
        return getAttributeGroups(
            config=self.config,
            projectname=projectname,
            filter=filter,
            filter_type=filter_type,
            filter_value=filter_value,
        )

    def getAttributeLinks(
        self,
        projectname: str,
        filter: str = "*",
        filter_type: FilterType = FilterType.STARTSWITH,
        filter_value: FilterValue = FilterValue.DISPLAYNAME,
    ) -> list[AttributeLink]:
        """Fetches AttributeLinks metadata with the given filters."""
        return getAttributeLinks(
            config=self.config,
            projectname=projectname,
            filter=filter,
            filter_type=filter_type,
            filter_value=filter_value,
        )

    # def getMetrics(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Metric]:
    #     """Fetches Metrics metadata with the given filters."""
    #     return getMetrics(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def getTiles(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Tile]:
    #     """Fetches Tiles metadata with the given filters."""
    #     return getTiles(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def getPages(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Page]:
    #     """Fetches Pages metadata with the given filters."""
    #     return getPages(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def getApplications(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Application]:
    #     """Fetches Applications metadata with the given filters."""
    #     return getApplications(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def getDiagrams(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Diagram]:
    #     """Fetches Diagrams metadata with the given filters."""
    #     return getDiagrams(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    def getColumns(
        self,
        projectname: str,
        filter: str = "*",
        filter_type: FilterType = FilterType.STARTSWITH,
        filter_value: FilterValue = FilterValue.DISPLAYNAME,
    ) -> list[Column]:
        """Fetches Columns metadata with the given filters."""
        return getColumns(
            config=self.config,
            projectname=projectname,
            filter=filter,
            filter_type=filter_type,
            filter_value=filter_value,
        )

    # def getReports(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Report]:
    #     """Fetches Reports metadata with the given filters."""
    #     return getReports(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def getSubProcesses(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[SubProcess]:
    #     """Fetches Reports metadata with the given filters."""
    #     return getSubProcesses(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    def getProjects(
        self,
        filter: str = "*",
        filter_type: FilterType = FilterType.STARTSWITH,
        filter_value: FilterValue = FilterValue.DISPLAYNAME,
    ) -> list[Project]:
        """Fetches Reports metadata with the given filters."""
        return getProjects(
            config=self.config,
            filter=filter,
            filter_type=filter_type,
            filter_value=filter_value,
        )

    # def getRules(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Rule]:
    #     """Fetches Rules metadata with the given filters."""
    #     return getRules(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def getVariances(
    #     self,
    #     projectname: str,
    #     filter: str = "*",
    #     filter_type: FilterType = FilterType.STARTSWITH,
    #     filter_value: FilterValue = FilterValue.DISPLAYNAME,
    # ) -> list[Variance]:
    #     """Fetches Variances metadata with the given filters."""
    #     return getVariances(
    #         config=self.config,
    #         projectname=projectname,
    #         filter=filter,
    #         filter_type=filter_type,
    #         filter_value=filter_value,
    #     )

    # def createVariances(self, projectname: str, variances: list[Variance]) -> None:
    #     """Creates or updates a list of Variances."""
    #     createVariances(config=self.config, projectname=projectname, variances=variances)

    # def deleteVariances(self, variances: list[str]) -> None:
    #     """Deletes a list of Variances by their IDs."""
    #     deleteVariances(config=self.config, variances=variances)

    # def createRules(self, projectname: str, rules: list[Rule]) -> None:
    #     """Creates or updates a list of Rules."""
    #     createRules(config=self.config, projectname=projectname, rules=rules)

    # def deleteRules(self, rules: list[str]) -> None:
    #     """Deletes a list of Rules by their IDs."""
    #     deleteRules(config=self.config, rules=rules)

    def deleteColumns(self, columns: list[str]) -> None:
        """Deletes a list of Imported Columns by their IDs."""
        deleteColumns(config=self.config, columns=columns)

    # def deleteMetrics(self, metrics: list[str]) -> None:
    #     """Deletes a list of Metrics by their IDs."""
    #     deleteMetrics(config=self.config, metrics=metrics)

    # def deleteTiles(self, tiles: list[str]) -> None:
    #     """Deletes a list of Tiles by their IDs."""
    #     deleteTiles(config=self.config, tiles=tiles)

    def deleteAttributeGroups(self, attributegroups: list[str]) -> None:
        """Deletes a list of AttributeGroups by their IDs."""
        deleteAttributeGroups(config=self.config, attributegroups=attributegroups)

    def deleteAttributeLinks(self, attributelinks: list[str]) -> None:
        """Deletes a list of AttributeLinks by their IDs."""
        deleteAttributeLinks(config=self.config, attributelinks=attributelinks)

    # def deletePages(self, pages: list[str]) -> None:
    #     """Deletes a list of Pages by their IDs."""
    #     deletePages(config=self.config, pages=pages)

    # def deleteApplications(self, applications: list[str]) -> None:
    #     """Deletes a list of Pages by their IDs."""
    #     deleteApplications(config=self.config, applications=applications)

    # def deleteDiagrams(self, diagrams: list[str]) -> None:
    #     """Deletes a list of Diagrams by their IDs."""
    #     deleteDiagrams(config=self.config, diagrams=diagrams)

    # def deleteReports(self, reports: list[str]) -> None:
    #     """Deletes a list of Reports by their IDs."""
    #     deleteReports(config=self.config, reports=reports)

    # def deleteProjects(self, projects: list[str]) -> None:
    #     """Deletes a list of Projects by their IDs."""
    #     deleteProjects(config=self.config, projects=projects)

    def createColumns(self, projectname: str, columns: list[Column]) -> None:
        """Creates or updates a list of ImportedColumns."""
        createColumns(config=self.config, projectname=projectname, columns=columns)

    # def createMetrics(self, projectname: str, metrics: list[Metric]) -> None:
    #     """Creates or updates a list of Metrics."""
    #     createMetrics(config=self.config, projectname=projectname, metrics=metrics)

    # def createTiles(self, projectname: str, tiles: list[Tile]) -> None:
    #     """Creates or updates a list of Tiles."""
    #     createTiles(config=self.config, projectname=projectname, tiles=tiles)

    def createAttributeGroups(
        self, projectname: str, attributegroups: list[AttributeGroup]
    ) -> None:
        """Creates or updates a list of AttributeGroups."""
        createAttributeGroups(
            config=self.config, projectname=projectname, attributegroups=attributegroups
        )

    def createAttributeLinks(
        self, projectname: str, attributelinks: list[AttributeLink]
    ) -> None:
        """Creates or updates a list of AttributeLinks."""
        createAttributeLinks(
            config=self.config, projectname=projectname, attributelinks=attributelinks
        )

    # def createPages(self, projectname: str, pages: list[Page]) -> None:
    #     """Creates or updates a list of Pages."""
    #     createPages(config=self.config, projectname=projectname, pages=pages)

    # def createApplications(
    #     self, projectname: str, applications: list[Application]
    # ) -> None:
    #     """Creates or updates a list of Applications."""
    #     createApplications(
    #         config=self.config, projectname=projectname, applications=applications
    #     )

    # def createDiagrams(self, projectname: str, diagrams: list[Diagram]) -> None:
    #     """Creates or updates a list of Diagrams."""
    #     createDiagrams(config=self.config, projectname=projectname, diagrams=diagrams)

    # def createReports(self, projectname: str, reports: list[Report]) -> None:
    #     """Creates or updates a list of Reports."""
    #     createReports(config=self.config, projectname=projectname, reports=reports)

    # def createProjects(self, projects: list[Project]) -> None:
    #     """Creates or updates a list of Projects."""
    #     createProjects(config=self.config, projects=projects)

    def getExportConfiguration(
        self,
        projectname: str,
        configuration_id: str,
        datasource_id: str,
    ) -> dict:
        """Fetches ExportConfigurations metadata."""
        return getExportConfiguration(
            config=self.config,
            projectname=projectname,
            configuration_id=configuration_id,
            datasource_id=datasource_id,
        )

    def createExportConfiguration(
        self,
        projectname: str,
        configuration_id: str,
        datasource_id: str,
        configuration_json: dict,
    ) -> None:
        """Creates or updates an ExportConfiguration."""
        return createExportConfiguration(
            config=self.config,
            projectname=projectname,
            configuration_id=configuration_id,
            datasource_id=datasource_id,
            configuration_json=configuration_json,
        )

    def deleteExportConfiguration(
        self,
        projectname: str,
        configuration_id: str,
        datasource_id: str,
    ) -> None:
        """Deletes an ExportConfiguration."""
        return deleteExportConfiguration(
            config=self.config,
            projectname=projectname,
            configuration_id=configuration_id,
            datasource_id=datasource_id,
        )
