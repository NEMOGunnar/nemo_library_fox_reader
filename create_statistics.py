import csv
import logging
import traceback
from pathlib import Path
from nemo_library_fox_reader.foxcore import FoxNemoLibrary
from nemo_library.utils.config import Config
from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo
from nemo_library_fox_reader.foxstatisticsinfo import FOXStatisticsInfo


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


foxReaderInfo = FOXReaderInfo()

config=Config()
foxreader_statistics_file = config.get_foxreader_statistics_file()

nl = FoxNemoLibrary(foxReaderInfo=foxReaderInfo)


# fox_folder = Path("../fox_files_excerpt") 
fox_folder = Path("fox_files/Demodaten") 
# fox_folder = Path("fox_files/Demodaten/ERP") 

for fox_file in fox_folder.rglob("*.fox"):
    logging.info("*" * 80)
    logging.info(f"Processing FOX file: {fox_file} into project 'FOX_{fox_file.stem}'")

    file_path = str(fox_file)

    # collects information about the current file
    foxReaderInfo.current_file_name = Path(file_path).name
    foxReaderInfo.current_file_path = file_path
    foxReaderInfo.current_fox_version = "unknown"

    try:
        nl.ReUploadFile(
            projectname=f"FOX_{fox_file.stem}",
            filename=file_path,
            update_project_settings=False,
            statistics_only=True,
            foxReaderInfo=foxReaderInfo
        )
    except Exception as exception:
        foxReaderInfo.add_exception(exception)
        if (exception == "list assignment index out of range"):
             logging.info("/////////////////////////////////////////////////////////////////////////////")
        traceback.print_exc()
        # logging.info(f"Exception: {exception}")


# logging.info(f"+++++++++++ foxreader_statistics_file={foxreader_statistics_file}")

        # df.to_csv(
        #     temp_file_path,
        #     index=False,
        #     sep=import_configuration.field_delimiter,
        #     na_rep="",
        #     escapechar=import_configuration.escape_character,
        #     lineterminator=import_configuration.record_delimiter,
        #     quotechar=import_configuration.optionally_enclosed_by,
        #     encoding="UTF-8",
        #     doublequote=False,
        # )
        # logging.info(f"file {temp_file_path} written. Number of records: {len(df)}")

header_row = ["FileName","FilePath","FOX-Version","Issue","Exception","Attribute","Formula","Format","Extra-Info"]

try:
    logging.info("=" * 80)
    with open(foxreader_statistics_file, "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=';', lineterminator='\n', doublequote='"')
            writer.writerow(header_row)
            for info in foxReaderInfo.statistics_infos:
                row = [f"{info.file_name}",f"{info.file_path}",f"{info.fox_version}",f"{info.issue}",f"{info.exception}",f"{info.attribute}",f"{info.formula}",f"{info.format}",f"{info.extra_info}"]
                writer.writerow(row)
                logging.info(f"Issue={info.issue} Ex={info.exception} File={info.file_name} version={info.fox_version} attr={info.attribute} formula={info.formula} format={info.format}")
except Exception as exception:
    logging.info(f"Exception writing CSV.File {exception}")

logging.info("=" * 80)
logging.info(f"Ready #={len(foxReaderInfo.statistics_infos)}")
