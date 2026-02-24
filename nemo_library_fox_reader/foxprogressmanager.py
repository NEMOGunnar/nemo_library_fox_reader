from datetime import datetime
import logging
from nemo_library_fox_reader.foxstatisticsinfo import FOXStatisticsInfo, IssueType


class FOXProgressManager:

   
    def __init__(self):
        self.total_steps = 0
        self.current_step = 0

    @staticmethod
    def info(message: str):
        logging.info(message)
        FOXProgressManager.allInfos.append(message)

    @staticmethod
    def warning(message: str):
        logging.warning(message)
        FOXProgressManager.allWarnings.append(message)
        
    @staticmethod
    def start():
        FOXProgressManager.start_time = datetime.now().replace(microsecond=0)
        
    @staticmethod
    def finish():
        FOXProgressManager.finish_time = datetime.now().replace(microsecond=0)
        duration = FOXProgressManager.finish_time - FOXProgressManager.start_time
        logging.info(f"FileIngestion took {duration}   start={FOXProgressManager.start_time}   finish={FOXProgressManager.finish_time}  ")
        

    allInfos: list[str] = []
    allWarnings: list[str] = []
    start_time: datetime = None 
    finish_time: datetime = None

