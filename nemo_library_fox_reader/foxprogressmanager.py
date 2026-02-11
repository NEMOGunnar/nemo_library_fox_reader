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
        

    allInfos: list[str] = []
    allWarnings: list[str] = []


    # def start(self, total_steps: int):
    #     self.total_steps = total_steps
    #     self.current_step = 0
    #     logging.info(f"Progress started: 0/{self.total_steps}")

    # def step(self):
    #     self.current_step += 1
    #     logging.info(f"Progress: {self.current_step}/{self.total_steps}")

    # def finish(self):
    #     logging.info("Progress finished.")