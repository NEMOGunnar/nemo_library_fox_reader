import logging
from nemo_library_fox_reader.foxfile import FOXFile
from nemo_library_fox_reader.foxmeta import FOXMeta


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

filename = "./Autodatenbank_2019.fox"

foxfile = FOXFile(filename)
try:
    
    df = foxfile.read()
    meta = FOXMeta(foxfile)
    meta.reconcile_metadata(projectname=projectname)
finally:
    foxfile.close()
