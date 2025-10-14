import logging
from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo

from nemo_library_fox_reader.foxcore import FoxNemoLibrary


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

foxReaderInfo = FOXReaderInfo()
nl = FoxNemoLibrary(foxReaderInfo=foxReaderInfo)

statistics_only = False


##nl.ReUploadFile(projectname="FOX USI 5 Calendar Sample",filename="D:/Dev/GIT/fox-reader/fox_files/Demodaten/BeispieltabellenInfoZoom2019/Sample Tables 2019/Calendar/Calendar.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 Datumsgruppe und Formeln",filename="../fox_files/Datumsgrupppe und Formeln.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 62 Bekleidungs-Shop mit Formeln und Datumsgruppe",filename="../fox_files/Bekleidungs-Shop mit Formeln und Datumsgrupppe.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 72 Bekleidungs-Shop mit letztem Attribut",filename="../fox_files/Bekleidungs-Shop mit letztem Attribut.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 93 FormelEinsBeispiel",filename="../fox_files/FormelEinsBeispiel.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 94 FormelEins mit Drilldown",filename="../fox_files/FormelEinsBeispielDrilldown.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 99 Städte Deutschland 2018",filename="../fox_files/Städte Deutschland 2018 mit Analyse und Summe.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI Test MultipleValues",filename="../fox_files/MultipleValues-Format.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 73 Printers",filename="../fox_files/printers.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI Printers MultipleValues",filename="../fox_files/Printers with MultipleValues.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 93 Printers",filename="../fox_files/printers without Attributegroup 'Paper sizes'.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX Statistics 4",filename="statistic_files/fox_statistics_09d.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 72 Bekleidungs-Shop",filename="../fox_files/Bekleidungs-Shop 2018 short.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
nl.ReUploadFile(projectname="FOX USI 63 Bekleidungs-Shop",filename="fox_files/Bekleidungs-Shop 2018 mit Analyse.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 99 Angebotsbestand",filename="../fox_files/Demodaten/BUAA/Angebotsbestand_Demodaten/Angebotsbestand_final.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 CeBit 2014 InfoZoomVending",filename="../fox_files/Demodaten/CeBit 2014/Vending/InfoZoomVending.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 93 EinkaufDetail",filename="../fox_files/Demodaten/ERP/EinkaufDetail.fox",update_project_settings=False, statistics_only=statistics_only, foxReaderInfo=foxReaderInfo)
