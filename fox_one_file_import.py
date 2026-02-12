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
##nl.ReUploadFile(projectname="FOX USI 93 Printers",filename="fox_files/printers without Attributegroup 'Paper sizes'.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX Statistics 2025-10-29",filename="statistic_files/fox_statistics_2025-10-29.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 69 Bekleidungs-Shop",filename="fox_files/Bekleidungs-Shop 2018 short.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 57 Bekleidungs-Shop",filename="fox_files/Bekleidungs-Shop 2018 mit Analyse.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 99 Angebotsbestand",filename="fox_files/Demodaten/BUAA/Angebotsbestand_Demodaten/Angebotsbestand_final.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 CeBit 2014 InfoZoomVending",filename="../fox_files/Demodaten/CeBit 2014/Vending/InfoZoomVending.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 91 EinkaufDetail",filename="fox_files/Demodaten/ERP/EinkaufDetail.fox",update_project_settings=False, statistics_only=statistics_only, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 89 Elektronikfachhandel Umsatz",filename="fox_files/Demodaten/BeispieltabellenInfoZoom2019/Beispieltabellen 2019/Umsatzanalyse/Elektronikfachhandel/Umsatz.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 95 Erste Bundesliga Trivialsport",filename="fox_files/Demodaten/BeispieltabellenInfoZoom2019/beispieldateien-cici/ErsteBundesliga.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 92 Movies",filename="fox_files/Demodaten/Movies/Movies.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 Analyzer Master Empty",filename="fox_files/Demodaten/proALPHA Analyzer Master/master_empty.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 Angebotsbestand",filename="fox_files/Demodaten/BUAA/Angebotsbestand.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 90 ABC-Kunden (without Trunc)",filename="fox_files/ABC-Kunden without trunc.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 84with ABC-Kunden (reduced with Trunc)",filename="fox_files/ABC-Kunden reduced with TRUNC.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 83without ABC-Kunden (reduced without Trunc)",filename="fox_files/ABC-Kunden reduced without TRUNC.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 57 ABC-Kunden",filename="fox_files/Demodaten/ABC-Kunden/ABC-Kunden.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 81 Prüfung offene RG",filename="fox_files/Demodaten/Prüfsoftware/Prüfung_offene_RG.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 90 Prüfung offene RG",filename="fox_files/Prüfung_offene_RG (ohne geschachtelte Gruppen).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 96 Formula with Links",filename="fox_files/Formula with Links.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 94 Buchausleihe",filename="fox_files/Demodaten/Bibliothek_CRM/Buchausleihe.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 Krankenkasse",filename="fox_files/Demodaten/Krankenkasse/Krankenkasse_neu.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 90 Autos",filename="fox_files/Autos.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 Autos (Coupled Group)",filename="fox_files/Autos (Coupled Group).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 95 PDH_PROD_PMCC_Extract 1",filename="fox_files/PDH_PROD_PMCC_Extract 1.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 74 Northwind (DateTime)",filename="fox_files/Nordwind (DateTime).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 39 Nordwind (CeBit 2014)",filename="fox_files/Nordwind.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 90 PDH_PROD_PMCC_DQChecks_Reduced 7b",filename="fox_files/PDH_PROD_PMCC_DQChecks_Reduced 7b.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 90 PDH_PROD_PMCC_Description_reduced",filename="fox_files/PDH_PROD_PMCC_Description_Reduced.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 85 PDH_PROD_PMCC_Description_reduced 5 (GTIN-EndUse)",filename="fox_files/PDH_PROD_PMCC_Description_Reduced 5 (GTIN-EndUse).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 72 PDH_PROD_PMCC_Description_reduced 5g (EAN)",filename="fox_files/PDH_PROD_PMCC_Description_Reduced 5g (EAN).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 71 PDH_PROD_PMCC_Description_Reduced 5j (-EndUse Application)",filename="fox_files/PDH_PROD_PMCC_Description_Reduced 5j (-EndUse Application).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 PDH_PROD_PMCC_Makelist",filename="fox_files/PDH_PROD_PMCC_Makelist.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 99 Nordwind Umsatz pro Jahr",filename="fox_files/Nordwind.Umsatz pro Jahr.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 92 Nordwind (Umsatz+Geburtstag)",filename="fox_files/Nordwind (Umsatz+Geburtstag).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 93 PDH_PROD_PMCC_Reduced (MaxInt+TooLong)",filename="fox_files/PDH_PROD_PMCC_Reduced (MaxInt+TooLong).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 97 PDH_PROD_PMCC_Reduced (TooLong)",filename="fox_files/PDH_PROD_PMCC_Reduced (TooLong).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 93 PDH_PROD_PMCC_Reduced (TooLong with Length)",filename="fox_files/PDH_PROD_PMCC_Reduced (TooLong with Length).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 95 PDH_PROD_PMCC_Reduced (TooLong Without Makelist)",filename="fox_files/PDH_PROD_PMCC_Reduced (TooLong Without Makelist).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 99 PDH_PROD_PMCC_Reduced (MaxInt)",filename="fox_files/PDH_PROD_PMCC_Reduced (MaxInt).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 98 PDH_PROD_PMCC_Reduced (CATALOG_NUMBER)",filename="fox_files/PDH_PROD_PMCC_Reduced (CATALOG_NUMBER).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 99 Fox2Nemo Test_Ber",filename="fox_files/Demodaten/Fox2Nemo/Test_Ber.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
##nl.ReUploadFile(projectname="FOX USI 48 PDH_PROD_PMCC_Global_Attributes_incl_SAP_2025-11-testing-nemo.01",filename="fox_files/PDH_PROD_PMCC_Global_Attributes_incl_SAP_2025-11-testing-nemo.01.fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)
nl.ReUploadFile(projectname="FOX USI 99 PDH_PROD_PMCC_Reduced (SAP Status)",filename="fox_files/PDH_PROD_PMCC_Reduced (SAP Status).fox",update_project_settings=False, statistics_only=False, foxReaderInfo=foxReaderInfo)



#python fox_one_file_import.py