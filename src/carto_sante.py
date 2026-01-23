import pandas as pd
import os
import sys
import duckdb

# Ajouter le dossier parent de src (le projet) au path pour importer utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.download_data import download_data
from utils.format_file import format_file
from utils.functions import create_dataframe_communes

# Définition des URLs et des fichiers correspondants
urls_dict = {
    "data_apl_medecins.xlsx" : "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_medecins_generalistes_xlsx/",
    "data_apl_infirmiers.xlsx" : "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_infirmieres_xlsx/",
    "data_apl_chirurgiens_dentiste.xlsx": "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_chirurgiens_dentistes_xlsx/",
    "data_apl_sages_femmes.xlsx" : "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_sages_femmes_xlsx/",
        }
init_download = download_data(url=urls_dict, name_file_saved="")
init_download.dict_download_file()
#Téléchargement du fichier des communes et EPCI
df_commune_epci = create_dataframe_communes(dir_path="./data/processed")
# Formating des fichiers reçu pour avoir un dataframe exploitable
apl_medecins = format_file(path=init_download.save_path + "/" + "data_apl_medecins.xlsx",extension="xlsx")
apl_infirmiers = format_file(path=init_download.save_path + "/" + "data_apl_infirmiers.xlsx",extension="xlsx")
apl_chirurgiens_dentiste = format_file(path=init_download.save_path + "/" + "data_apl_chirurgiens_dentiste.xlsx",extension="xlsx")
apl_sages_femmes = format_file(path=init_download.save_path + "/" + "data_apl_sages_femmes.xlsx",extension="xlsx")
# Dataframe
df_apl_medecins = apl_medecins.read_file()
df_apl_infirmiers = apl_infirmiers.read_file()
df_apl_chirurgiens_dentiste = apl_chirurgiens_dentiste.read_file()
df_apl_sages_femmes = apl_sages_femmes.read_file()

# Jointure avec les communes et EPCI pour le format final
sql_medecins = """
COPY (
SELECT c.epci_code,
    c.epci_nom,
    "Code commune INSEE" AS code_insee,
    "APL aux médecins généralistes de 65 ans et moins " AS apl_medecins
    from df_apl_medecins AS d
    INNER JOIN df_commune_epci AS c
    ON d."Code commune INSEE" = c.code_insee)
    TO './data/processed/apl_medecins_clean.csv' (HEADER, DELIMITER ';');
"""
sql_infirmiers = """
COPY (
SELECT c.epci_code,
    c.epci_nom,
    "Code commune INSEE" AS code_insee,
    "APL aux infirmières" AS apl_infirmiers
    from df_apl_infirmiers AS d
    INNER JOIN df_commune_epci AS c
    ON d."Code commune INSEE" = c.code_insee)
    TO './data/processed/apl_infirmiers_clean.csv' (HEADER, DELIMITER ';');
"""
sql_chirurgiens_dentiste = """
COPY (
SELECT c.epci_code,
    c.epci_nom,
    "Code commune INSEE" AS code_insee,
    "APL aux chirurgiens-dentistes" AS apl_chirurgiens_dentiste
    from df_apl_chirurgiens_dentiste AS d
    INNER JOIN df_commune_epci AS c
    ON d."Code commune INSEE" = c.code_insee)
    TO './data/processed/apl_chirurgiens_dentiste_clean.csv' (HEADER, DELIMITER ';');
"""
sql_sages_femmes = """
COPY (
SELECT c.epci_code,
    c.epci_nom,
    "Code commune INSEE" AS code_insee,
    "APL aux sages-femmes" AS apl_sages_femmes
    from df_apl_sages_femmes AS d
    INNER JOIN df_commune_epci AS c
    ON d."Code commune INSEE" = c.code_insee)
    TO './data/processed/apl_sages_femmes_clean.csv' (HEADER, DELIMITER ';');
"""

con = duckdb.connect(database=':memory:')
con.execute(sql_medecins)
con.execute(sql_sages_femmes)
con.execute(sql_infirmiers)
con.execute(sql_chirurgiens_dentiste) 