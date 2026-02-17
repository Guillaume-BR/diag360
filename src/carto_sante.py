import pandas as pd
import os
import sys
import duckdb
from math import *

# Ajouter le dossier parent de src (le projet) au path pour importer utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.download_data import download_data
from utils.format_file import format_file

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
df_epci = pd.read_csv("./data/processed/epci_membres.csv",sep=',')

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

#On modifie le code_insee de paris pour mettre 75056
df_apl_medecins["Code commune INSEE"] = df_apl_medecins["Code commune INSEE"].astype(str).apply(lambda x: "75056" if x.startswith("75") else x)
df_apl_infirmiers["Code commune INSEE"] = df_apl_infirmiers["Code commune INSEE"].astype(str).apply(lambda x: "75056" if x.startswith("75") else x)
df_apl_chirurgiens_dentiste["Code commune INSEE"] = df_apl_chirurgiens_dentiste["Code commune INSEE"].astype(str).apply(lambda x: "75056" if x.startswith("75") else x)
df_apl_sages_femmes["Code commune INSEE"] = df_apl_sages_femmes["Code commune INSEE"].astype(str).apply(lambda x: "75056" if x.startswith("75") else x)

# Jointure avec les communes et EPCI pour le format final
sql_medecins = """
COPY (
SELECT
    c.dept_epci as dept_id,
    c.siren as id_epci,
    c.epci_nom as epci_lib,
    'i064' as id_indicator,
    -- sum(CAST("APL aux médecins généralistes de 65 ans et moins " as float)) AS total_apl_medecins_moins_65_ans_ou_moins,
    -- count(*) as nb_apl_medecins_records,
    ROUND(sum(CAST(#3 as float) * CAST(#4 as float))/sum(CAST(#4 as float)),1) AS valeur_brute,
    '2025' AS annee
    FROM df_epci AS c
    LEFT JOIN df_apl_medecins AS d
    ON d."Code commune INSEE" = c.code_insee
    GROUP BY c.dept_epci, c.siren, c.epci_nom
    ORDER BY c.dept_epci, c.siren
    )
    TO './data/processed/i064_apl_medecins_clean.csv' (HEADER, DELIMITER ';');
"""

sql_infirmiers = """
COPY (
SELECT     c.dept_epci as dept_id,
    c.siren as id_epci,
    c.epci_nom as epci_lib,
    'i067' as id_indicator,
    -- sum(CAST("APL aux infirmières" as float)) AS total_apl_infirmiers_moins_65_ans_ou_moins,
    -- count(*) as nb_apl_infirmiers_records,
    ROUND(sum(CAST(#3 as float) * CAST(#4 as float))/sum(CAST(#4 as float)),1) AS valeur_brute,
        '2025' AS annee
    FROM df_epci AS c
    LEFT JOIN df_apl_infirmiers AS d
    ON d."Code commune INSEE" = c.code_insee
    GROUP BY c.dept_epci, c.siren, c.epci_nom
    ORDER BY c.dept_epci, c.siren
    )
    TO './data/processed/i067_apl_infirmiers_clean.csv' (HEADER, DELIMITER ';');
"""
sql_chirurgiens_dentiste = """
COPY (
SELECT 
c.dept_epci as dept_id,
    c.siren as id_epci,
    c.epci_nom as epci_lib,
    'i069' as id_indicator,
    -- sum(CAST("APL aux chirurgiens-dentistes" as float)) AS total_apl_chirurgiens_dentiste_moins_65_ans_ou_moins,
    -- count(*) as nb_apl_chirurgiens_dentiste_records,
    ROUND(sum(CAST(#3 as float) * CAST(#4 as float))/sum(CAST(#4 as float)),1) AS valeur_brute,
    '2025' AS annee
    FROM df_epci AS c
    LEFT JOIN df_apl_chirurgiens_dentiste AS d
    ON d."Code commune INSEE" = c.code_insee
    GROUP BY c.dept_epci, c.siren, c.epci_nom
    ORDER BY c.dept_epci, c.siren
    )
    TO './data/processed/i069_apl_chirurgiens_dentiste_clean.csv' (HEADER, DELIMITER ';');
"""
sql_sages_femmes = """
COPY (
SELECT c.dept_epci as dept_id,
    c.siren as id_epci,
    c.epci_nom as epci_lib,
    'i068' as id_indicator,
    -- sum(CAST("APL aux sages-femmes" as float)) AS total_apl_sages_femmes_moins_65_ans_ou_moins,
    -- count(*) as nb_apl_sages_femmes_records,
    ROUND(sum(CAST(#3 as float) * CAST(#4 as float))/sum(CAST(#4 as float)),1) as valeur_brute,
    '2025' AS annee
    FROM df_epci AS c
    LEFT JOIN df_apl_sages_femmes AS d
    ON d."Code commune INSEE" = c.code_insee
    GROUP BY c.dept_epci, c.siren, c.epci_nom
    ORDER BY c.dept_epci, c.siren
    )
    TO './data/processed/i068_apl_sages_femmes_clean.csv' (HEADER, DELIMITER ';');
"""

con = duckdb.connect(database=':memory:')
con.execute(sql_medecins)
con.execute(sql_sages_femmes)
con.execute(sql_infirmiers)
con.execute(sql_chirurgiens_dentiste) 
con.close()
