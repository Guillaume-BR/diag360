import time
from zipfile import Path
import requests
import pandas as pd
import os
import sys
import re 
import duckdb

"""Ce script télécharge les fichiers Excel depuis les URLs fournies,
l'objectif est de récupérer les fichiers Excel depuis carto_sante.

Contrainte : 
- Il faut que les liens des téléchargements soient toujours les mêmes
- Il faut que le format des fichiers Excel soit toujours le même (en-tête à la 9e ligne)
- Il faut que la colonne "Code commune INSEE" soit toujours présente dans les fichiers Excel
"""
base_dir = os.path.dirname(os.path.dirname(__file__))  # racine du projet diag360

raw_dir = os.path.join(base_dir, "data", "data_cat_nat", "raw")

os.makedirs(raw_dir, exist_ok=True)
# 1. Ajouter le dossier parent de src (le projet) au path pour importer utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import download_file, float_to_codepostal

# 2. On récupère le dossier "diag360" (parent de "src")
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 3. On définit le chemin pour les fichiers que l'on veut créer
dossier_source = os.path.join(root_dir, "fichier_source")


# 4. On s'assure que le dossier existe avant d'appeler la fonction
os.makedirs(dossier_source, exist_ok=True)

# Définition des URLs et des fichiers correspondants
urls_dict = {
    "data_apl_medecins.xlsx" : "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_medecins_generalistes_xlsx/",
    "data_apl_infirmiers.xlsx" : "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_infirmieres_xlsx/",
    "data_apl_chirurgiens_dentiste.xlsx": "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_chirurgiens_dentistes_xlsx/",
    "data_apl_sages_femmes.xlsx" : "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_sages_femmes_xlsx/",
        }
chemin_complet_urls = [os.path.join(dossier_source, k) for k,v in urls_dict.items()]

for k, v in urls_dict.items():
    download_file(v, dossier_source, k)

dict_fichier_onglet = {}
for file in chemin_complet_urls:
    onflet_fichier_xlsx = pd.ExcelFile(file).sheet_names
    # On cherche exactement 4 chiffres (\d{4}) pour les années
    # On convertit en int pour pouvoir les comparer plus tard
    onglets_avec_annees = {}
    for nom in onflet_fichier_xlsx:
        match = re.search(r'\d{4}', nom)
        if match:
            onglets_avec_annees[nom] = int(match.group())
    # On récupère l'onglet avec l'année la plus récente
        nom_onglet_recent = [k for k, v in onglets_avec_annees.items() if v == max(onglets_avec_annees.values())]
        # 5. On remplit le dictionnaire final : { nom_fichier : nom_onglet }
    dict_fichier_onglet[file] = nom_onglet_recent[0]

df_apl_medecins = pd.read_excel(
    chemin_complet_urls[0],
    sheet_name=dict_fichier_onglet[chemin_complet_urls[0]],
    header=8,
)
df_apl_infirmiers = pd.read_excel(
    chemin_complet_urls[1],
    sheet_name=dict_fichier_onglet[chemin_complet_urls[1]],
    header=8,
)
df_apl_chirurgien_dentiste = pd.read_excel(
    chemin_complet_urls[2],
    sheet_name=dict_fichier_onglet[chemin_complet_urls[2]],
    header=8,
)
df_apl_sage_femme = pd.read_excel(
    chemin_complet_urls[3],
    sheet_name=dict_fichier_onglet[chemin_complet_urls[3]],
    header=8,
)


# On récupère la liste des communes par EPCI pour jointure
com_url = (
        "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"
    )
download_file(com_url, extract_to=raw_dir, filename="communes_france_2025.csv")
df_com = pd.read_csv(os.path.join(raw_dir, "communes_france_2025.csv"))
df_com = float_to_codepostal(df_com, "code_postal")

def commune_to_epci(table_to_join, table_ref):
    query = f"""
    SELECT  dam.*, dc.epci_code AS siren, dc.epci_nom AS epci_nom
    FROM {table_to_join} as dam
    JOIN {table_ref} as dc
        on dam."Code commune INSEE" = dc.code_insee
    """
    return duckdb.sql(query).df()


medecin_epci = commune_to_epci("df_apl_medecins","df_com")
infirmier_epci = commune_to_epci("df_apl_infirmiers","df_com")
chirurgien_dentiste_epci = commune_to_epci("df_apl_chirurgien_dentiste","df_com")
sage_femme_epci = commune_to_epci("df_apl_sage_femme","df_com")

