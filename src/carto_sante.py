import time
from zipfile import Path
import requests
import pandas as pd
import os
import sys
import re 

"""Ce script télécharge les fichiers Excel depuis les URLs fournies,
l'objectif est de récupérer les fichiers Excel depuis carto_sante.

Contrainte : 
- Il faut que les liens des téléchargements soient toujours les mêmes
- Il faut que le format des fichiers Excel soit toujours le même (en-tête à la 9e ligne)
"""
base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_cat_nat"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

# 1. Ajouter le dossier parent de src (le projet) au path pour importer utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import download_file, create_dataframe_communes

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

# On récupère la liste des communes par EPCI pour jointure
df_com = create_dataframe_communes(raw_dir)



# On charge les données de l'onglet récent
#df_apl_medecins = pd.read_excel(chemin_complet, sheet_name=onglet_recent, header=8)
