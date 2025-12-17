import sys
import pandas as pd
import duckdb
import os

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

def main():
    # Define URLs and file paths
    path_cat_nat =  "../data/raw/cat_nat.csv"
    df_cat_nat = pd.read_csv(path_cat_nat,skiprows=2)

    #Mise en forme des données
    mapping = {
    "Code": "siren",
    "Libellé": "nom_epci",
    "Nombre d'Arrêtés de Catastrophes Naturelles publiés au J.O.": "nb_cat_nat"
    }
    df_cat_nat = df_cat_nat.rename(columns=mapping)
    df_cat_nat.loc[df_cat_nat['siren'] == 75056, 'siren'] = 200054781   
    print("Données cat_nat chargées et renommées.")

    # Création de la table duckdb pour les jointures
    com_url = (
        "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"
    )
    extract_path = "../data/data_cat_nat/raw"
    download_file(com_url, extract_to=extract_path, filename="communes_france_2025.csv")
    df_com = duckdb.read_csv(os.path.join(extract_path, "communes_france_2025.csv"))

    #Surface de chaque epci
    query = """
    SELECT 
        epci_code as siren,
        sum(superficie_km2) AS superficie_km2
    FROM df_com
    WHERE (superficie_km2 IS NOT NULL) AND (epci_code != 'ZZZZZZZZZ')
    GROUP BY epci_code
    """

    df_surface_epci = duckdb.sql(query)

    query = """
    SELECT e.siren,
        df_cat_nat.nb_cat_nat,
        ROUND(1.0*df_cat_nat.nb_cat_nat / e.superficie_km2, 3) AS cat_nat_per_km2
    FROM df_surface_epci AS e
    LEFT JOIN df_cat_nat
    ON e.siren = df_cat_nat.siren
    """

    df_cat_nat_final = duckdb.sql(query)

    #Sauvegarde du fichier final
    if not os.path.exists("../data/data_cat_nat/processed/"):
        os.makedirs("../data/data_cat_nat/processed/")
    df_cat_nat_final.write_csv("../data/data_cat_nat/processed/cat_nat_per_epci.csv")

    print("Fichier sauvegardé : ../data/data_cat_nat/processed/cat_nat_per_epci.csv")


if __name__ == "__main__":
    main()  # asso.py  

