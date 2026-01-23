import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_dist_soin"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # chargement des données des urgences
    df_dist_urg = duckdb.read_csv(raw_dir / "dist_urgence.csv", skiprows=2)

    # Création du dataframe des communes (cf functions.py)
    df_com = create_dataframe_communes(raw_dir)

    # Création de la table duckdb des epci
    df_epci = create_dataframe_epci(raw_dir)

    # création de la table du code epci et du nom associé
    query = """
    SELECT 
        DISTINCT siren,
        raison_sociale AS nom_epci,
        dept
    FROM df_epci
    """
    df_epci = duckdb.sql(query)
    print(f"df_epci.shape: {df_epci.df().shape}")

    # Changement des noms de colonnes
    mapping_urg = {
        "Code": "code_insee",
        "Libellé": "nom_commune",
        "Distance à la structure la plus proche 2024": "dist_urgence_min",
    }

    df_dist_urg = df_dist_urg.df().rename(columns=mapping_urg)

    # Jointure des données distance moyenne aux urgences
    query = """
    SELECT 
        DISTINCT epci_code as siren,
        ROUND(AVG(TRY_CAST(dist_urgence_min AS DOUBLE)),2) AS dist_urgence_moyenne_km
    FROM df_com
    LEFT JOIN df_dist_urg
    ON df_com.code_insee = df_dist_urg.code_insee
    WHERE epci_code != 'ZZZZZZZZZ'
    GROUP BY epci_code
    """

    df_dist_urg_moy = duckdb.sql(query)
    print(f"taille df_dist_urg_moy: {df_dist_urg_moy.df().shape}")

    # Jointure des deux dataframes et du dataframe des epci
    query = """ 
    SELECT
        d.siren as id_epci,
        'i148' AS id_indicator,
        e.dist_urgence_moyenne_km as valeur_brute,
        '2024' AS annee
    FROM df_epci d
    LEFT JOIN df_dist_urg_moy e
    ON d.siren = e.siren
    """

    df_dist_soin_final = duckdb.sql(query)

    # Sauvegarde du fichier final
    output_file = processed_dir / "dist_urgence_per_epci.csv"
    df_dist_soin_final.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()
