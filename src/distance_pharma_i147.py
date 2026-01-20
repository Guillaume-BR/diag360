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
    # chargement des données
    df_dist_pharma = duckdb.read_csv(raw_dir / "dist_pharma.csv", skiprows=2)

    # Création du dataframe des communes (cf functions.py)
    df_com = create_dataframe_communes(raw_dir)

    # Changement des noms de colonnes
    mapping_pharma = {
        "Code": "code_insee",
        "Libellé": "nom_commune",
        "Distance à la pharmacie la plus proche 2024": "dist_pharma_min",
    }

    df_dist_pharma = df_dist_pharma.df().rename(columns=mapping_pharma)
    print(df_dist_pharma.head())

    # Jointure des données distance moyenne aux pharmacies
    query = """
    SELECT
        DISTINCT epci_code as id_epci,
        'i147' AS id_indicator,
        ROUND(AVG(TRY_CAST(dist_pharma_min AS DOUBLE)),2) AS valeur_brute,
        '2024' AS annee
    FROM df_com
    LEFT JOIN df_dist_pharma
    ON df_com.code_insee = df_dist_pharma.code_insee
    WHERE epci_code != 'ZZZZZZZZZ'
    GROUP BY epci_code
    """

    df_dist_pharma = duckdb.sql(query)
    print(f"taille df_dist_pharma: {df_dist_pharma.df().shape}")

    # Sauvegarde du fichier final
    output_file = processed_dir / "dist_pharma_per_epci.csv"
    df_dist_pharma.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()
