import sys
import pandas as pd
import duckdb
import os
from pathlib import Path


# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_sau"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # Téléchargement de la table com
    df_com = create_dataframe_communes(raw_dir)

    # Téléchagement de la table de la sau
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/022cb00f-38f2-4fe7-8895-e3467d3d9255"
    )
    download_file(url, extract_to=raw_dir, filename="sau_2025.csv")
    df_sau = pd.read_csv(raw_dir / "sau_2025.csv", sep=",")

    # calcul de la superficie des EPCI à partir des communes
    query = """
    SELECT 
        epci_code AS siren,
        SUM(superficie_km2) AS superficie_km2
    FROM df_com
    WHERE (superficie_km2 IS NOT NULL) AND (epci_code != 'ZZZZZZZZZ')
    GROUP BY epci_code
    """

    df_surface_epci = duckdb.sql(query)
    print(f"df_surface_epci.shape: {df_surface_epci.df().shape}")

    # Traitement de la table sau
    df_sau = df_sau[df_sau["date_mesure"].str.startswith("2020")]

    # Jointure entre df_sau et df_surface_epci
    query = """
    SELECT
        df_sau.geocode_epci AS id_epci,
        'i114' AS id_indicator,
        ROUND((df_sau.valeur / 100) / df_surface_epci.superficie_km2 * 100,1) AS valeur_brute,
        '2025' AS annee
    FROM df_sau
    LEFT JOIN df_surface_epci
    ON df_sau.geocode_epci = df_surface_epci.siren
    WHERE df_sau.geocode_epci != 'ZZZZZZZZZ'
    """
    df_sau_merged = duckdb.sql(query)

    # Sauvegarde des données
    df_sau_merged.write_csv(str(processed_dir / "part_sau_sur_total.csv"))
    print("Données sauvegardées dans part_sau_sur_total.csv")


if __name__ == "__main__":
    main()
