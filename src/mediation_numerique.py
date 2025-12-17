import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_medi_num"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

def main():
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/398edc71-0d51-4cb6-9cbe-2540a4db573c"
    )

    mediation_file = raw_dir / "mediation_numerique.csv"

    # Télécharger et extraire les données
    download_file(url, extract_to=raw_dir, filename="mediation_numerique.csv")
    df_mediation_num = pd.read_csv(mediation_file, low_memory=False)

    # Regroupement par code_insee communes
    query = """ 
    SELECT 
        count(id) AS nb_mediation,
        code_insee
    FROM df_mediation_num
    GROUP BY code_insee
    """

    df_mediation_num_grouped = duckdb.sql(query)
    print(df_mediation_num_grouped.df().head())

    # Téléchargement des données communes
    com_url = (
        "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"
    )

    communes_file = raw_dir / "communes_france_2025.csv"

    download_file(com_url, extract_to=raw_dir, filename="communes_france_2025.csv")
    df_com = pd.read_csv(communes_file, low_memory=False)


    # Jointure des données
    query = """ 
    SELECT
        df_com.epci_code AS siren,
        SUM(df_mediation_num_grouped.nb_mediation) AS nb_mediation_epci,
        SUM(df_com.population) AS population_epci
    FROM df_com
    LEFT JOIN df_mediation_num_grouped
    ON df_com.code_insee = df_mediation_num_grouped.code_insee
    WHERE epci_code::VARCHAR NOT LIKE '%ZZZZ'
    GROUP BY df_com.epci_code
    """
    df_epci_mediation = duckdb.sql(query)
    print(df_epci_mediation.df().head())

    # dataframe final avec le nombre de médiation numérique pour 10000 habitants
    query = """ 
    SELECT 
        siren,
        nb_mediation_epci,
        round(10000 * nb_mediation_epci / population_epci, 2) AS mediation_per_10k_habs 
    FROM df_epci_mediation
    ORDER BY siren
    """

    df_mediation_num_final = duckdb.sql(query)
    print(df_mediation_num_final.df().head())

    # Sauvegarde du fichier final
    output_file = processed_dir / "mediation_numerique.csv"
    df_mediation_num_final.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")

if __name__ == "__main__":
    main()  # mediation_numerique.py
