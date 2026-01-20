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

    # Télécharger et extraire les données médiation
    download_file(url, extract_to=raw_dir, filename="mediation_numerique.csv")
    df_mediation_num = pd.read_csv(mediation_file, low_memory=False)

    # Télecharger les données communes
    df_com = create_dataframe_communes(raw_dir)

    # Création de df_epci
    df_epci = create_dataframe_epci(raw_dir)

    # On ne garde que les colonnes siren, nom_epci et dept
    query = """
    SELECT DISTINCT
        siren,
        raison_sociale AS nom_epci,
        dept
    FROM df_epci
    """
    df_epci = duckdb.sql(query)

    # Regroupement par code_insee communes
    query = """ 
    SELECT 
        count(id) AS nb_mediation,
        code_insee
    FROM df_mediation_num
    GROUP BY code_insee
    """
    df_mediation_num_grouped = duckdb.sql(query)

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
        e2.siren,
        e2.nom_epci,
        e2.dept,
        e1.nb_mediation_epci,
        round(10000 * e1.nb_mediation_epci / e1.population_epci, 2) AS mediation_per_10k_habs 
    FROM df_epci_mediation AS e1
    LEFT JOIN df_epci AS e2
    ON e1.siren = e2.siren
    ORDER BY e2.dept,e2.siren
    """

    df_mediation_num_final = duckdb.sql(query)

    # Sauvegarde du fichier final
    output_file = processed_dir / "mediation_numerique.csv"
    df_mediation_num_final .write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")

    query_bdd = """
    SELECT 
        siren as id_epci,
        'i095' AS id_indicator,
        mediation_per_10k_habs as valeur_brute,
        '2026' AS annee
    FROM df_mediation_num_final
    """

    df_mediation_num_bdd = duckdb.sql(query_bdd)

    #sauvegarde pour la bdd
    output_file_bdd = processed_dir / "mediation_numerique_bdd.csv"
    df_mediation_num_bdd.write_csv(str(output_file_bdd))
    print(f"Fichier sauvegardé pour la bdd : {output_file_bdd}")


if __name__ == "__main__":
    main()  # mediation_numerique.py
