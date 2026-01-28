import sys
import pandas as pd
import duckdb
import os
from pathlib import Path


# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_phyto"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # Téléchargement de la table epci
    df_epci = create_dataframe_epci(raw_dir)

    # Téléchagement de la table de la sau
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/022cb00f-38f2-4fe7-8895-e3467d3d9255"
    )
    download_file(url, extract_to=raw_dir, filename="sau_2025.csv")
    df_sau = pd.read_csv(raw_dir / "sau_2025.csv", sep=",")
    print(df_sau.head())

    # Téléchargement de la table phyto
    url_phyto = (
        "https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c"
    )
    download_file(url_phyto, extract_to=raw_dir, filename="achat_commune_phyto.parquet")
    df_phyto = duckdb.read_parquet(str(raw_dir / "achat_commune_phyto.parquet"))

    #Préparation de df_sau : on ne garde que 2020
    query_sau = """ 
    SELECT 
        geocode_epci, 
        ROUND(TRY_CAST(valeur AS DOUBLE), 2) AS sau_ha
    FROM df_sau
    WHERE geocode_epci NOT LIKE 'Z%' AND date_mesure LIKE '2020%'
    """
    
    df_sau = duckdb.sql(query_sau)

    #query filtrer df_epci
    query =  """
    SELECT
        DISTINCT siren,
        raison_sociale,
        dept
    FROM df_epci
    """
    df_epci_filtered = duckdb.sql(query)

    # Jointure entre df_epci et df_phyto
    query = """
    SELECT 
        df_phyto.annee,
        df_epci.siren,
        TRY_CAST(df_phyto.quantite_substance AS DOUBLE) AS quantite_substance
    FROM df_epci 
    INNER JOIN df_phyto 
    ON df_epci.insee = df_phyto.code_insee
    """

    df_phyto_merged = duckdb.sql(query)
    print(f"df_phyto_merged.shape: {df_phyto_merged.df().shape}")

    # Calcul de la moyenne annuelle par EPCI
    query_avg = """ 
    WITH df_temp AS (
        SELECT
            siren,
            COUNT(DISTINCT annee) AS n_years,
            SUM(quantite_substance) AS total_quantite_substance
        FROM df_phyto_merged
        GROUP BY siren
    )

    SELECT
        siren as id_epci,
        (1.0*total_quantite_substance / n_years) AS avg_annual_phyto
    FROM df_temp
    """

    avg_annual_phyto = duckdb.sql(query_avg)

    query_bdd = """
    SELECT
        aap.siren as id_epci,
        'i114' AS id_indicator,
        ROUND((1.0 * aap.avg_annual_phyto / ds.sau_ha), 3) AS valeur_brute,
        '2023' AS annee
    FROM avg_annual_phyto AS aap
    INNER JOIN df_sau AS ds
        ON aap.siren = ds.geocode_epci
    """

    df_final = duckdb.sql(query_bdd)

    # Sauvegarde du fichier final
    output_path = processed_dir / "phyto_epci_sau.csv"
    df_final.to_csv(output_path, index=False)
    print(f"Fichier sauvegardé : {output_path}")

    #tableau complet
    query_complete = """ 
    SELECT
        df_epci.dept,
        df_epci.siren AS id_epci,
        df_epci.raison_sociale AS nom_epci,
        df_final.id_indicator,
        df_final.valeur_brute
    FROM df_epci
    LEFT JOIN df_final
        ON df_epci.siren = df_final.id_epci
    """

    df_complete = duckdb.sql(query_complete)
    output_complete_path = processed_dir / "i114_phyto_epci_sau_complete.csv"
    df_complete.to_csv(output_complete_path, index=False)
    print(f"Fichier complet sauvegardé : {output_complete_path}")


if __name__ == "__main__":
    main()
