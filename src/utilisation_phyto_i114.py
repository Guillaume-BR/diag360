import sys
import pandas as pd
import duckdb
import os
from pathlib import Path
from io import BytesIO
import pyarrow.parquet as pq



# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import download_file, download_stock_file

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_phyto"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # Téléchargement de la table epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=',')

    # Téléchagement de la table de la sau
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/022cb00f-38f2-4fe7-8895-e3467d3d9255"
    )
    content = download_file(url)
    df_sau = pd.read_csv(BytesIO(content), sep=",")
    print(df_sau.head())

    # Téléchargement de la table phyto
    url_phyto = (
        "https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c"
    )

    #download_stock_file(url: str, extract_to: str = ".", filename: str = None)
    download_stock_file(url_phyto, raw_dir, "achat_commune_phyto.parquet")
    print("coucou")
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

    # Jointure entre df_epci et df_phyto
    query = """
    SELECT 
        df_phyto.annee,
        df_epci.siren,
        TRY_CAST(df_phyto.quantite_substance AS DOUBLE) AS quantite_substance
    FROM df_epci 
    INNER JOIN df_phyto 
    ON df_epci.code_insee = df_phyto.code_insee
    """

    df_phyto_merged = duckdb.sql(query)

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
        siren,
        (1.0*total_quantite_substance / n_years) AS avg_annual_phyto
    FROM df_temp
    """

    avg_annual_phyto = duckdb.sql(query_avg)

    query_bdd = """
    WITH epci AS (
    SELECT 
        DISTINCT siren, 
        dept_epci, 
        epci_nom 
    FROM df_epci)
    
    SELECT
        epci.dept_epci AS dept_id,
        epci.siren AS id_epci,
        epci.epci_nom AS epci_lib,
        'i114' AS id_indicator,
        ROUND((1.0 * aap.avg_annual_phyto / ds.sau_ha), 3) AS valeur_brute,
        '2023' AS annee
    FROM epci
    LEFT JOIN avg_annual_phyto AS aap
        ON epci.siren = aap.siren
    LEFT JOIN df_sau AS ds
        ON epci.siren = ds.geocode_epci
    ORDER BY epci.dept_epci, epci.siren
    """

    df_final = duckdb.sql(query_bdd)

    # Sauvegarde du fichier final
    output_path = processed_dir / "i114_kg_phyto_per_ha_sau.csv"
    df_final.write_csv(str(output_path))
    print(f"Fichier sauvegardé : {output_path}")


if __name__ == "__main__":
    main()
