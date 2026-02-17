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
        "https://www.data.gouv.fr/api/1/datasets/r/b27d31a6-107b-46ee-8427-518799b488f0"
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

    #traitement de df_sau : on ajoute des zéros et on corrige les codes insee de Paris, Lyon, Marseille pour les faire correspondre à ceux de l'INSEE
    df_sau['geocode_commune'] = df_sau['geocode_commune'].apply(lambda x: str(x).zfill(5))
    df_sau['geocode_commune'] = df_sau['geocode_commune'].apply(lambda x: '75056' if x.startswith('75') and isinstance(x, str) else x)
    df_sau['geocode_commune'] = df_sau['geocode_commune'].apply(lambda x: '13055' if x.startswith('132') and isinstance(x, str ) else x)
    df_sau['geocode_commune'] = df_sau['geocode_commune'].apply(lambda x: '69123' if x.startswith('693') and isinstance(x, str) else x)
                                                                                                                          
    #Correction de Paris, Lyon, Marseille dans df_phyto
    df_phyto = duckdb.sql("""
    SELECT
        *,
        CASE
            WHEN code_insee LIKE '75%'  THEN '75056'
            WHEN code_insee LIKE '132%' THEN '13055'
            WHEN code_insee LIKE '693%' THEN '69123'
            ELSE code_insee
        END AS code_insee
    FROM df_phyto
""")

    #Préparation de df_sau : on ne garde que 2020
    query_sau = """ 
    SELECT 
        df_epci.siren,
        ROUND(SUM(TRY_CAST(valeur AS DOUBLE)), 2) AS sau_ha
    FROM df_sau
    LEFT JOIN df_epci
    ON df_sau.geocode_commune = df_epci.code_insee
    WHERE date_mesure LIKE '2020%'
    GROUP BY df_epci.siren
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
        ON epci.siren = ds.siren
    ORDER BY epci.dept_epci, epci.siren
    """

    df_final = duckdb.sql(query_bdd)

    # Sauvegarde du fichier final
    output_path = processed_dir / "i114_kg_phyto_per_ha_sau.csv"
    df_final.write_csv(str(output_path))
    print(f"Fichier sauvegardé : {output_path}")


if __name__ == "__main__":
    main()
