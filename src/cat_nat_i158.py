import sys
import pandas as pd
import duckdb
import os
from pathlib import Path
from io import BytesIO
import zipfile

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import download_file, create_dataframe_communes, create_dataframe_epci

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_cat_nat"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # Define URLs and file paths
    URL = (
        "https://www.data.gouv.fr/api/1/datasets/r/d6fb9e18-b66b-499c-8284-46a3595579cc"
    )
    zip_content = download_file(URL)
    with zipfile.ZipFile(BytesIO(zip_content)) as z:
        with z.open("catnat_gaspar.csv") as f:
            df_cat_nat = pd.read_csv(f, sep=";", low_memory=False)    

    # Création de la table duckdb des epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=',')

    #nombre de cat nat par commune sur 40 ans
    query = """
    SELECT cod_commune AS code_insee, count(*) AS nb_cat_nat
    FROM df_cat_nat
    GROUP BY cod_commune
    """
    df_cat_nat_communes = duckdb.sql(query)
    print(f"df_cat_nat_communes.shape: {df_cat_nat_communes.df().shape}")

    # Surface de chaque epci et nb de cat nat par epci sur 40 ans
    query = """
    WITH df_temp AS (
    SELECT 
        df_epci.siren AS siren,
        df_cat_nat_communes.nb_cat_nat,
        df_epci.superficie_km2
    FROM df_epci
    LEFT JOIN df_cat_nat_communes
    ON df_epci.code_insee = df_cat_nat_communes.code_insee
    )

    SELECT 
        siren,
        SUM(nb_cat_nat) AS nb_cat_nat_total,
        SUM(superficie_km2) AS superficie_epci_km2,
        ROUND(nb_cat_nat_total / superficie_epci_km2, 3) AS cat_nat_per_km2
    FROM df_temp
    GROUP BY siren
    """

    df_cat_nat_temp = duckdb.sql(query)
    print(f"df_cat_nat_temp.shape: {df_cat_nat_temp.df().shape}")

    # Ajout du nom des epci
    query_complete = """
    SELECT 
        df_epci.dept_epci as dept_id,
        CAST(df_epci.siren AS VARCHAR) as id_epci,
        df_epci.epci_nom as epci_lib,
        'i158' AS id_indicator,
        df_cat_nat_temp.cat_nat_per_km2 as valeur_brute,
        '2025' AS annee
    FROM df_epci
    LEFT JOIN df_cat_nat_temp
    ON df_cat_nat_temp.siren = df_epci.siren
    GROUP BY df_epci.dept_epci, df_epci.siren, df_epci.epci_nom, df_cat_nat_temp.cat_nat_per_km2
    ORDER BY df_epci.dept_epci, df_epci.siren
    """

    df_cat_nat_final = duckdb.sql(query_complete)
    print(f"df_cat_nat_final.shape: {df_cat_nat_final.df().shape}")

    # Sauvegarde du fichier final
    output_file = processed_dir / "i_158_cat_nat_per_epci.csv"
    df_cat_nat_final.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()  # asso.py
