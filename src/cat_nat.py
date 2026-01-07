import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

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
    download_file(URL, extract_to=raw_dir, filename="gaspar.zip")
    extract_zip(raw_dir / "gaspar.zip", extract_to=raw_dir)
    for file in os.listdir(raw_dir):
        if not file.startswith("catnat"):
            os.remove(raw_dir / file)
    path_cat_nat = raw_dir / "catnat_gaspar.csv"
    df_cat_nat = pd.read_csv(path_cat_nat, sep=";", low_memory=False)

    # Création de la table duckdb pour les jointures
    df_com = create_dataframe_communes(raw_dir)

    #Création de la table duckdb des epci
    df_epci = create_dataframe_epci(raw_dir)

    # Mise en forme des données
    # mapping = {
    # "Code": "siren",
    # "Libellé": "nom_epci",
    # "Nombre d'Arrêtés de Catastrophes Naturelles publiés au J.O.": "nb_cat_nat"
    # }
    # df_cat_nat = df_cat_nat.rename(columns=mapping)
    # df_cat_nat.loc[df_cat_nat['siren'] == 75056, 'siren'] = 200054781
    # print("Données cat_nat chargées et renommées.")

    #création de la table du code epci et du nom associé
    query = """
    SELECT DISTINCT
        siren,
        raison_sociale AS nom_epci,
        dept
    FROM df_epci
    """
    df_epci = duckdb.sql(query)
    print(f"df_epci.shape: {df_epci.df().shape}")

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
        df_com.epci_code AS siren,
        df_cat_nat_communes.nb_cat_nat,
        df_com.superficie_km2
    FROM df_com
    LEFT JOIN df_cat_nat_communes
    ON df_com.code_insee = df_cat_nat_communes.code_insee
    WHERE (superficie_km2 IS NOT NULL) AND (epci_code != 'ZZZZZZZZZ')
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
    query = """
    SELECT 
        df_epci.siren,
        df_epci.nom_epci,
        df_epci.dept,
        df_cat_nat_temp.nb_cat_nat_total,
        df_cat_nat_temp.superficie_epci_km2,
        df_cat_nat_temp.cat_nat_per_km2
    FROM df_cat_nat_temp
    LEFT JOIN df_epci
    ON df_cat_nat_temp.siren = df_epci.siren
    ORDER BY df_epci.dept, df_epci.siren
    """

    df_cat_nat_final = duckdb.sql(query)
    print(f"df_cat_nat_final.shape: {df_cat_nat_final.df().shape}")

    # Sauvegarde du fichier final
    output_file = processed_dir / "cat_nat_per_epci.csv"
    df_cat_nat_final.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()  # asso.py
