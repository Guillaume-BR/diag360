import sys
import pandas as pd
import duckdb
import os
from pathlib import Path
from io import BytesIO

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import download_file, create_dataframe_communes, create_dataframe_epci

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_densite_pharma"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # chargement des données des pharmacies
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
    )
    content = download_file(url)
    df_pharma = pd.read_csv(BytesIO(content), sep=";", dtype=str, skiprows=1, header=None)

    # Chargement de la table epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=',')

    # Chargement de la table des communes
    df_com = create_dataframe_communes()

    # Traitement des données de pharmacies
    df_pharma = df_pharma.iloc[:, [12,13,15, 19]]
    df_pharma.rename(columns={19: "type", 15: "code_postal",12:"code_com", 13: "dept"}, inplace=True)
    df_pharma['code_insee'] = df_pharma['dept'] + df_pharma['code_com']
    df_pharma = df_pharma.loc[df_pharma["type"].str.startswith("Pharmacie")].reset_index(
        drop=True
    )

    # Correction des codes insee de Paris, Lyon, Marseille pour les faire correspondre à ceux de l'INSEE
    df_pharma.loc[df_pharma["code_insee"].str.startswith("75"), "code_insee"] = "75056"
    df_pharma.loc[df_pharma["code_insee"].str.startswith("693"), "code_insee"] = "69123"
    df_pharma.loc[df_pharma["code_insee"].str.startswith("132"), "code_insee"] = "13055"


    query_final = """
    SELECT
        df_epci.dept_epci AS dept_id,
        df_epci.siren AS id_epci,
        df_epci.epci_nom AS epci_lib,
        'i066' AS id_indicator,
        round(COUNT(df_pharma.code_insee) / cast(df_epci.total_pop_mun AS FLOAT) * 10000,2) AS valeur_brute,
        '2026' AS annee
    FROM df_epci
    LEFT JOIN df_pharma AS df_pharma
        ON df_pharma.code_insee = df_epci.code_insee
    GROUP BY df_epci.siren, df_epci.dept_epci, df_epci.epci_nom, df_epci.total_pop_mun
    ORDER BY df_epci.dept_epci, df_epci.siren
        """

    df_densite_pharma_final = duckdb.sql(query_final)
    print(f"df_densite_pharma_final.shape: {df_densite_pharma_final.df().shape}")

    #Sauvegarde du fichier final
    df_densite_pharma_final.write_csv(str(processed_dir / "i066_densite_pharma.csv"))
    print(f"Fichier sauvegardé : {processed_dir / 'i066_densite_pharma.csv'}")


if __name__ == "__main__":
    main()
