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
    df_pharma = df_pharma.iloc[:, [15, 19]]
    df_pharma.rename(columns={19: "type", 15: "code_insee"}, inplace=True)
    df_pharma = df_pharma.loc[df_pharma["type"].str.startswith("Phar")].reset_index(
        drop=True
    )

    df_pharma["code_postal"] = df_pharma["code_insee"].apply(lambda x: x.split(" ")[0])
    df_pharma.drop(columns=["code_insee"], inplace=True)

    df_pharma['code_postal'] = df_pharma['code_postal'].apply(lambda x: '75000' if x.startswith('75') else x)

    # Jointure avec les données des communes pour récupérer le nombre de pharma par epci
    query = """
    SELECT
        df_epci.siren,
        COUNT(df_pharma.code_postal) AS nb_pharma,
    FROM df_com
    LEFT JOIN df_pharma
        ON df_pharma.code_postal = df_com.code_postal
    LEFT JOIN df_epci
        ON df_com.code_insee = df_epci.code_insee
    GROUP BY df_epci.siren
    """

    df_nb_pharma = duckdb.sql(query)
    print(f"df_nb_pharma.shape: {df_nb_pharma.df().shape}")

    query_final = """
    SELECT
        df_epci.dept_epci AS dept_id,
        CAST(df_epci.siren AS VARCHAR) as id_epci,
        df_epci.epci_nom as epci_lib,
        'i066' AS id_indicateur,
        ROUND((df_nb_pharma.nb_pharma / df_epci.total_pop_mun) * 10000, 2) AS valeur_brute,
        '2025' AS annee
    FROM df_epci
    LEFT JOIN df_nb_pharma
    ON df_nb_pharma.siren = df_epci.siren
    GROUP BY dept_id, df_epci.siren, epci_lib, valeur_brute
    ORDER BY df_epci.dept_epci,df_epci.siren
    """

    df_densite_pharma_final = duckdb.sql(query_final)
    print(f"df_densite_pharma_final.shape: {df_densite_pharma_final.df().shape}")

    #Sauvegarde du fichier final
    df_densite_pharma_final.write_csv(str(processed_dir / "i066_densite_pharma.csv"))
    print(f"Fichier sauvegardé : {processed_dir / 'i066_densite_pharma.csv'}")


if __name__ == "__main__":
    main()
