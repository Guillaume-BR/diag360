import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

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
    download_file(url, extract_to=raw_dir, filename="pharmacies.csv")
    df_pharma = pd.read_csv(
        raw_dir / "pharmacies.csv", sep=";", dtype=str, skiprows=1, header=None
    )

    # Chargement de la table epci
    df_epci = create_dataframe_epci(raw_dir)

    # Chargement de la table des communes
    df_com = create_dataframe_communes(raw_dir)

    # Traitement des données de pharmacies
    df_pharma = df_pharma.iloc[:, [15, 19]]
    df_pharma.rename(columns={19: "type", 15: "code_insee"}, inplace=True)
    df_pharma = df_pharma.loc[df_pharma["type"].str.startswith("Phar")].reset_index(
        drop=True
    )

    df_pharma["code_postal"] = df_pharma["code_insee"].apply(lambda x: x.split(" ")[0])
    df_pharma.drop(columns=["code_insee"], inplace=True)

    # Jointure avec les données des communes pour récupérer le nombre de pharma par commune
    query = """
    SELECT
        df_com.epci_code AS id_epci,
        'i066' AS id_indicator,
        COUNT(df_pharma.code_postal) AS valeur_brute,
        '2025' AS annee
    FROM df_pharma
    LEFT JOIN df_com
        ON df_pharma.code_postal = df_com.code_postal
    GROUP BY id_epci
    HAVING id_epci != 'ZZZZZZZZZ'
    """

    result = duckdb.sql(query)

    # On garde la population totale des epci
    query = """ 
    SELECT 
        DISTINCT siren, 
        dept,
        raison_sociale AS nom_epci,
        TRY_CAST(REPLACE(total_pop_tot,' ','') AS INTEGER) as total_pop 
        FROM df_epci
    """
    df_epci_pop_tot = duckdb.sql(query)

    query_final = """
    SELECT
        df_epci_pop_tot.dept,
        df_epci_pop_tot.siren as id_epci,
        df_epci_pop_tot.nom_epci,
        'i066' AS id_indicator,
        ROUND((result.valeur_brute/ df_epci_pop_tot.total_pop) * 10000, 2) AS valeur_brute
    FROM df_epci_pop_tot
    LEFT JOIN result 
    ON result.id_epci = df_epci_pop_tot.siren
    WHERE result.id_epci IS NOT NULL
    ORDER BY df_epci_pop_tot.dept,df_epci_pop_tot.siren

    """

    df_densite_pharma_final = duckdb.sql(query_final)
    print(f"df_densite_pharma_final.shape: {df_densite_pharma_final.df().shape}")

    #Sauvegarde du fichier final
    df_densite_pharma_final.write_csv(str(processed_dir / "i066_densite_pharma.csv"))
    print(f"Fichier sauvegardé : {processed_dir / 'i066_densite_pharma.csv'}")

    # Calcul du nombre de pharmacie pour 10000 habitants
    query_bdd = """
    SELECT 
        result.id_epci,
        result.id_indicator,
        ROUND((result.valeur_brute/ df_epci_pop_tot.total_pop) * 10000, 2) AS valeur_brute,
        result.annee
    FROM df_epci_pop_tot
    LEFT JOIN result 
    ON result.id_epci = df_epci_pop_tot.siren
    WHERE result.id_epci IS NOT NULL
    """

    df_densite_pharma = duckdb.sql(query_bdd)

    # Sauvegarde du résultat final
    df_densite_pharma.write_csv(str(processed_dir / "densite_pharma_i066.csv"))
    print("Traitement terminé. Fichier sauvegardé dans le dossier 'processed'.")


if __name__ == "__main__":
    main()
