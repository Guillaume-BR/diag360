import sys
import pandas as pd
import duckdb
import os
from pathlib import Path
import requests

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_pe"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # Define URLs and file paths
    offset = [100 * i for i in range(0, 13)]
    for offset in offset:
        url = f"https://data.caf.fr/api/explore/v2.1/catalog/datasets/txcouv_pe_epci/records?limit=100&offset={offset}&refine=annee%3A%222023%22"
        response = requests.get(url)
        r = response.json()
        df_pe = pd.json_normalize(r["results"])
        if offset == 0:
            df_pe_total = df_pe.copy()
        else:
            df_pe_total = pd.concat([df_pe_total, df_pe], ignore_index=True)

    df_pe_final = df_pe_total[["numepci", "txcouv_epci"]].copy()
    df_pe_final = df_pe_final.rename(
        columns={"numepci": "id_epci", "txcouv_epci": "valeur_brute"}
    )

    query = """
    SELECT
        id_epci,
        'i130' AS id_indicator,
        valeur_brute,
        '2023' AS annee
    FROM df_pe_final
    """
    df_pe_final = duckdb.sql(query)

    # Sauvegarde des données traitées
    output_path = processed_dir / "txcouv_pe_epci_2023.csv"
    df_pe_final.write_csv(str(output_path))
    print(f"Données petite enfance sauvegardées dans {output_path}")

    df_epci = create_dataframe_epci(raw_dir)

    #query complete
    query = """ 
        SELECT 
            DISTINCT siren AS id_epci,
            raison_sociale AS nom_epci,
            dept
        FROM df_epci
        """
    df_epci = duckdb.sql(query)

    #query complete with join
    query = """
        SELECT 
            s1.dept,
            CAST(s1.id_epci AS VARCHAR) AS id_epci,
            s1.nom_epci,
            'i130' AS id_indicator,
            s2.valeur_brute
        FROM df_epci AS s1
        LEFT JOIN df_pe_final AS s2
            ON CAST(s1.id_epci AS VARCHAR) = CAST(s2.id_epci AS VARCHAR)
        ORDER BY s1.dept, s1.id_epci
        """
    
    df_complete = duckdb.sql(query)

    
    output_path_complete = processed_dir / "i130_txcouv_pe.csv"
    df_complete.write_csv(str(output_path_complete))
    print(f"Données petite enfance complètes sauvegardées dans {output_path_complete}")


if __name__ == "__main__":
    main()
