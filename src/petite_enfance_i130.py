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
    df_pe_final = df_pe_final.rename(columns={"numepci": "id_epci", "txcouv_epci": "valeur_brute"})

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


if __name__ == "__main__":
    main()
