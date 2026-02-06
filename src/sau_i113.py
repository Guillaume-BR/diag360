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
data_dir = base_dir / "data" / "data_sau"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():

    #Création de df_epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=',')

    # Téléchagement de la table de la sau
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/022cb00f-38f2-4fe7-8895-e3467d3d9255"
    )
    content = download_file(url)
    df_sau = pd.read_csv(BytesIO(content), sep=",")

    # Traitement de la table sau
    df_sau = df_sau[df_sau["date_mesure"].str.startswith("2020")]

    # query complete
    query = """
        SELECT
            s1.dept_epci AS dept_id,
            CAST(s1.siren AS VARCHAR) AS id_epci,
            s1.epci_nom AS epci_lib,
            'i113' AS id_indicator,
            ROUND(s2.valeur/ SUM(s1.superficie_hectare)*100, 2) AS valeur_brute,
            '2025' AS annee
        FROM df_epci AS s1
        LEFT JOIN df_sau AS s2
            ON CAST(s1.siren AS VARCHAR) = CAST(s2.geocode_epci AS VARCHAR)
        GROUP BY s1.dept_epci, s1.siren, s1.epci_nom, s2.valeur
        ORDER BY s1.dept_epci, s1.siren
        """
    df_complete = duckdb.sql(query)
    output_path_complete = processed_dir / "i113_part_sau.csv"
    df_complete.write_csv(str(output_path_complete))
    print(f"Données complètes sauvegardées dans {output_path_complete}")


if __name__ == "__main__":
    main()
