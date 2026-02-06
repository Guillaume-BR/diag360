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
data_dir = base_dir / "data" / "data_medi_num"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/398edc71-0d51-4cb6-9cbe-2540a4db573c"
    )

    # Télécharger et extraire les données médiation
    content = download_file(url)
    df_mediation_num = pd.read_csv(BytesIO(content), low_memory=False)
    
    # Création de df_epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=',')


    # Regroupement par code_insee communes
    query = """ 
    SELECT 
        count(id) AS nb_mediation,
        code_insee
    FROM df_mediation_num
    GROUP BY code_insee
    """
    df_mediation_num_grouped = duckdb.sql(query)

    # Jointure des données
    query = """ 
    SELECT
        df_epci.dept_epci AS dept_id,
        df_epci.siren AS id_epci,
        df_epci.epci_nom AS epci_lib,
        'i095' AS id_indicator,
        ROUND(SUM(df_mediation_num_grouped.nb_mediation) / df_epci.total_pop_mun * 10000, 2) AS valeur_brute,
        '2024' AS annee
    FROM df_epci
    LEFT JOIN df_mediation_num_grouped
    ON df_epci.code_insee = df_mediation_num_grouped.code_insee
    GROUP BY df_epci.siren, dept_id, epci_lib, df_epci.total_pop_mun
    ORDER BY dept_id, id_epci
    """
    df_epci_mediation = duckdb.sql(query)
    print(df_epci_mediation.df().head())

    # Sauvegarde du fichier final
    output_file = processed_dir / "i095_mediation_numerique_per_10k_habs.csv"
    df_epci_mediation.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()  # mediation_numerique.py
