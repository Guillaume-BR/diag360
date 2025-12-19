import sys
import pandas as pd
import duckdb
import os
from pathlib import Path


# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_asso"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def create_full(path_folder):
    """
    Lit tous les fichiers CSV d'un dossier, filtre certaines colonnes,
    concatène les résultats et supprime chaque fichier après lecture.

    Parameters
    ----------
    path_folder : str
        Chemin vers le dossier contenant les fichiers CSV.

    Returns
    -------
    pd.DataFrame
        DataFrame complet avec les colonnes 'adrs_codeinsee' et 'adrs_codepostal'
        pour les lignes où 'position' == 'A'.
    """
    df_full = pd.DataFrame()

    for file_name in os.listdir(path_folder):
        if file_name.endswith(".csv") and file_name.startswith("rna_waldec"):
            file_path = os.path.join(path_folder, file_name)

            # Lire le CSV
            df_temp = pd.read_csv(file_path, sep=";")
            print(f"Fichier lu : {file_path} avec {len(df_temp)} lignes.")
            df_temp = df_temp.loc[
                df_temp["position"] == "A"
            ]  # filtre les association en activité
            df_temp = df_temp[["adrs_codeinsee", "adrs_codepostal"]]

            # Concaténer dans le DataFrame complet
            df_full = pd.concat([df_full, df_temp], ignore_index=True, axis=0)

            # Supprimer le fichier après lecture
            os.remove(file_path)

    print(f"Dataframe complet créé.")
    return df_full


def main():
    # Define URLs and file paths
    zip_url = "https://www.data.gouv.fr/api/1/datasets/r/c2334d19-c752-413f-b64b-38006d9d0513"  # Replace with actual URL
    filename_asso = "data_asso.zip"

    # Download and extract the zip file
    download_file(zip_url, extract_to=raw_dir, filename=filename_asso)
    extract_zip(os.path.join(raw_dir, filename_asso), extract_to=raw_dir)
    
    # Create full dataframe from extracted CSV files
    df = create_full(path_folder=raw_dir)

    # Homogenize NaN values
    df_asso_cleaned = homogene_nan(df).reset_index(drop=True)

    # Correction des nan
    df_nan = df_asso_cleaned.loc[
        df_asso_cleaned[["adrs_codeinsee", "adrs_codepostal"]].isna().any(axis=1)
    ]
    df_sans_nan = df_asso_cleaned.dropna().reset_index(drop=True)
    df_nan_postal = df_nan.loc[df_nan["adrs_codepostal"].isna()]

    # Création de la table duckdb pour les jointures
    df_com = create_dataframe_communes(raw_dir)

    # Récupération des codes postaux manquants via jointure avec df_com
    query = """ 
        SELECT DISTINCT e1.adrs_codeinsee, e2.code_insee, e2.code_postal
        FROM df_nan_postal e1
        LEFT JOIN df_com e2
        ON (e1.adrs_codeinsee = e2.code_insee)
        ORDER BY e1.adrs_codeinsee
        """
    df_sans_nan_postal = duckdb.sql(query).df().dropna()
    df_sans_nan_postal = df_sans_nan_postal[["adrs_codeinsee", "code_postal"]]
    df_sans_nan_postal.rename(columns={"code_postal": "adrs_codepostal"}, inplace=True)

    # Combinaison des deux dataframes pour obtenir le dataframe complet
    df_asso_complete = (
        pd.concat(
            [df_sans_nan[["adrs_codeinsee", "adrs_codepostal"]], df_sans_nan_postal],
            ignore_index=True,
            axis=0,
        )
        .sort_values(["adrs_codeinsee", "adrs_codepostal"])
        .dropna()
        .reset_index(drop=True)
    )

    # Création de la table duckdb pour les jointures avec les epci
    df_epci = create_dataframe_epci()

    query = """
        SELECT 
            e2.dept,
            e2.siren, 
            REPLACE(e2.total_pop_mun, ' ', '') AS population,
            count(*) AS nb_asso
        FROM df_asso_complete e1
        LEFT JOIN df_epci e2
        ON e1.adrs_codeinsee = e2.insee
        GROUP BY e2.dept,e2.siren,e2.total_pop_mun
        ORDER BY dept, siren
        """

    df_asso_epci = duckdb.sql(query).df().dropna()
    query = """ 
        SELECT *, round(1.0*TRY_CAST(nb_asso AS DOUBLE) / TRY_CAST(population AS DOUBLE) * 1000,2) as asso_per_1000_habitants
        FROM df_asso_epci
        ORDER BY dept,siren
        """

    df_asso_summary = duckdb(query)

    # Sauvegarde du fichier final
    output_file = processed_dir / "asso_per_epci.csv"
    df_asso_summary.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()  # asso.py
