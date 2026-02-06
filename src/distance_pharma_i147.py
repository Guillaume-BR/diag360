import sys
import pandas as pd
import duckdb
import os
from pathlib import Path
import numpy as np

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_dist_soin"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # chargement des données
    df_dist_pharma = pd.read_csv(raw_dir / "dist_pharma.csv", skiprows=2, sep=";")

    # Création de la table duckdb des epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=",")

    # Changement des noms de colonnes
    mapping_pharma = {
        "Code": "code_insee",
        "Libellé": "nom_commune",
        "Distance à la pharmacie la plus proche 2024": "dist_pharma_min",
    }

    df_dist_pharma = df_dist_pharma.rename(columns=mapping_pharma)

    # On traite les données de distance aux pharmacies
    df_dist_pharma["code_insee"] = df_dist_pharma["code_insee"].apply(
        lambda x: "75056" if x.startswith("75") else x
    )
    # on remplace les lignes où dist_pharma_min est "'N/A - résultat non disponible' par un vrai NA"
    df_dist_pharma.loc[
        df_dist_pharma["dist_pharma_min"].str.contains("N/A", na=False),
        "dist_pharma_min",
    ] = np.nan
    # on groupe par code_insee en fisant la moyenne ds distance
    df_dist_pharma["dist_pharma_min"] = (
        df_dist_pharma["dist_pharma_min"].str.replace(",", ".").astype(float)
    )
    df_dist_pharma = df_dist_pharma.groupby("code_insee", as_index=False).agg(
        {"dist_pharma_min": "mean"}
    )

    # Jointure des données distance moyenne aux pharmacies par epci
    query_final = """ 
    SELECT
        df_epci.dept_epci AS dept_id,
        df_epci.siren AS id_epci,
        df_epci.epci_nom as epci_lib,
        'i147' AS id_indicator,
        ROUND(AVG(TRY_CAST(dist_pharma_min AS DOUBLE)),2) AS valeur_brute,
        '2024' AS annee
    FROM df_epci
    LEFT JOIN df_dist_pharma
    ON df_epci.code_insee = df_dist_pharma.code_insee
    GROUP BY siren, epci_nom, dept_id
    ORDER BY dept_id, id_epci
    """
    df_dist_pharma_final = duckdb.sql(query_final)

    # Sauvegarde du fichier final
    output_file_final = processed_dir / "i_147_dist_pharma.csv"
    df_dist_pharma_final.write_csv(str(output_file_final))
    print(f"Fichier sauvegardé : {output_file_final}")


if __name__ == "__main__":
    main()
