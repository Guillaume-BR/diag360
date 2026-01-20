import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_covoit"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # définition des urls
    # url_nb_lieu_covoit = "https://www.data.gouv.fr/api/1/datasets/r/4fd78dee-e122-4c0d-8bf6-ff55d79f3af1"
    # téléchargement des données
    # download_file(url_nb_lieu_covoit, raw_dir, filename="nb_lieux_covoiturage.csv")
    # mais manque à priori des données

    filename_nb_lieu_covoit = "nb-lieux-covoiturage_2025_export.csv"

    # Lecture des données relative au covoiturage
    df_nb_lieu_covoit = duckdb.read_csv(raw_dir / filename_nb_lieu_covoit)

    # Téléchargement des données epci pour jointure
    df_epci = create_dataframe_epci(raw_dir)

    # On sélectionne uniquement les colonnes utiles
    query = """ 
    SELECT 
        DISTINCT siren,
        raison_sociale AS nom_epci,
        dept,
        TRY_CAST(REPLACE(total_pop_tot,' ','') AS DOUBLE) AS total_pop_tot
    FROM df_epci
    """
    df_epci_filtered = duckdb.sql(query)

    # Calcul par epci du nombre de lieux de covoiturage pour 10000 habitants
    query = """
    WITH df_nb_lieu_covoit_filtered AS (
        SELECT 
            territoryid AS siren,
            sum(valeur) AS nb_aires_covoiturage
        FROM df_nb_lieu_covoit
        WHERE type_lieu = 'Aire de covoiturage'
        GROUP BY territoryid
        )
    
    SELECT 
        e1.siren AS id_epci,
        'i149' AS id_indicator,
        ROUND(e2.nb_aires_covoiturage / e1.total_pop_tot * 10000,3) AS valeur_brute,
        '2025' AS annee
    FROM df_epci_filtered e1
    LEFT JOIN df_nb_lieu_covoit_filtered e2
    ON e1.siren = e2.siren
    """

    df_nb_lieu_covoit_relative = duckdb.sql(query)

    # Sauvegarde du fichier final
    output_file = processed_dir / "aires_covoit_per_epci.csv"
    df_nb_lieu_covoit_relative.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()
