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

    filename_nb_trajets_covoit = "nb-trajets-covoiturage_2024_export.csv"

    # Lecture des données relative au covoiturage
    df_nb_covoit = duckdb.read_csv(raw_dir / filename_nb_trajets_covoit, sep=",")

    # Téléchargement des données epci pour jointure
    df_epci = create_dataframe_epci(raw_dir)

    # On sélectionne uniquement les colonnes utiles
    df_epci_filtered = duckdb.sql(
        """ 
    SELECT 
        DISTINCT siren,
        raison_sociale AS nom_epci,
        dept,
        TRY_CAST(REPLACE(total_pop_tot,' ','') AS DOUBLE) AS total_pop_tot
    FROM df_epci
    """
    )

    # Calcul par epci du nombre de trajets de covoiturage pour 10 000 habitants
    query = """ 
    SELECT
        e1.siren as id_epci,
        'i150' AS id_indicator,
        ROUND((1.0*e2.valeur)/e1.total_pop_tot*10000 ,3) AS valeur_brute,
        '2024' AS annee
    FROM df_epci_filtered e1
    LEFT JOIN df_nb_covoit e2
    ON e2.territoryid = e1.siren
    """

    df_nb_trajets_relative = duckdb.sql(query)

    # Sauvegarde du fichier final
    output_file = processed_dir / "nb_covoit_per_epci.csv"
    df_nb_trajets_relative.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")

    # query complete
    query = """ 
    SELECT
        e1.dept,
        e1.siren as id_epci,
        e1.nom_epci,
        'i150' AS id_indicator,
        ROUND((1.0*e2.valeur)/e1.total_pop_tot*10000 ,3) AS valeur_brute
    FROM df_epci_filtered e1
    LEFT JOIN df_nb_covoit e2
    ON e2.territoryid = e1.siren
    ORDER BY e1.dept, e1.siren
    """

    df_nb_trajet_complete = duckdb.sql(query)
    output_file_complete = processed_dir / "i150_nb_covoit.csv"
    df_nb_trajet_complete.write_csv(str(output_file_complete))
    print(f"Fichier sauvegardé : {output_file_complete}")


if __name__ == "__main__":
    main()
