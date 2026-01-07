import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_zone_urb"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def main():
    # Chargement local des données mais sinon utiliser :
    # https://docs.google.com/spreadsheets/d/1y6yy7_XCmhSUIqBmzZ200mgMo93YuZS8/edit?usp=sharing&ouid=108793438427721456504&rtpof=true&sd=true
    path_zones_urb = base_dir / "data" / "raw" / "zones_urbaines.csv"
    df_zones_urb = pd.read_csv(path_zones_urb)

    # Chargement des données des EPCI
    df_epci = create_dataframe_epci(raw_dir)

    # Chargement des données des aménagements cyclables
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/f5d6ae97-b62e-46a7-ad5e-736c8084cee8"
    )
    download_file(url, extract_to=raw_dir, filename="amenagement_cyclable.parquet")
    df_amenagement_cyclable = duckdb.read_parquet(
        str(raw_dir / "amenagement_cyclable.parquet")
    )

    # Chargement des données des communes
    df_com = create_dataframe_communes(raw_dir)

    # Traitement des données des zones urbaines
    df_zones_urb.drop("Unnamed: 6", axis=1, inplace=True)
    df_zones_urb.drop(
        "* Les donnes proviennent de Corine Land Cover millésime 2018",
        axis=1,
        inplace=True,
    )
    mapping = {
        "SIREN": "siren",
        "Nom de l'EPCI": "nom_epci",
        "Nature de l'EPCI": "nature_epci",
        "Superficie de l'EPCI (km²)": "superficie_epci",
        "Superficie des territoires artificialisés* (km²)": "superficie_artificialisee",
        "Part de la superficie artificialisée": "part_percent_superficie_artificialisee",
    }

    df_zones_urb.rename(columns=mapping, inplace=True)
    df_zones_urb["superficie_epci"] = (
        df_zones_urb["superficie_epci"].replace(",", ".", regex=True).astype(float)
    )
    df_zones_urb["superficie_artificialisee"] = (
        df_zones_urb["superficie_artificialisee"]
        .replace(",", ".", regex=True)
        .astype(float)
    )
    df_zones_urb["part_percent_superficie_artificialisee"] = (
        df_zones_urb["part_percent_superficie_artificialisee"]
        .replace(",", ".", regex=True)
        .replace(" %", "", regex=True)
        .astype(float)
    )
    print(f"df_zones_urb.shape: {df_zones_urb.shape}")
    print(df_zones_urb.head())

    # Regroupement par EPCI dans df_epci
    query = """ 
    SELECT DISTINCT siren, raison_sociale AS nom_epci, dept
    FROM df_epci
    """
    df_epci = duckdb.sql(query)

    # calcul de la superficie des EPCI à partir des communes
    query = """
    SELECT 
        epci_code AS siren,
        SUM(superficie_km2) AS superficie_km2
    FROM df_com
    WHERE (superficie_km2 IS NOT NULL) AND (epci_code != 'ZZZZZZZZZ')
    GROUP BY epci_code
    """

    df_surface_epci = duckdb.sql(query)
    print(f"df_surface_epci.shape: {df_surface_epci.df().shape}")

    # Jointure des données des zones urbanisées avec les EPCI
    query = """
    SELECT
        epci.siren,
        epci.nom_epci,
        epci.dept,
        sepci.superficie_km2,
        zu.superficie_artificialisee,
        zu.part_percent_superficie_artificialisee
    FROM df_epci epci
    LEFT JOIN df_zones_urb zu
    ON zu.siren = epci.siren
    LEFT JOIN df_surface_epci sepci
    ON sepci.siren = epci.siren
    """

    df_zone_urbanise_merged = duckdb.sql(query)
    print(f"taille df_zone_urbanise_merged: {df_zone_urbanise_merged.df().shape}")

    # Amenagement cyclable par communes
    query = """
    WITH df_temp AS(
    SELECT 
        code_com_d AS code_insee,
        count(id_osm) AS nb_amenagements
    FROM df_amenagement_cyclable
    GROUP BY code_com_d)
    
    SELECT  
        df_com.code_insee,
        df_temp.nb_amenagements,
        df_com.epci_code
    FROM df_com
    LEFT JOIN df_temp
    ON df_com.code_insee = df_temp.code_insee 
    """

    df_amenagements_par_communes = duckdb.sql(query)
    print(
        f"df_amenagements_par_communes.shape: {df_amenagements_par_communes.df().shape}"
    )

    # aménagements cyclables par EPCI
    query = """ 
    SELECT
        epci_code,
        SUM(nb_amenagements) AS total_amenagements
    FROM df_amenagements_par_communes
    WHERE epci_code::varchar not like '%Z'
    GROUP BY epci_code
    """

    df_amenagements_par_epci = duckdb.sql(query)

    # On merge les aménagements cyclables avec les zones urbanisées
    query = """ 
    SELECT 
        zu.*,
        ape.total_amenagements,
        ROUND(ape.total_amenagements / zu.superficie_artificialisee,2) AS amenagements_per_km2
    FROM df_amenagements_par_epci ape
    LEFT JOIN df_zone_urbanise_merged zu
    ON ape.epci_code = zu.siren
    ORDER BY zu.dept, zu.siren
    """

    df_zone_urbanise_final = duckdb.sql(query)
    print(f"df_zone_urbanise_final.shape: {df_zone_urbanise_final.df().shape}")

    # Sauvegarde du fichier final
    output_file = processed_dir / "zone_urbanise_per_epci.csv"
    df_zone_urbanise_final.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()
