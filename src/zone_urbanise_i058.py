import sys
import os
import pandas as pd
import geopandas as gpd
import duckdb
from pathlib import Path
from shapely import wkb


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

    print(df_zones_urb.head())

    # Chargement des données des EPCI
    df_epci = create_dataframe_epci(raw_dir)

    print(df_epci.df().head())

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

    #traitement des données des aménagements cyclables
    # 1. Charger l'extension sur l'instance par défaut de DuckDB
    duckdb.sql("INSTALL spatial;")
    duckdb.sql("LOAD spatial;")

    query = """
        SELECT * EXCLUDE (geometry), 
               ST_AsWKB(geometry) AS geometry 
        FROM df_amenagement_cyclable
        WHERE ST_GeometryType(geometry) IN ('LINESTRING', 'MULTILINESTRING')
    """

    df_amenagement_cyclable = duckdb.sql(query) 
    print(f"df_amenagement_cyclable.shape: {df_amenagement_cyclable.df().shape}")
    
    df_pandas = df_amenagement_cyclable.df()
    df_pandas['geometry'] = df_pandas['geometry'].apply(lambda x: wkb.loads(bytes(x)) if x else None)

    # 2. On crée le GeoDataFrame directement à partir du DF existant
    gdf = gpd.GeoDataFrame(df_pandas, geometry='geometry', crs="EPSG:4326")

    # 3. Calcul des kilomètres
    # On projette vers le système métrique (EPSG:2154 pour la France)
    # .length donne des mètres, on divise par 1000 pour les km
    gdf['distance_km'] = gdf.to_crs(epsg=2154).geometry.length / 1000

    df_temp_pandas = pd.DataFrame(gdf.drop(columns='geometry'))

    
    # Jointure des données des zones urbanisées avec les EPCI
    query = """
    SELECT
        DISTINCT epci.siren,
        zu.superficie_artificialisee,
    FROM df_epci epci
    LEFT JOIN df_zones_urb zu
    ON zu.siren = epci.siren
    """

    df_zone_urbanise_merged = duckdb.sql(query)
    print(f"df_zone_urbanise_merged.shape: {df_zone_urbanise_merged.df().shape}")

    # Amenagement cyclable par epci
    query = """
    WITH df_temp AS(
    SELECT 
        code_com_d AS code_insee,
        sum(distance_km) AS km_amenagements
    FROM df_temp_pandas
    GROUP BY code_com_d)
    
    SELECT  
        df_com.epci_code,
        sum(df_temp.km_amenagements) AS km_amenagements
    FROM df_com
    LEFT JOIN df_temp
    ON df_com.code_insee = df_temp.code_insee 
    WHERE df_com.epci_code != 'ZZZZZZZZZ'
    GROUP BY df_com.epci_code
    """

    df_amenagements_par_epci = duckdb.sql(query)

    # On merge les aménagements cyclables avec les zones urbanisées
    query_bdd = """ 
    SELECT 
        ape.epci_code AS id_epci,
        'i058' AS id_indicator,
        ROUND(ape.km_amenagements / zu.superficie_artificialisee,2) AS valeur_brute,
        '2025' AS annee
    FROM df_amenagements_par_epci ape
    LEFT JOIN df_zone_urbanise_merged zu
    ON ape.epci_code = zu.siren
    """

    #sauvegarde du dataframe final
    df_final = duckdb.sql(query_bdd).df()
    print(df_final.head())
    path_output = processed_dir / "i058_zone_urbanise.csv"
    df_final.to_csv(path_output, index=False)

    #query_complete
    query = """ 
    WITH s1 AS (
    SELECT 
        DISTINCT siren, 
        raison_sociale, 
        dept
    FROM df_epci)

    SELECT
        s1.dept,
        s1.siren AS id_epci,
        s1.raison_sociale AS nom_epci,
        'i058' AS id_indicator,
        s2.valeur_brute
    FROM s1
    LEFT JOIN df_final AS s2
        ON CAST(s1.siren AS VARCHAR) = CAST(s2.id_epci AS VARCHAR)
    ORDER BY s1.dept, s1.siren
    """

    df_complete = duckdb.sql(query)
    output_path_complete = processed_dir / "i058_zone_urbanise_complete.csv"
    df_complete.write_csv(str(output_path_complete))
    print(f"Données zone urbanisée complètes sauvegardées dans {output_path_complete}")          
if __name__ == "__main__":
    main()
