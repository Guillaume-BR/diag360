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
        "https://www.data.gouv.fr/api/1/datasets/r/b27d31a6-107b-46ee-8427-518799b488f0"
    )
    content = download_file(url)
    df_sau = pd.read_csv(BytesIO(content), sep=",")

    #Chargement de la table avec les surface des communes
    #URL = (
    #    "https://www.insee.fr/fr/statistiques/fichier/4505239/ODD_PARQUET.zip"
    #)
    #zip_content = download_file(URL)
    #with zipfile.ZipFile(BytesIO(zip_content)) as z:
    #    with z.open("catnat_gaspar.csv") as f:
    #        df_cat_nat = pd.read_csv(f, sep=";", low_memory=False)
    
    df_communes = duckdb.read_parquet("./data/data_sau/raw/ODD_COM.parquet")

    # Traitement de la table sau
    df_sau = df_sau[df_sau["date_mesure"].str.startswith("2020")].copy()
    df_sau['geocode_commune'] = df_sau['geocode_commune'].astype(str).str.zfill(5)

    #Traitement de la table des communes pour ne garder que les codes insee et les surfaces
    query = """ 
    SELECT 
        codgeo,
        libgeo,
        A2021 AS surface
    FROM df_communes
    WHERE variable = 'surface'
    """

    df_surf_com = duckdb.sql(query)

    query = """
    SELECT
        dept_epci as dept_id,
        siren as id_epci,
        epci_nom AS lib_epci,
        'i113' AS id_indicator,
        ROUND(sum(df_sau.valeur/100) / sum(surface)  * 100,3) AS valeur_brute,
        '2020' AS annee
    FROM df_epci
    LEFT JOIN df_surf_com 
        ON df_epci.code_insee = df_surf_com.codgeo
    LEFT JOIN df_sau
        ON df_epci.code_insee = df_sau.geocode_commune
    GROUP BY siren, dept_epci, epci_nom
    ORDER BY dept_epci, siren
    """

    df_sau_final = duckdb.sql(query)
    output_path_complete = processed_dir / "i113_part_sau.csv"
    df_sau_final.write_csv(str(output_path_complete))
    print(f"Données complètes sauvegardées dans {output_path_complete}")


if __name__ == "__main__":
    main()
