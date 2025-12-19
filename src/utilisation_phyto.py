import sys
import pandas as pd
import duckdb
import os
from pathlib import Path


# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_phyto"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

def main():
    #Téléchargement de la table epci
    df_epci = create_dataframe_epci(raw_dir)

    #Téléchagement de la table de la sau
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/dbdd3481-107b-4eed-b66a-7f9dda1c7b78"
    )
    download_file(url, extract_to=raw_dir, filename="sau_2025.csv")
    df_sau = pd.read_csv(raw_dir / "sau_2025.csv", sep=",")
    print(df_sau.head())

    #Téléchargement de la table phyto
    url_phyto = (
        "https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c"
    )
    download_file(url_phyto, extract_to=raw_dir, filename="achat_commune_phyto.parquet")
    df_phyto = duckdb.read_parquet( str(raw_dir / "achat_commune_phyto.parquet"))

    #Jointure entre df_epci et df_phyto
    query = """
    SELECT 
        df_phyto.annee,
        df_epci.dept,
        df_epci.siren,
        TRY_CAST(df_phyto.quantite_substance AS DOUBLE) AS quantite_substance
    FROM df_epci 
    INNER JOIN df_phyto 
    ON df_epci.insee = df_phyto.code_insee
    ORDER BY dept, annee
    """

    df_phyto_merged = duckdb.sql(query)

    # Quantité totale de substance par département, par année et par EPCI
    query = """ 
    SELECT 
        annee,
        dept,
        siren,
        SUM(quantite_substance) AS total_quantite_substance
    FROM df_phyto_merged
    GROUP BY siren, dept, annee
    ORDER BY dept, annee, siren
    """

    df_phyto_epci_year = duckdb.sql(query)

    #jointure des données phyto avec la surface agricole utile
    query = """
    SELECT 
        e1.annee,
        e1.dept,
        e1.siren,
        e1.total_quantite_substance,
        e2.valeur,
        e1.total_quantite_substance / NULLIF(e2.valeur,0) AS quantite_par_ha_sau
    FROM df_phyto_epci_year e1
    LEFT JOIN df_sau e2
    ON e1.siren = e2.geocode_epci
    ORDER BY e1.dept, e1.annee, e1.siren
    """

    df_final = duckdb.sql(query)

    #Sauvegarde du fichier final
    output_path = processed_dir / "phyto_epci_sau.csv"
    df_final.to_csv(output_path, index=False)
    print(f"Fichier sauvegardé : {output_path}")
    
if __name__ == "__main__":
    main()