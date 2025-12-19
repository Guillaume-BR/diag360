import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_cat_nat"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

def main():
    # Define URLs and file paths
    path_cat_nat =  base_dir / "data" / "raw" / "cat_nat.csv"
    df_cat_nat = pd.read_csv(path_cat_nat,skiprows=2,sep=';')

    # Création de la table duckdb pour les jointures
    df_com = create_dataframe_communes(raw_dir)

    #Mise en forme des données
    mapping = {
    "Code": "siren",
    "Libellé": "nom_epci",
    "Nombre d'Arrêtés de Catastrophes Naturelles publiés au J.O.": "nb_cat_nat"
    }
    df_cat_nat = df_cat_nat.rename(columns=mapping)
    df_cat_nat.loc[df_cat_nat['siren'] == 75056, 'siren'] = 200054781   
    print("Données cat_nat chargées et renommées.")

    #Surface de chaque epci
    query = """
    SELECT 
        epci_code as siren,
        sum(superficie_km2) AS superficie_km2
    FROM df_com
    WHERE (superficie_km2 IS NOT NULL) AND (epci_code != 'ZZZZZZZZZ')
    GROUP BY epci_code
    """

    df_surface_epci = duckdb.sql(query)

    query = """
    SELECT e.siren,
        df_cat_nat.nb_cat_nat,
        ROUND(1.0*df_cat_nat.nb_cat_nat / e.superficie_km2, 3) AS cat_nat_per_km2
    FROM df_surface_epci AS e
    LEFT JOIN df_cat_nat
    ON e.siren = df_cat_nat.siren
    """

    df_cat_nat_final = duckdb.sql(query)

    #Sauvegarde du fichier final
    output_file = processed_dir / "cat_nat_per_epci.csv"
    df_cat_nat_final.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")

if __name__ == "__main__":
    main()  # asso.py  

