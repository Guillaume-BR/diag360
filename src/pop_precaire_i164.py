import sys
import pandas as pd
import duckdb
import os
from pathlib import Path
from io import BytesIO


# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import download_file, download_stock_file, float_to_codepostal

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_pop_precaire"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

def main():
    #Chargement de la table des populaitons précaires
    data_precaire = pd.read_csv(base_dir / "data" / "raw" / "pop_precaire.csv", sep=";", header=2)

    #chargement de la table des epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=',')

    #Traitement de la table des populations précaires
    df_precaire = data_precaire.rename(columns={"Code":"code_insee", "Libellé":"nom_com","Part de la population en emploi précaire 2022":"valeur" })

    #query pour avoir l'indicteur
    query = """ 
    SELECT 
        df_epci.dept_epci as dept_id,
        df_epci.siren as epci_id,
        df_epci.epci_nom AS epci_lib,
        'i037' as id_indicator,
        round(sum(df_epci.pop_mun_commune * TRY_CAST(df_precaire.valeur AS float)) / df_epci.total_pop_mun,2) as valeur_brute,
        '2022' as annee
    FROM df_epci
    JOIN df_precaire
        ON df_epci.code_insee = df_precaire.code_insee
    GROUP BY df_epci.dept_epci,df_epci.siren, df_epci.epci_nom,total_pop_mun
    ORDER BY df_epci.dept_epci,df_epci.siren
    """

    df_final = duckdb.sql(query)
    print(df_final.df().shape)
    
    #sauvegarde du fichier final
    output_file = processed_dir / "i164_pop_precaire.csv"
    df_final.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")

if __name__ == "__main__":
    main()

