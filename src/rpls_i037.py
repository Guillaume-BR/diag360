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
data_dir = base_dir / "data" / "data_rpls"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

def main():
    #chargement des données de rpls
    data_rpls = pd.read_csv(raw_dir / "resultats_rpls.csv",sep=';', low_memory=False)

    #Chargement de la table des epci
    df_epci = pd.read_csv(base_dir / "data" / "processed" / "epci_membres.csv", sep=',')

    #Traitement de la table des rpls
    df_rpls = (data_rpls[['DEPCOM_ARM','nb_ls2023', 'nb_ls2019']]
               .copy()
               .rename(columns=
                       {'DEPCOM_ARM': 'code_insee', 
                        'nb_ls2023': 'ls_2023', 
                        'nb_ls2019': 'ls_2019'
                        })
    )
   
    df_rpls['code_insee'] = df_rpls['code_insee'].replace(' ','').apply(lambda x: str(x).zfill(5))
    df_rpls['ls_2023'] = df_rpls['ls_2023'].str.replace('\xa0','').astype(int)
    df_rpls['ls_2019'] = df_rpls['ls_2019'].str.replace('\xa0','').astype(int)

    #modification des codes_inse de paris, marseille et lyon pour les faire correspondre à ceux de l'epci
    df_rpls.loc[df_rpls['code_insee'].str.startswith("75"), 'code_insee'] = "75056"
    df_rpls.loc[df_rpls['code_insee'].str.startswith("132"), 'code_insee'] = "13055"
    df_rpls.loc[df_rpls['code_insee'].str.startswith("693"), 'code_insee'] = "69123"

    #query pour avoir l'indicteur
    query = """
    SELECT 
        df_epci.dept_epci as dept_id,
        df_epci.siren as epci_id,
        df_epci.epci_nom AS epci_lib,
        'i037' as id_indicator,
        case WHEN sum(df_rpls.ls_2019) = 0 THEN null
             ELSE ROUND((sum(df_rpls.ls_2023) - sum(df_rpls.ls_2019)) / sum(df_rpls.ls_2019)  * 100, 2) END as valeur_brute,
        '2023' as annee
    FROM df_epci
    JOIN df_rpls 
        ON df_epci.code_insee = df_rpls.code_insee
    GROUP BY df_epci.dept_epci,df_epci.siren, df_epci.epci_nom
    ORDER BY df_epci.dept_epci,df_epci.siren
    """

    df_final = duckdb.sql(query)

    print(f"taille du résultat : {df_final.df().shape}")

    #sauvegarde du résultat
    output_path = processed_dir / "i037_rpls.csv"
    df_final.write_csv(str(output_path))
    print(f"résultat sauvegardé dans : {output_path}")

if __name__ == "__main__":
    main()


