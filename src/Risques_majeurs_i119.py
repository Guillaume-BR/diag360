import sys
import pandas as pd
import duckdb
import os
from pathlib import Path

# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "risques_majeurs"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)



def fetch_api_payload() -> tuple[pd.DataFrame, Path]:
    """Charge et nettoie les données de risques majeurs"""

    # Lire le CSV
    path_file = raw_dir / "i119.csv"
    if not path_file.exists():
        raise FileNotFoundError(f"Fichier {path_file} introuvable après extraction")

    df_risques = pd.read_csv(path_file, sep=";", header=2, low_memory=False)

    mapping = {
        "Code": "code_insee",
        "Libellé": "nom_commune",
        "risque d'inondations, 2019": "inondations",
        "risque de mouvements de terrain, 2019": "mouvements_terrain",
        "risque de séismes, 2019": "seismes",
        "risque d'avalanches, 2019": "avalanches",
        "risque de feux de forêt, 2019": "feux_foret",
        "risque de lié à des phénomènes atmosphériques, 2019": "phenomenes_atmo",
        "risque d'éruptions volcaniques, 2019": "eruptions",
        "risque industriel, 2019": "industriel",
        "risque nucléaire, 2019": "nucleaire",
        "risque de rupture de barrage, 2019": "barrage",
        "risque lié au transport de marchandises dangereuses, 2019": "transport_matieres",
        "risque lié aux engins de guerre": "engins_guerre",
        "risque d'affaissements miniers, 2019": "affaissements_miniers",
    }

    df_risques = df_risques.rename(columns=mapping).drop(columns=["nom_commune"])

    df_risques["code_insee"] = df_risques["code_insee"].apply(lambda x: str(x).zfill(5))
    
    # Modification des valeurs commençant par "N/A" en Nan
    df_risques = df_risques.replace(r"^N/A.*", pd.NA, regex=True)
    
    return df_risques

def main():
    # chargement des données des pharmacies
    df_risques = fetch_api_payload()

    # Chargement de la table des communes
    df_com = create_dataframe_communes(raw_dir)

    #jointure avec les communes pour obtenir les codes epci
    query = """ 
    SELECT 
        df_com.epci_code AS id_epci,
        df_risques.*
    FROM df_risques
    LEFT JOIN df_com
        ON df_risques.code_insee = df_com.code_insee"""
    
    df_risques_epci = duckdb.sql(query)
    print(df_risques_epci.df().head())

    #compter le nombre de risques majeurs par epci
    query = """ 
    SELECT
        id_epci,
        SUM(TRY_CAST(inondations AS INTEGER)) AS inondations,
        SUM(TRY_CAST(mouvements_terrain AS INTEGER)) AS mouvements_terrain,
        SUM(TRY_CAST(seismes AS INTEGER)) AS seismes,
        SUM(TRY_CAST(avalanches AS INTEGER)) AS avalanches,
        SUM(TRY_CAST(feux_foret AS INTEGER)) AS feux_foret,
        SUM(TRY_CAST(phenomenes_atmo AS INTEGER)) AS phenomenes_atmo,
        SUM(TRY_CAST(eruptions AS INTEGER)) AS eruptions,
        SUM(TRY_CAST(nucleaire AS INTEGER)) AS nucleaire,
        SUM(TRY_CAST(barrage AS INTEGER)) AS barrage,
        SUM(TRY_CAST(transport_matieres AS INTEGER)) AS transport_matieres,
        SUM(TRY_CAST(engins_guerre AS INTEGER)) AS engins_guerre,
        SUM(TRY_CAST(affaissements_miniers AS INTEGER)) AS affaissements_miniers,
        SUM(TRY_CAST(industriel AS INTEGER)) AS industriel
    FROM df_risques_epci
    WHERE id_epci IS NOT NULL and id_epci != 'ZZZZZZZZZ'
    GROUP BY id_epci
    """

    df_total_risques = duckdb.sql(query)

    #MAitenant si pour chaque risque on mets si >0 alors 1 sinon 0
    query_bdd = """
    SELECT
        id_epci,
        'i119' AS id_indicator,
        SUM(CASE WHEN inondations > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN mouvements_terrain > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN seismes > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN avalanches > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN feux_foret > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN phenomenes_atmo > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN eruptions > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN nucleaire > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN barrage > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN transport_matieres > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN engins_guerre > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN affaissements_miniers > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN industriel > 0 THEN 1 ELSE 0 END)
        AS total_risques,
        '2019' AS annee
    FROM df_total_risques
    GROUP BY id_epci;
    """
    df_total_risques_par_epci = duckdb.sql(query_bdd)

    #sauvegarde du fichier traité
    output_path = processed_dir / "i119_total_risques_epci.csv"
    df_total_risques_par_epci.write_csv(str(output_path))
    print(f"Données risques majeurs sauvegardées dans {output_path}")

    df_epci = create_dataframe_epci(raw_dir)

    query = """ 
        SELECT 
            DISTINCT siren AS id_epci,
            raison_sociale AS nom_epci,
            dept
        FROM df_epci
    """

    df_epci = duckdb.sql(query)

    #query complete with join
    query = """
        SELECT
            s1.dept,
            CAST(s1.id_epci AS VARCHAR) AS id_epci,
            s1.nom_epci,
            'i119' AS id_indicator,
            s2.total_risques as valeur_brute 
        FROM df_epci AS s1
        LEFT JOIN df_total_risques_par_epci AS s2
            ON CAST(s1.id_epci AS VARCHAR) = CAST(s2.id_epci AS VARCHAR)
        ORDER BY s1.dept, s1.id_epci
    """

    df_complete = duckdb.sql(query)

    output_path_complete = processed_dir / "i119_total_risques.csv"
    df_complete.write_csv(str(output_path_complete))
    print(f"Données risques majeurs complètes sauvegardées dans {output_path_complete}")


if __name__ == "__main__":
    main()