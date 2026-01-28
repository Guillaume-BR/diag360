import sys
import pandas as pd
import duckdb
import os
from pathlib import Path


# Ajouter le dossier parent de src (le projet) au path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.functions import *

base_dir = Path(__file__).resolve().parent.parent  # racine du projet diag360
data_dir = base_dir / "data" / "data_asso"

raw_dir = data_dir / "raw"
processed_dir = data_dir / "processed"

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)


def asso_sans_alsace() -> pd.DataFrame:
    """
    Charge les données des associations hors Alsace-Moselle.

    Returns
    -------
    pd.DataFrame
        DataFrame contenant les données des associations hors Alsace-Moselle nettoyées.
    """
    url_complete = (
        "https://www.data.gouv.fr/api/1/datasets/r/cc7b8f0c-45ea-4444-8b55-55d30bc34ac5"
    )
    filename_asso = "rna_waldec_20250901_complete.parquet"
    download_file(url_complete, extract_to=raw_dir, filename=filename_asso)
    df_asso = duckdb.read_parquet(str(raw_dir / filename_asso))

    # On garde les assos actives et on retire celles que l'on ne peut rattacher à une commune
    query = """ 
    SELECT 
        id,
        adrs_codeinsee, 
        adrs_codepostal
    FROM df_asso 
    WHERE position = 'A' 
            AND (adrs_codeinsee IS NOT NULL OR adrs_codepostal IS NOT NULL)
            AND (adrs_codeinsee != '0' OR adrs_codepostal != '00000')
            AND (adrs_codeinsee !='0' OR adrs_codepostal IS NOT NULL)
    ORDER BY adrs_codeinsee
    """

    df_asso_filtered = duckdb.sql(query).df()

    # Récupération des valeurs code_insee et code_postal manquantes via jointure avec df_com
    query_insee_not_null = """
    SELECT
        *
    FROM df_asso_filtered
    WHERE adrs_codeinsee IS NOT NULL 
        AND adrs_codeinsee!='0'
        """

    df_insee_not_null = duckdb.sql(query_insee_not_null).df()

    query_insee_null = """
    SELECT
        *
    FROM df_asso_filtered
    WHERE adrs_codeinsee IS NULL 
        OR adrs_codeinsee='0'
    """
    df_insee_null = duckdb.sql(query_insee_null).df()

    df_com = create_dataframe_communes(raw_dir)

    for _, row in df_insee_null.iterrows():
        code_postal = row["adrs_codepostal"]
        code_insee = df_com[df_com["code_postal"] == code_postal]["code_insee"].values[
            0
        ]
        print(f"Code postal: {code_postal} -> Code insee: {code_insee}")
        row["adrs_codeinsee"] = code_insee

    query = """ 
    select * from df_insee_null
    union
    select * from df_insee_not_null
    """

    df_asso_als = duckdb.sql(query)

    # On règle les problèmes de code postal
    query_sans_pb_postal = """ 
    SELECT * 
    FROM df_asso_als 
    WHERE adrs_codepostal not like '00%' 
    ORDER BY adrs_codeinsee
    """

    df_sans_pb_postal = duckdb.sql(query_sans_pb_postal).df()

    query_pb_postal = """ 
    SELECT *
    FROM df_asso_als
    WHERE adrs_codepostal like '00%'
    ORDER BY adrs_codeinsee
    """

    df_pb_postal = duckdb.sql(query_pb_postal)

    ##Correction des codes postaux de Paris
    query_paris_corrige = """ 
    SELECT 
        id, 
        '75056' AS adrs_codeinsee,
        '75012' AS adrs_codepostal
    FROM df_pb_postal 
    WHERE adrs_codeinsee = '75112' and adrs_codepostal like '00%'
    ORDER BY adrs_codeinsee
    """

    df_paris_corrige = duckdb.sql(query_paris_corrige).df()

    ##On revient aux codes postaux problématiques en enlevant paris corrigé
    query_pb_postal_sans_paris = """
    SELECT * 
    FROM df_pb_postal
    WHERE adrs_codeinsee != '75112'
    """
    df_pb_postal_sans_paris = duckdb.sql(query_pb_postal_sans_paris).df()

    # Correction des derniers codes postaux problématiques via jointure avec df_com
    for _, row in df_pb_postal_sans_paris.iterrows():
        code_insee = row["adrs_codeinsee"]
        try:
            code_postal = df_com[df_com["code_insee"] == code_insee][
                "code_postal"
            ].values[0]
        except IndexError:
            print(f"Code insee: {code_insee} not found in df_com")
            continue

    # On supprime les derniers problèmes
    df_pb_postal_clean = df_pb_postal_sans_paris[
        df_pb_postal_sans_paris["adrs_codepostal"] != "00000"
    ]

    # On concatène les dataframes pour obtenir le dataframe final
    df_asso_sans_final = pd.concat(
        [df_sans_pb_postal, df_pb_postal_clean, df_paris_corrige]
    )

    return df_asso_sans_final


def asso_alsace_moselle() -> pd.DataFrame:
    # Données des associations pour 57,67,68 (Alsace-Moselle)
    dico_url = {
        "57": "https://www.data.gouv.fr/api/1/datasets/r/f5073265-9689-441c-bd6d-8d9fbd360161",
        "67": "https://www.data.gouv.fr/api/1/datasets/r/b7acf7a2-1480-465e-b02b-22633d0a378d",
        "68": "https://www.data.gouv.fr/api/1/datasets/r/b7d6b412-5da6-4ed2-97cc-c9d8e7b321de",
    }

    df_asso = pd.DataFrame()
    for dept, url in dico_url.items():
        download_file(url, extract_to=raw_dir, filename=f"asso_{dept}.csv")
        df_dept = pd.read_csv(str(raw_dir / f"asso_{dept}.csv"), sep=";", dtype=str)
        df_asso = pd.concat([df_asso, df_dept], ignore_index=True)

    # On garde les assos inscrites
    query = """ 
    SELECT 
    NUMERO_AMALIA as id,
    COMMUNE as commune,
    CODE_POSTAL as adrs_codepostal
    FROM df_asso
    WHERE ETAT_ASSOCIATION = 'INSCRITE'
    ORDER BY CODE_POSTAL
    """

    df_asso_filtered = duckdb.sql(query).df()

    # On règle les problèmes de code postal
    # query = """
    # SELECT *
    # FROM df_asso_filtered
    # WHERE LENGTH(adrs_codepostal) != 5
    # """
    # df_asso_filtered_5 = duckdb.sql(query).df()

    mapping_code_postal = {
        "57": "57050",
        "5700": "57000",
        "570000": "57000",
        "573000": "57300",
        "57657660": "57660",
        "67000": "67000",
        "670000": "67000",
        "680000": "68000",
        "681180 ": "68118",
        "684809": "68480",
        "686102": "68610",
    }

    df_asso_corrige = df_asso_filtered.replace({"adrs_codepostal": mapping_code_postal})
    return df_asso_corrige


def main():
    # Données des associations hors Alsace-Moselle
    df_asso_sans_als = asso_sans_alsace().reset_index(drop=True)

    # Données des associations pour 57,67,68 (Alsace-Moselle)
    df_asso_als = asso_alsace_moselle().reset_index(drop=True)

    # Création de la table duckdb des communes
    df_com = create_dataframe_communes(raw_dir)

    # Création de la table duckdb des epci
    df_epci = create_dataframe_epci(raw_dir)

    # jointure avec les communes pour récupérer les codes insee
    query_join = """ 
    SELECT 
        a.id,
        c.nom_standard_majuscule as commune,
        a.adrs_codepostal,
        c.code_insee as adrs_codeinsee  
    FROM df_asso_als a
    LEFT JOIN df_com c
    ON a.commune = c.nom_standard_majuscule 
        AND LEFT(a.adrs_codepostal, 2) = LEFT(c.code_postal, 2)
    ORDER BY a.adrs_codepostal  
    """

    df_joined = duckdb.sql(query_join)

    # Concaténation des deux dataframes des assos
    query_union = """
    SELECT 
        id,
        adrs_codeinsee,
        adrs_codepostal 
    FROM df_joined

    UNION

    SELECT * 
    FROM df_asso_sans_als
    ORDER BY adrs_codeinsee
    """

    df_asso_complete = duckdb.sql(query_union)

    # Jointure avec les epci pour le calcul de l'indicateur
    query = """ 
    SELECT 
        TRY_CAST(siren AS INTEGER) AS siren,
        raison_sociale,
        dept,
        insee,
        TRY_CAST(REPLACE(total_pop_tot, ' ', '') AS INTEGER) AS total_pop_tot,
    from df_epci
    """

    df_epci = duckdb.sql(query)

    # query pour le calcul de l'indicateur i131
    query = """
    SELECT 
        e2.dept,
        CAST(e2.siren AS VARCHAR) as id_epci, 
        e2.raison_sociale as nom_epci,
        'i131' AS id_indcator,
        ROUND(count(e1.adrs_codeinsee) / e2.total_pop_tot * 1000,2) AS valeur_brute
    FROM df_epci e2
    LEFT JOIN df_asso_complete e1
    ON e1.adrs_codeinsee = e2.insee
    GROUP BY e2.dept,e2.siren,e2.total_pop_tot, e2.raison_sociale
    ORDER BY dept, siren
    """

    df_asso_epci = duckdb.sql(query)
    
    # Sauvegarde du fichier final
    output_file = processed_dir / "i131_asso_per_epci.csv"
    df_asso_epci.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")

    # Calcul de l'indicateur pour la BDD interne
    query_bdd = """
    SELECT 
        id_epci, 
        id_indcator,
        valeur_brute,
        '2025' AS annee
    FROM df_asso_epci
    """
    df_asso_bdd = duckdb.sql(query_bdd)

    # Sauvegarde du fichier bdd
    output_file = processed_dir / "asso_per_epci.csv"
    df_asso_bdd.write_csv(str(output_file))
    print(f"Fichier sauvegardé : {output_file}")


if __name__ == "__main__":
    main()  # asso.py
