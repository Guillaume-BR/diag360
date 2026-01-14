#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from bs4 import BeautifulSoup
import pandas as pd
import csv
import re

file_path = "/home/guillaume/Documents/Github/diag360/data/data_mediaraw/media_locaux.txt"

def extraire_donnees_media(chemin_fichier):
        with open(chemin_fichier, 'r', encoding='utf-8') as f:
            contenu = f.read()

        # 1. On récupère d'abord tout le texte entre les balises <a>...</a>
        # Le format est : <a href="#">Texte (Ville)</a>
        balises = re.findall(r'<a href="#">(.*?)</a>', contenu)

        data = []

        for item in balises:
            # 2. On sépare le nom de la ville
            # On cherche la DERNIÈRE parenthèse de la chaîne
            # (.*) -> Nom du média
            # \s -> espace
            # \(([^)]+)\)$ -> Contenu de la dernière parenthèse à la fin de la chaîne
            match = re.search(r'(.*)\s\(([^)]+)\)$', item)

            if match:
                nom_media = match.group(1).strip()
                ville = match.group(2).strip()
                data.append([nom_media, ville])
            else:
                # Cas de secours si le format est différent
                data.append([item, "Inconnue"])

        # 3. Création du DataFrame
        df = pd.DataFrame(data, columns=['Nom_media', 'Ville'])
        return df

    # Exécution
try:
    df_medias = extraire_donnees_media(str(file_path))
    print("Extraction réussie :")
    print(df_medias.head())

    # Optionnel : Sauvegarder en CSV
    df_medias.to_csv("medias_extraits.csv", index=False)
except Exception as e:
    print(f"Erreur lors de la lecture du fichier : {e}")

df_medias.head()


# In[73]:


df_medias.head(70)


# In[75]:


df_medias.to_csv("../data/processed/media.csv", index=False, sep=";")


# In[12]:


import os
import sys
from pathlib import Path
import requests

def download_file(url: str, extract_to: str = '.', filename: str = None) -> None : 
    """
    Télécharge un fichier depuis une URL et l'enregistre localement.

    Le fichier est téléchargé uniquement s'il n'existe pas déjà
    dans le répertoire de destination.

    Parameters
    ----------
    url : str
        URL du fichier à télécharger.
    extract_to : str, optional
        Répertoire de destination du fichier (par défaut : répertoire courant).
    filename : str
        Nom du fichier local (avec extension).

    Raises
    ------
    requests.exceptions.RequestException
        En cas d'erreur réseau lors du téléchargement.
    """

    if not os.path.exists(extract_to):
        os.makedirs(extract_to, exist_ok=True)
        print(f"Dossier créé : {extract_to}")

    filename = os.path.join(extract_to, filename)

    if not os.path.exists(filename):
        response = requests.get(url)
        response.raise_for_status()
        print(f"Téléchargement du fichier : {filename}")

        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Fichier téléchargé avec succès : {filename}")

def float_to_codepostal(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Convertit une colonne contenant des codes postaux numériques en format chaîne à 5 caractères.

    Cette fonction est destinée aux cas où les codes postaux ont été lus comme
    des nombres flottants (ex. `1400.0`) et doivent être restaurés en chaînes
    avec zéros initiaux (ex. `01400`).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame contenant la colonne à transformer.
    col : str
        Nom de la colonne contenant les codes postaux.

    Returns
    -------
    pandas.DataFrame
        DataFrame avec la colonne des codes postaux convertie en chaînes
        de longueur 5.

    Notes
    -----
    - La fonction modifie le DataFrame en place et le retourne.
    - Les valeurs manquantes sont converties en chaînes `'nan'`
      si elles ne sont pas nettoyées en amont.
    """

    df[col] = (
        df[col]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.zfill(5)
    )
    return df

def create_dataframe_communes(dir_path):
    com_url = (
        "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"
    )
    download_file(com_url, extract_to=dir_path, filename="communes_france_2025.csv")
    df_com = pd.read_csv(dir_path / "communes_france_2025.csv")
    df_com = float_to_codepostal(df_com, "code_postal")
    return df_com

df_com = create_dataframe_communes(Path("../data/raw/"))


# In[13]:


df_com.head()


# In[14]:


df_com.columns


# In[ ]:





# In[15]:


import duckdb


# In[16]:


query = """
SELECT 
    code_insee,
    nom_standard,
    dep_code,
    epci_code,
    epci_nom,
    df_medias.Nom AS media_nom  
FROM df_com  
INNER JOIN df_medias
ON df_com.nom_standard = df_medias.Ville
"""

df_result = duckdb.query(query).to_df()
df_result.shape


# In[ ]:


df_result[df_result["d"] == "14"]


# ## Suppression des doublons de villes

# In[18]:


dup_com = (
    df_com
    .groupby("nom_standard")
    .size()
    .reset_index(name="n_com")
    .query("n_com > 1")
)

dup_com


# In[19]:


dup_medias = (
    df_medias
    .groupby("Ville")
    .size()
    .reset_index(name="n_medias")
    .query("n_medias > 1")
)
dup_medias


# In[20]:


villes_ambigues = (
    dup_com
    .merge(dup_medias, left_on="nom_standard", right_on="Ville", how="inner")
)

villes_ambigues


# In[84]:


df_result[df_result["nom_standard"] == "Blanquefort"]


# In[22]:


df_temp = df_result[
    (df_result["nom_standard"] != "Bailleul") |
    (df_result["dep_code"] == '59')
]


# In[ ]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Blanquefort") | ((df_temp["dep_code"] == '33') & df_temp["media_nom"] == "R.I.G" )
                  ]


# In[24]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Castres") | ((df_temp["dep_code"] == '81'))
                  ]


# In[25]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Chaumont") | ((df_temp["dep_code"] == '52'))]


# In[26]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Clamecy") | ((df_temp["dep_code"] == '58'))]


# In[27]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Falaise") | ((df_temp["dep_code"] == '14'))]


# In[28]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Flers") | ((df_temp["dep_code"] == '61'))]


# In[29]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Fontaine") | ((df_temp["dep_code"] == '38'))]


# In[30]:


df_temp = df_temp[ (df_temp["nom_standard"] != "La Rochelle") | ((df_temp["dep_code"] == '17'))]


# In[31]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Langon") | ((df_temp["dep_code"] == '33'))]


# In[ ]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Marmagne") | ((df_temp["dep_code"] == '71'))]


# In[33]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Montreuil") | ((df_temp["dep_code"] == '93'))]


# In[34]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Moulins") | ((df_temp["dep_code"] == '03'))]


# In[35]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Olivet") | ((df_temp["dep_code"] == '45'))]


# In[36]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Prades") | ((df_temp["dep_code"] == '66'))]


# In[37]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Rochefort") | ((df_temp["dep_code"] == '17'))]


# In[38]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Saint-Claude") | ((df_temp["dep_code"] == '39'))]


# In[39]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Saint-Nazaire") | ((df_temp["dep_code"] == '44'))]


# In[40]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Saint-Claude") | ((df_temp["dep_code"] == '39'))]


# In[41]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Saint-Omer") | ((df_temp["dep_code"] == '62'))]


# In[42]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Saint-Raphaël") | ((df_temp["dep_code"] == '83'))]


# In[43]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Ussel") | ((df_temp["dep_code"] == '19'))]


# In[44]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Valence") | 
                  ((df_temp["nom_standard"] == "Valence") & (df_temp["dep_code"] == '82') & (df_temp["media_nom"].isin(["VFM","La Dépêche du Midi"])))|
                  ((df_temp["nom_standard"] == "Valence") & (df_temp["dep_code"] == '26') & (~df_temp["media_nom"].isin(["VFM","La Dépêche du Midi"])))
                  ]


# In[45]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Verdun") | ((df_temp["dep_code"] == '55'))]


# In[46]:


df_temp = df_temp[ (df_temp["nom_standard"] != "Vernon") | ((df_temp["dep_code"] == '27'))]


# In[47]:


df_temp.shape


# In[48]:


df_temp.drop_duplicates(inplace=True)
df_temp.shape


# In[76]:


#différence des villes entre df_médias et df_result_final
set_villes_medias = set(df_medias["Ville"].unique())
set_villes_result = set(df_temp["nom_standard"].unique())
set_villes_diff = set_villes_medias - set_villes_result
set_villes_diff


# In[3]:


ville_mapping = {
    "7 à Poitiers": "Poitiers",
    "89 Rnb": "Auxerre",
    "ADN": "Saint-Affrique",
    "Avranches FM": "Avranches",
    "Beaubreuil": "Limoges",
    "Bourg Les Valence": "Bourg-lès-Valence",
    "C.F.M.": "Châteaubriant",
    "C2L": "Châlette-sur-Loing",
    "Charleville-Mézieres": "Charleville-Mézières",
    "Cherbourg": "Cherbourg-en-Cotentin",
    "Cherbourg-En-Cotentin": "Cherbourg-en-Cotentin",
    "Château du Loir": "Montval-sur-Loir",
    "Château-Chinon (Ville": "Château-Chinon (Ville)",
    "Cierp Gaud": "Cierp-Gaud",
    "DNA": "Strasbourg",
    "Digne les Bains": "Digne-les-Bains",
    "Echouboulains": "Échouboulains",
    "Hdr": "Rouen",
    "L'informateur de Seine et Oise": "Versailles",
    "La Radio Étudiante": "Orange",
    "Le Journal du Sud-Vienne": "Civray",
    "MAP": "Nantes",
    "Made in Perpignan": "Perpignan",
    "Marennes Oléron TV": "Marennes",
    "Normandie Picardie": "Louviers",
    "Ou Radio Samoens": "Samoëns",
    "O² Radio": "Cenon",
    "R.A.M": "Embrun",
    "R.V.B": "Bergerac",
    "R.V.M.": "Crépy-en-Valois",
    "RCB": "Besançon",
    "RCV99FM": "Lille",
    "RDB": "Le Cheylard",
    "REIPM FM": "Pau",
    "RKS 97": "Grenoble",
    "RPL": "Landivisiau",
    "Radio Bergerac": "Bergerac",
    "Radio Caen Métropole Normandie": "Caen",
    "Radio Mégahertz": "Ladignac-le-Long",
    "Radio Thau Sete": "Sète",
    "Radio bassin Arcachon": "Arcachon",
    "Rdm": "Thiaucourt-Regniéville",
    "Rgb": "Brive-la-Gaillarde",
    "Rjm": "Montluçon",
    "Rlp": "Coulounieix-Chamiers",
    "Rmj": "Magnac-Laval",
    "Rvi 101.4": "Villefranche-de-Lonchat",
    "Rvm": "Le Bélieu",
    "SAINT-AIGNAN DE GRAND LIEU": "Saint-Aignan-Grandlieu",
    "Saint-Quentin-en-Yvelines": "Montigny-le-Bretonneux",
    "Sanary": "Sanary-sur-Mer",
    "Sarlat": "Sarlat-la-Canéda",
    "St Philbert de Grand-Lieu": "Saint-Philbert-de-Grand-Lieu",
    "TVB": "Lyon",
    "Terrasson": "Terrasson-Lavilledieu",
    "Télévision Loire 7": "Saint-Genest-Lerpt",
    "Témoins Sur Les Ondes": "Lille",
    "VO News 95": "Cergy",
    "Vallée Du Rhône": "Romans-sur-Isère",
    "Vaux-Sur-Mer": "Vaux-sur-Mer",
    "Wrp": "Paris",
    "femmes en Périgord": "Périgueux",
    "la Seyne": "La Seyne-sur-Mer",
    "s": "Grenoble",
    "zone de publication libre": "Palaiseau",
    "e": "Lorient",
    "S": "Lyon",
}

df_medias["Ville"] = df_medias["Ville"].replace(ville_mapping)


# In[ ]:


nom_mapping = {
    'Mistral Social Club': 'Salon-de-Provence',
    'Mon Pays': 'Toulouse',
    'Tamtam': 'Bezons',
}

df_medias["Ville"] = df_medias["Nom"].replace(nom_mapping)


# In[2]:


df_medias.loc[df_medias["Ville"] ==  'S'
               , "Nom"]


# In[195]:


df_com.loc[df_com["nom_standard"].str.startswith('La Seyne')]


# In[78]:


df_medias[df_medias["Ville"].isin(set_villes_diff)].shape


# In[49]:


# Finaliser le DataFrame
df_result_final = df_temp.copy()


# In[66]:


#ligne à supprimer
import requests
url_media_non_independants = "https://raw.githubusercontent.com/mdiplo/Medias_francais/refs/heads/master/medias.tsv"
df = pd.read_csv(url_media_non_independants, sep = "\t")
df


# In[67]:


#on retire de df_result_final les médias présents dans df
df_final = df_result_final[~df_result_final["media_nom"].isin(df["Nom"])]
df_final.shape


# In[68]:


df_final.head(50)


# In[69]:


query_by_dept = """ 
SELECT 
    dep_code as dept,
    count(media_nom) AS n_medias
FROM df_final
GROUP BY dep_code
ORDER BY dep_code
"""

nb_medias_par_dept = duckdb.query(query_by_dept).to_df()
nb_medias_par_dept


# In[70]:


query_by_epci = """ 
SELECT
    dep_code as dept,
    epci_code,
    count(media_nom) AS n_medias
FROM df_final
GROUP BY dep_code, epci_code
ORDER BY dep_code, epci_code
"""

nb_medias_par_epci = duckdb.query(query_by_epci).to_df()
nb_medias_par_epci


# In[ ]:




