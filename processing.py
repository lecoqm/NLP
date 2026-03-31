#IMPORTS
import pandas as pd
import os

#Get dataframe
candidate_info = pd.read_csv('data/archelect_search.csv')

path = os.getcwd() + '/data/'
text_extracted = pd.DataFrame(columns=['id', 'text'])
for folder in os.listdir(path):
    if "." not in folder:
        for file in os.listdir(path + folder):
            with open(path + folder + '/' + file, encoding="utf-8") as f:
                text = f.read()
            text_extracted = pd.concat([text_extracted, pd.DataFrame([[file.replace('.txt', ''), text]], columns=['id','text'])])

text_extracted = pd.merge(text_extracted, candidate_info, how='inner')

text_extracted = text_extracted[['id', 'text', 'date', 'contexte-election',
       'contexte-tour', 'cote', 'departement',
       'departement-insee', 'identifiant de circonscription', 'titulaire-sexe',
       'titulaire-profession', 'titulaire-mandat-en-cours',
       'titulaire-mandat-passe', 'titulaire-associations',
       'titulaire-autres-statuts', 'titulaire-soutien', 'titulaire-liste',
       'titulaire-decorations']]

text_extracted.to_csv("data/processed_data.csv", encoding='utf-8', index=False, header=True)