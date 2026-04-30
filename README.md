# Topic Modeling and temporal evolution
This project analyse the Archelec corpus under the spectrum of topic modelling and temporal evolution.

## create venv

* create a virtual environment: ```virtualenv -p python3 venv```
* activate the virtual env: ```venv/Scripts/activate```
* install the required packages:  ```pip install -r requirements.txt```
* register the virtualenv with jupyter: ``` python -m ipykernel install --name=venv ```

## Notebooks and code

To create a csv file with all the text and meta-data needed, run:
```bash
python preprocessing.py \
  --base-dir data \
  --candidate-file archelect_search.csv \
  --soutien-mapping-csv data/partis_correspondance.csv \
  --mandat-mapping-csv data/mandats_correspondance.csv \
  --output data/texts_processed.csv \
  --columns id text date contexte-tour departement "identifiant de circonscription" titulaire-sexe titulaire-age titulaire-mandat-en-cours titulaire-mandat-passe titulaire-soutien
```

`first_topic_analysis.ipynb` gives the results of section 2. `LDA.ipynb` gives the results of section 4. `DTM.ipynb` gives the results of section 5.
