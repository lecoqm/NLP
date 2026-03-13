import sqlite3
from arkindex_export import open_database, Element, Metadata, Transcription, database
from arkindex_export.queries import list_children
from pathlib import Path
from tqdm import tqdm

DB_PATH = Path("sciencespo-archelec-20260217-121320.sqlite")




def index_database(db_path: Path, vacuum: bool = False):
    """
    Add performance indexes to an Arkindex SQLite export.
    """

    # Initialize database connection
    open_database(db_path)

    if database.is_closed():
        database.connect()

    with database.atomic():
        # Critical for recursive CTE performance
        database.execute_sql("""
            CREATE INDEX IF NOT EXISTS idx_elementpath_parent_child
            ON element_path(parent_id, child_id);
        """)

        database.execute_sql("""
            CREATE INDEX IF NOT EXISTS idx_elementpath_child
            ON element_path(child_id);
        """)

        # Useful if filtering on Element.type
        database.execute_sql("""
            CREATE INDEX IF NOT EXISTS idx_element_type
            ON element(type);
        """)

    if vacuum:
        print("Running VACUUM (this may take time)...")
        database.execute_sql("VACUUM;")

    database.close()

    print("Indexing completed.")


# Index the database before opening it
index_database(DB_PATH)

# load the  export
open_database(DB_PATH)
# create a folder to store the text files
TEXT_FOLDER = "text_files"
output_folder = Path(TEXT_FOLDER)
output_folder.mkdir(exist_ok=True)
YEARS = ['1981', '1988', '1993']
#YEARS = ['1958', '1962', '1967', '1968', '1973', '1978', '1981', '1988', '1993']
ELECTIONS = ['legislatives', 'presidentielle']
folder_id = {}
for year in YEARS:
    folder_id[year]= {}
    for type in ELECTIONS:
        # create the folder
        year_folder = output_folder / year
        year_folder.mkdir(exist_ok=True)
        type_folder = year_folder / type
        type_folder.mkdir(exist_ok=True)

# compute some statistics
print("Number of folders", Element.select().where(Element.type == 'folder').count())
print("Number of pages:", Element.select().where(Element.type == 'page').count())

folder_id['1958']['legislatives'] = '9336ff1b-d7c6-4ade-80c7-a77449facf56'
folder_id['1962']['legislatives'] = '8af5fb84-1925-4e6f-b504-82c8f6d6c84c'
folder_id['1967']['legislatives'] = 'c28c555f-3fa8-49af-9ca4-cc23f5a43a2b'
folder_id['1968']['legislatives'] = 'f931f0ad-b61d-4c04-970d-41db4d8977da'
folder_id['1973']['legislatives'] = '28a50138-4f88-4b52-aca6-457bf12e53ec'
folder_id['1978']['legislatives'] = 'b8b432e3-aaa1-4213-a95a-16a043b5b177'
folder_id['1981']['legislatives'] = 'd51ea3db-68ee-4cc0-a87f-736ee17c5f87'
folder_id['1988']['legislatives'] = 'dfba9f5c-02de-478c-85c5-0ee780455433'
folder_id['1993']['legislatives'] = 'cf29300f-40bf-4b61-be93-6cb631be8fab'

for year in YEARS:
    print ('year', year)
    for e_type in ELECTIONS:
        print ('elections', e_type)
        f_id = folder_id[year].get(e_type, None)
        if f_id:
            documents = list_children(f_id).where(Element.type == 'document')
            print(f_id,"Number of documents", documents.count())
            transcriptions_number = 0
            for document in tqdm(documents):
                pages = list_children(document.id).where(Element.type == 'page')
                transcriptions = ""
                for page in pages:
                    page_transcription = Transcription.select().where(Transcription.element == page.id).first()
                    if page_transcription:
                        transcriptions += page_transcription.text

                if transcriptions:
                    with open(f"{TEXT_FOLDER}/{year}/{e_type}/{document.name}.txt", "w") as f:
                        f.write(transcriptions)
                    transcriptions_number += 1
            print("Number of transcriptions", transcriptions_number)

