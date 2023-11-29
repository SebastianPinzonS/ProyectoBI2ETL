from google.cloud import storage
import sqlite3
from io import StringIO
import pandas as pd

def download_csv(key_path, bucket_name, file_name, selected_columns):
    storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    content = blob.download_as_text(encoding="latin-1")
    df = pd.read_csv(StringIO(content), sep=',')
    selected_data = df[selected_columns].dropna().reset_index(drop=True)
    return selected_data

def insert_data_into_sqlite(cursor, table_name, data):
    for _, row in data.iterrows():
        # Construct the SQL query
        query = f'INSERT INTO {table_name} VALUES ({",".join(map(repr, row.values))})'

        # Execute the query
        cursor.execute(query)

if __name__ == "__main__":
    key_path = "fit-reducer-406602-fd2b8b95e849.json"
    bucket_name = "no_asma_2021"
    file_name = "Datos_proyecto_II_BI_2021_sin_asma.csv"
    db_name = "mmd-no-asma-2021.db"
    fact_table = "ft_enfermedades_mentales"
    columns_fact_table = ["DIRECTORIO_PER","NPCFP14C"]
    df = download_csv(key_path, bucket_name, file_name, columns_fact_table)

    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    insert_data_into_sqlite(cursor, fact_table, df)


    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()



