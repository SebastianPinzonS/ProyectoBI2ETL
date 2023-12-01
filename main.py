from google.cloud import storage
import sqlite3
from io import StringIO
import pandas as pd

def download_csv(key_path, bucket_name, file_name, selected_columns, special_columns):
    storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text(encoding="latin-1")
    df = pd.read_csv(StringIO(content), sep=',')
    selected_data = df[selected_columns]
    selected_data[special_columns].fillna("no aplica", inplace = True)
    selected_data = selected_data.dropna().reset_index(drop=True)
    

    return selected_data

def insert_data_into_sqlite(cursor, table_name, data):
    try:
        for _, row in data.iterrows():
            query = f'INSERT OR IGNORE INTO {table_name} VALUES ({",".join(map(repr, row.values))})' ##To-Do: Es vulnerable a ataques de inyeccion
            cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
    

if __name__ == "__main__":
    key_path = "fit-reducer-406602-fd2b8b95e849.json"
    bucket_name = "no_asma_2021"
    file_name = "Datos_proyecto_II_BI_2021_sin_asma.csv"
    db_name = "mmd-no-asma-2021.db"
    fact_table = "ft_encuestados"
    ln = "localizacion"
    special_column = "NOMBRE_LOCALIDAD"
    columns_fact_table = ["DIRECTORIO_PER","NPCEP4","NOMBRE_LOCALIDAD","MPIO","SEXO","NPCEP26","NPCFP14C","NVCBP11AA"]
    columns_dpto_table = ["MPIO","NOMBRE_LOCALIDAD"] 
    ft = download_csv(key_path, bucket_name, file_name, columns_fact_table, special_column)
    dptot = download_csv(key_path, bucket_name, file_name, columns_dpto_table, special_column)
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    insert_data_into_sqlite(cursor,  ln, dptot)
    insert_data_into_sqlite(cursor, fact_table, ft)
    
    


    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()



