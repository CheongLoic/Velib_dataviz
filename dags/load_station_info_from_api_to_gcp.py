from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import requests

# Arguments par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Délai entre chaque tentative en cas d'échec
}


BUCKET_NAME = 'us-central1-composer-serv-a-d79c16fe-bucket'  # Nom du bucket
BUCKET_FOLDER = "data" #folder in the bucket
BQ_DATASET = "velib_dataset" #Name of the dataset in Bigquery
BQ_TABLE = "station_info"  # data table : Nom de la table BigQuery 
filename = "velib_data_station_info.csv"  # Nom du fichier CSV
BUCKET_CSV_DATA_LOCATION = f"{BUCKET_FOLDER}/{filename}" 




# Fonction pour charger un DataFrame sur GCS
def load_station_info_to_gcp():
    # Get data from API
    api = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
    response = requests.get(api)

    # if (response.status_code == 200):
    print("API station_information is OK")
    #print(response.json())
    df = pd.DataFrame(response.json()["data"]["stations"])
    df = df.astype({'stationCode':int})
    df['credit_card'] = df.rental_methods.str.contains('CREDITCARD', regex=False)
    df.drop(columns=['rental_methods'], inplace = True)
    df.rename({
            "lat": "latitude",
            "lon": "longitude",
            "name": "station_name",
        }, axis=1, inplace=True)
    print("station_information fetched from API")
    
    # Initialiser le client Google Cloud Storage
    print("Connexion au GCS bucket...")
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)    
    # Convertir le DataFrame en CSV et charger directement sur GCS
    blob = bucket.blob(BUCKET_CSV_DATA_LOCATION)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv') #upload data
    print(f"Fichier chargé avec succès dans GCS : gs://{BUCKET_NAME}/{BUCKET_CSV_DATA_LOCATION}")



# Création du DAG
with DAG(
    'load_station_info_to_gcp',
    default_args=default_args,
    schedule_interval="0 1 1 * *",  #1er jour du mois à 1h du matin
    catchup=False,
    tags=['station_info'],
) as dag:
    
    
    # Tâche pour uploader le DataFrame
    upload_data_station_info_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=load_station_info_to_gcp,
        provide_context=True,
    )


    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket=BUCKET_NAME,
        source_objects=[BUCKET_CSV_DATA_LOCATION],  # retrive the BUCKET_CSV_DATA_LOCATION from xCom
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        schema_fields = [
            {"name": "station_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "stationCode", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "station_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "capacity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "credit_card", "type": "STRING", "mode": "NULLABLE"},
        ],  # Schéma de la table BigQuery
        write_disposition='WRITE_TRUNCATE',  # Supprime données de la table et rajoute
        source_format='CSV',  # Format du fichier source
        skip_leading_rows=1,  # Ignore la première ligne (en-têtes)
        field_delimiter=',',  # Délimiteur CSV
        allow_quoted_newlines=True,  # Autorise les retours à la ligne dans les champs
    )

    upload_data_station_info_to_gcs >> load_gcs_to_bq
