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


current_datetime = datetime.now() # Récupération de la date actuelle pour nommer le fichier
BUCKET_NAME = 'us-central1-composer-serv-a-d79c16fe-bucket'  # Nom du bucket
BUCKET_FOLDER = "data" #folder in the bucket
BQ_DATASET = "velib_dataset" #Name of the dataset in Bigquery
BQ_TABLE = "station_status"  # data table : Nom de la table BigQuery 


"""
This function concatenate a BUCKET_FOLDER and a csv filename 
Objetif : push value to xCom on Airflow and retrieve it later
Example of what we get : 'velib_data_station_status_2024-12-25_12-25-00.csv'
"""
def set_BUCKET_CSV_DATA_LOCATION():
    formatted_date = current_datetime.strftime('%Y-%m-%d_%H-%M-%S')  # Format compatible pour un fichier
    filename = f"velib_data_station_status_{formatted_date}.csv"  # Nom du fichier CSV
    BUCKET_CSV_DATA_LOCATION = f"{BUCKET_FOLDER}/{filename}" 
    return BUCKET_CSV_DATA_LOCATION


# Fonction pour charger un DataFrame sur GCS
def load_station_status_to_gcp(**kwargs):
    # Get data from API
    api = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    response = requests.get(api)
    
    df = pd.DataFrame(response.json()["data"]["stations"])
    df['timestamp'] = df["last_reported"].astype(str) #string
    df = df.astype({
        'is_installed':bool, 
        'is_returning':bool, 
        'is_renting':bool, 
        'stationCode':int,
        'last_reported':'datetime64[s]'}) #timestamp to YYYY-MM-DD HH:mm:ss format
    num_bikes_available_types = df["num_bikes_available_types"].apply(lambda l: pd.Series({**l[0], **l[1]})) # contain json data with only 2 values : mechanical and ebike
    df =  pd.concat([df, num_bikes_available_types], axis=1)
    df.drop(columns=['numBikesAvailable', 'numDocksAvailable', 'num_bikes_available_types'], inplace = True)
    df.rename({
        "mechanical": "mechanical_bike_available",
        "ebike": "electric_bike_available",
        "last_reported": "time",
    }, axis=1, inplace=True)
    

    # BUCKET_CSV_DATA_LOCATION = BUCKET_CSV_DATA_LOCATION 
    BUCKET_CSV_DATA_LOCATION = kwargs['ti'].xcom_pull(task_ids='set_csv_filename') #get data from xCom for function set_csv_filename()
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
    'load_station_status_to_gcp',
    default_args=default_args,
    schedule_interval="35 * * * *",  # Toutes les 10 minutes
    catchup=False,
    tags=['station_status'],
) as dag:
    
    

    # Tâche pour uploader le DataFrame
    set_csv_filename = PythonOperator(
        task_id='set_csv_filename',
        python_callable=set_BUCKET_CSV_DATA_LOCATION,
        provide_context=True,
    )


    # Tâche pour uploader le DataFrame
    upload_data_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=load_station_status_to_gcp,
        provide_context=True,
    )


    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket=BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='set_csv_filename') }}"],  # retrive the BUCKET_CSV_DATA_LOCATION from xCom
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        schema_fields = [
                {"name": "station_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "num_bikes_available", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "num_docks_available", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "is_installed", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "is_returning", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "is_renting", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "stationCode", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
                {"name": "mechanical_bike_available", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "electrical_bike_available", "type": "INTEGER", "mode": "NULLABLE"},
            ],  # Schéma de la table BigQuery
        write_disposition='WRITE_APPEND',  # Ajoute les données à la table existante
        source_format='CSV',  # Format du fichier source
        skip_leading_rows=1,  # Ignore la première ligne (en-têtes)
        field_delimiter=',',  # Délimiteur CSV
        allow_quoted_newlines=True,  # Autorise les retours à la ligne dans les champs
    )

    set_csv_filename >> upload_data_to_gcs >> load_gcs_to_bq
