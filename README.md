# Velib_dataviz


# Google Cloud Platform (GCP)

## Install a Composer Environment
A Composer Environment is a service cloud to manage pipeline  of workflows with Airflows.

1. Create an account at the GCP. Personnaly, i use a free-trial
    Create a project. Select your project
2. Go to **API & Services** and activate the **API Cloud Composer**
3. Go to **IAM & Admin** and create a service account : 
    - Enter the name of the service account 
    - Grant the following roles :
        * Environment and Storage Object Administrator
        * Editor
        * Service Account User 
        * Logs Viewer
        * Composer Worker 
        * Cloud Composer v2 API Service Agent Extension
        - Storage Admin (permission to load data from GCS)
        - Cloud SQL Admin
        - Bigquery Admin
4. At the Cloud Console, enter `Composer`, then click it
5. Create a new Cloud Composer Environment 
6. Choose Composer 2
    - Choose a name to your Composer Environment 
    - Choose a location
    - Choose latest airflow version
    - Choose the service account you previously created
    - Choose environment resources
    - Keep the rest of the defaults settings
    - Click on create
7. The Composer Environment takes ~25min to be created
8. When you create a composer environment, it also create a bucket where your dags are
9. Open the Cloud Shell on GCP and run the command `gsutil ls -la`. It gives the list of bucket folders at the Google Cloud Storage

<p align="center">
  <img src="img/cloud_shell.png">
  <img src="img/bucket_folder.png">
</p>

```bash
gsutil cp gs://<votre-bucket-composer>/dags/<nom_du_dag>.py . #télécharger le dag localement dans le Cloud Shell
gsutil cp <dag_a_envoyer.py> gs://<votre-bucket-composer>/dags/<nom_du_dag>.py #envoyer dag dans le bucket
```



Create table in Bigquery
1. Navigate to BigQuery Console : Go to the [BigQuery page](https://console.cloud.google.com/bigquery) in the Google Cloud Console.
2. Select Dataset : Choose the dataset where you want to load the data.
3. Create Table : Click the “Create Table” button.
4. Source : Choose “Google Cloud Storage” as the source.
5. File Format : Select the format of your file (e.g., CSV, JSON, Avro, Parquet, ORC).
6. GCS URI : Enter the URI of your GCS file (e.g., `gs://bucket-name/file-name`).
7. Schema : Define the schema manually or use the auto-detect feature.
8. Create Table : Click “Create Table” to load the data.


# Data Visaluation on POWER BI

CURRENTLY DOING IT

<!---

python -m venv env

source env/bin/activate

AIRFLOW_VERSION=2.10.3

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.10.3 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.8.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"



# airflow standalone


airflow users create \
    --username cheongloic \
    --firstname loic \
    --lastname cheong\
    --role Admin \
    --email cheongloic@gmail.com

airflow webserver
airflow scheduler


systemctl --user start docker-desktop




airflow docker credentials: 
- user : airflow
- password : airflow


loic-cheong@loic-cheong-VirtualBox:~$ docker ps
Cannot connect to the Docker daemon at unix:///home/loic-cheong/.docker/desktop/docker.sock. Is the docker daemon running?


install airflow with docker : https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

-->





