# Velib_dataviz

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