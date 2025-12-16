from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime

default_args = {
    'owner' : 'felipe',
    'start_date' : datetime(2025,12,15),
    'retries' : 5   
    
}


with DAG(
    'subida_storage_elt',
    default_args = default_args,
    schedule_interval = None,
    catchup = False
) as dag:
    
    subir_archivo = LocalFilesystemToGCSOperator(
        task_id = 'subir_datos_steam',
        src = '/opt/airflow/dags/data/steam_final.json',
        dst = 'raw/steam_raw_{{ ds }}.json',
        bucket = 'zoomcamp-steam-data-project',
        gcp_conn_id = 'google_cloud_default',
        mime_type = 'application/json'
    )
    
    subir_archivo