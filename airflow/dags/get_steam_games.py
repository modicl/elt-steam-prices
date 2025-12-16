from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from plugins import extract_games , extract_game_prices , extract_game_genres_publishers, join_steam_data



default_args = {
    'owner' : 'felipe',
    'start_date' : datetime(2025,12,15),
    'retries' : 5   
    
}


with DAG(
    'obtener_steam_games_data_v02',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
) as dag:
    obtener_steam_games = PythonOperator(
        task_id = 'obtener_datos_steam',
        python_callable = extract_games.run_extraction
    )
    
    obtener_steam_precios = PythonOperator(
        task_id = 'obtener_precios_steam',
        python_callable = extract_game_prices.run_extraction
    )
    
    obtener_steam_metadatos = PythonOperator(
        task_id = 'obtener_metadatos',
        python_callable = extract_game_genres_publishers.run_extraction
    )
    
    unir_datos_steam = PythonOperator(
        task_id = 'unir_datos',
        python_callable = join_steam_data.run_transformation
    )
    
    subir_archivo = LocalFilesystemToGCSOperator(
        task_id = 'subir_datos_steam_gcs',
        src = '/opt/airflow/dags/data/steam_final.json',
        dst = 'raw/steam_raw_{{ ds }}.json',
        bucket = 'zoomcamp-steam-data-project',
        gcp_conn_id = 'google_cloud_default',
        mime_type = 'application/json'
    )
    
    obtener_steam_games >> [obtener_steam_precios, obtener_steam_metadatos] >> unir_datos_steam >> subir_archivo