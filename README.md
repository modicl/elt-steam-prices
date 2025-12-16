# Steam Data Pipeline con Airflow

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) para extraer datos de videojuegos desde la API de Steam, procesarlos y generar un dataset unificado. Utiliza Apache Airflow orquestado con Docker.

## 📋 Prerrequisitos

Antes de comenzar, asegúrate de tener instalado:
- **Docker** y **Docker Compose**.
- Una **Steam Web API Key**. Puedes obtenerla gratuitamente en [https://steamcommunity.com/dev/apikey](https://steamcommunity.com/dev/apikey).

## 🚀 Instalación y Configuración

### 1. Clonar el repositorio
Descarga este proyecto en tu máquina local.

### 2. Configurar Credenciales

#### Steam API Key
Actualmente, la API Key está configurada en el script de extracción.
1. Abre el archivo `airflow/dags/plugins/extract_games.py`.
2. Busca la llamada a la función `get_all_steam_games` al final del archivo.
3. Reemplaza `'7F0CED09D773F260B336D65553BEDF2D'` con tu propia API Key si es necesario.

#### Google Cloud Credentials (Opcional)
Si planeas subir los datos a Google Cloud Storage (GCS):
1. Coloca tu archivo de credenciales JSON de Google Cloud en `airflow/google_credentials.json`.
2. Asegúrate de que el archivo se llame exactamente `google_credentials.json`.

### 3. Construir y Levantar Contenedores
Este proyecto utiliza una imagen personalizada de Airflow para incluir `steamcmd` (necesario para extraer metadatos).

Desde la carpeta `airflow/`:

```bash
# 1. Construir la imagen personalizada (SOLO LA PRIMERA VEZ o si cambias el Dockerfile)
docker-compose build

# 2. Iniciar los servicios
docker-compose up -d
```

Esto iniciará el Webserver, Scheduler y Postgres.

## ▶️ Ejecución

1. Abre tu navegador y ve a [http://localhost:8080](http://localhost:8080).
2. Inicia sesión con las credenciales por defecto (usualmente `airflow` / `airflow` si no se han cambiado en el docker-compose).
3. Busca el DAG llamado **`obtener_steam_games_data_v01`**.
4. Activa el DAG (toggle a "On") y ejecútalo manualmente (botón "Play").

## 📂 Estructura del Proyecto

- **`airflow/dags/`**: Contiene la definición del DAG (`get_steam_games.py`).
- **`airflow/dags/plugins/`**: Scripts Python con la lógica ETL:
  - `extract_games.py`: Descarga la lista completa de juegos.
  - `extract_game_prices.py`: Consulta precios por país.
  - `extraer_game_genres_publishers.py`: Usa `steamcmd` para obtener desarrolladores y géneros.
  - `join_steam_data.py`: Une todo en un archivo final.
- **`airflow/raw_data/`**: Carpeta donde se guardan los datos crudos intermedios (mapeada al host).
- **`airflow/dags/data/`**: Carpeta donde se guarda el resultado final `steam_final.json`.

## 🛠️ Solución de Problemas Comunes

- **Error de permisos en `raw_data`**: Asegúrate de que la carpeta `airflow/raw_data` tenga permisos de escritura.
- **Logs no visibles**: Si no puedes ver los logs de las tareas, asegúrate de estar usando `LocalExecutor` en el `docker-compose.yaml` para desarrollo local.
- **SteamCMD falla**: Verifica que la imagen se haya construido correctamente con `docker-compose build`.

## 📦 Output

El resultado final será un archivo JSON ubicado en `airflow/dags/data/steam_final.json` con la información consolidada de los juegos, precios y metadatos.
