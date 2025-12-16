import requests
import json
import os
import time

# CONFIGURACION
INPUT_FILENAME = 'steam_games.json'
OUTPUT_FILENAME = 'steam_games_prices.json'
BATCH_SIZE = 50
COUNTRIES = ['us','cl','ar']
RATE_LIMIT_SLEEP = 1.6

def cargar_juegos(path):
    if not os.path.exists(path):
        print(f"Error: El archivo de entrada {path} no existe.")
        return []
        
    with open(path, 'r' , encoding = 'utf-8') as f:
        data = json.load(f)
    return data


def obtener_precios(app_ids , country):
    # Convertimos la lista de ints a string separado por comas
    ids_str = ','.join(map(str,app_ids))
    
    url = 'https://store.steampowered.com/api/appdetails'
    params = {
        "appids" : ids_str,
        "cc" : country,
        "filters" : "price_overview" # Solo permite este filtro si es multiples appids
    }
    
    try:
        response = requests.get(url , params = params , timeout = 10)
        
        if response.status_code == 429:
            print("====== WARNING ======")
            print("Codigo 429!!")
            print("====== FIN WARNING ======")
            time.sleep(60)
            return None # Retornamos None para reintentar el batch
        
        if response.status_code != 200:
            print(f"Error {response.status_code} en {country}")
            return {}
        
        
        return response.json()
    except Exception as e:
        print(f"Excepción en {country} : {e}")
        return {}
    
def run_extraction():
    """Función principal para ser llamada desde Airflow"""
    
    # Definir directorio de trabajo (raw_data)
    # Preferimos la ruta del contenedor, con fallback local
    raw_data_dir = '/opt/airflow/raw_data'
    if not os.path.exists('/opt/airflow'):
        # Fallback para ejecución local fuera de Docker
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # plugins -> dags -> airflow -> raw_data (ajustar según estructura real)
        # Asumiendo que plugins está en airflow/plugins o airflow/dags/plugins
        # Si está en airflow/dags/plugins:
        airflow_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        raw_data_dir = os.path.join(airflow_dir, 'raw_data')

    print(f"Usando directorio de trabajo: {raw_data_dir}")
    
    if not os.path.exists(raw_data_dir):
        print(f"Creando directorio: {raw_data_dir}")
        os.makedirs(raw_data_dir)

    input_path = os.path.join(raw_data_dir, INPUT_FILENAME)
    output_path = os.path.join(raw_data_dir, OUTPUT_FILENAME)

    games = cargar_juegos(input_path)
    if not games:
        print("No se cargaron juegos. Terminando.")
        return

    total_games = len(games)
    print(f"Total de juegos a procesar : {total_games}")
    
    appids = [game['appid'] for game in games]
    
    # Estructura deseada: { "10": { "us": {...}, "cl": {...}, "ar": {...} } }
    precios_finales = {}
    
    # Si el archivo de salida ya existe, podríamos querer cargar lo que ya hay para no empezar de cero

    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                precios_finales = json.load(f)
            print(f"Cargados {len(precios_finales)} precios existentes.")
        except:
            print("No se pudo leer el archivo de salida existente, empezando de cero.")

    # Iteramos primero por lotes, y DENTRO del lote por países.
    for i in range(0, total_games, BATCH_SIZE):
        batch_ids = appids[i : i + BATCH_SIZE]
        print(f"Procesando lote {i} a {i + len(batch_ids)}...")
        
        # Aseguramos que las llaves existan en el diccionario final
        for app_id in batch_ids:
            str_id = str(app_id)
            if str_id not in precios_finales:
                precios_finales[str_id] = {}

        for country in COUNTRIES:
            datos_precios = obtener_precios(batch_ids, country)
            
            if datos_precios:
                for app_id_str, info in datos_precios.items():
                    # Solo guardamos si fue exitoso y tiene datos
                    if info.get('success') and 'data' in info:
                        data_content = info['data']
                        
                        # Verificar que 'data' sea un diccionario y no una lista vacía
                        if isinstance(data_content, dict) and 'price_overview' in data_content:
                            if app_id_str not in precios_finales:
                                precios_finales[app_id_str] = {}
                                
                            precios_finales[app_id_str][country] = data_content['price_overview']
            
            # Pausa entre países para no saturar
            time.sleep(RATE_LIMIT_SLEEP)
            
        # Guardado parcial cada X lotes
        if i % (BATCH_SIZE * 5) == 0:
             with open(output_path , 'w' , encoding = 'utf-8') as f:
                json.dump(precios_finales, f, indent=4)

    # Guardado final
    with open(output_path , 'w' , encoding = 'utf-8') as f:
        json.dump(precios_finales, f, indent=4)
        
if __name__ == "__main__":
    run_extraction()