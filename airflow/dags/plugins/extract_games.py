import requests
import json
import os
import time

def get_last_procesed_appid_v2(file_path):
    if not os.path.exists(file_path):
        return 0
    else:
        try:
            with open(file_path , 'r' , encoding = 'utf-8') as f:
                last_id = json.load(f)
            return last_id
        except Exception as e:
            print("========= ERROR =========")
            print(f"Algo ocurrió : {e}")
            print("========= FIN ERROR =========")
            return 0
            

def get_all_steam_games(key , last_app_id, output_dir):
    # Definimos las rutas completas usando el directorio de salida
    steam_games_path = os.path.join(output_dir, 'steam_games.json')
    last_app_id_path = os.path.join(output_dir, 'last_app_id.json')

    # Endpoint oficial que devuelve ID y Nombre de todo el catálogo de Steam
    url = f"https://api.steampowered.com/IStoreService/GetAppList/v1/?{key}"
    
    query_params = {
        "include_games": True,
        "include_dlc": True,
        "include_software": False,
        "last_appid" : last_app_id,
        "max_results" : 10000
    }
    
    params = {
        "key": key,
        "input_json": json.dumps(query_params)
    }
    
    print(f"El ultimo id de la otra sesion fue : {last_app_id}")
    
    try:
        print("Consultando el catálogo completo de Steam...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        apps = []
        try:
            apps = data['response']['apps']
        except KeyError as e:
            print(f"========= WARNING =========")
            print(f"Ya no encuentro más apps! (KeyError 'apps')")
            print(f"========= TERMINANDO PROCESO =========")
            return None # Indicamos que no hay más datos

        if not apps:
            print("La lista de apps está vacía. Terminando.")
            return None

        # Intentamos obtener el last_appid para la siguiente iteración
        try:
            next_last_id = data['response']['last_appid']
        except Exception as e:
            print(f"========= WARNING =========\n No encontré un last_id , ultimo batch de datos!, me traere la id de la ultima app desde la lista")
            print(f"========= FIN WARNING =========")
            next_last_id = apps[-1]['appid']
        
        print(f"¡Éxito! Se encontraron {len(apps)} aplicaciones en total.")
            
        # Guardar todo en un archivo
        if os.path.isfile(steam_games_path):
            print("========= INFO =========")
            print(f"El archivo {steam_games_path} existe, apendamos....")
            print("========= FIN INFO =========") 
            with open(steam_games_path, 'r', encoding = 'utf-8') as f:
                existing_data = json.load(f)
            
            if isinstance(existing_data , list):
                existing_data.extend(apps)
            else:
                print("========= ERROR =========")
                print("El JSON a apendar no es una lista!")
                print("========= FIN ERROR =========")
            
            with open(steam_games_path, 'w' , encoding = 'utf-8') as f:
                json.dump(existing_data, f , indent = 4)
        else:
            print("========= INFO =========")
            print(f"El archivo {steam_games_path} no existe, creando por primera vez")
            print("========= FIN INFO =========")
            with open(steam_games_path, 'w', encoding = 'utf-8') as f:
                json.dump(apps, f, ensure_ascii = False, indent = 4)
        
        # Guardamos la ultima id en el archivo
        print(f"Ultima id app de la query actual: {next_last_id}")
        with open(last_app_id_path, 'w' , encoding = 'utf-8') as f_last_id:          
            try:
                json.dump(next_last_id , f_last_id , indent = 4)
            except Exception as e:
                print(f"Algo ocurrió al guardar last_id: {e}")
        
        return next_last_id

    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con la API: {e}")
        raise e # Re-lanzamos para que Airflow sepa que falló
    except KeyError as e:
        print(f"Error de llave : {e}")
        raise e

def run_extraction():
    """Función principal llamada por Airflow"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    airflow_dir = os.path.dirname(os.path.dirname(current_dir))
    raw_data_dir = os.path.join(airflow_dir, 'raw_data')
    
    if not os.path.exists(raw_data_dir):
        print(f"Creando directorio: {raw_data_dir}")
        os.makedirs(raw_data_dir)

    last_app_id_file = os.path.join(raw_data_dir, 'last_app_id.json')
    
    # Bucle principal
    current_last_id = get_last_procesed_appid_v2(last_app_id_file)
    
    while True:
        print(f"--- Iniciando batch comenzando en {current_last_id} ---")
        next_id = get_all_steam_games('7F0CED09D773F260B336D65553BEDF2D', current_last_id, raw_data_dir)
        
        if next_id is None:
            print("No se devolvió un next_id. Fin del proceso.")
            break
        
        if next_id == current_last_id:
            print("El ID no ha cambiado. Posible fin de datos o bucle infinito. Terminando.")
            break
            
        current_last_id = next_id
        # Pequeña pausa para no saturar la API (opcional)
        time.sleep(1)

if __name__ == "__main__":
    run_extraction()