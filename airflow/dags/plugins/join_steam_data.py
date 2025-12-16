import json
import os
from datetime import date

def run_transformation():
    # CONFIGURACION DE RUTAS
    # Detectamos si estamos en Airflow (Docker) o local
    if os.path.exists('/opt/airflow'):
        base_dir = '/opt/airflow'
        raw_data_dir = os.path.join(base_dir, 'raw_data')
        output_dir = os.path.join(base_dir, 'dags', 'data')
    else:
        # Fallback local
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # plugins -> dags -> airflow
        airflow_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        raw_data_dir = os.path.join(airflow_dir, 'raw_data')
        output_dir = os.path.join(airflow_dir, 'dags', 'data')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    GAMES_LIST_FILE = os.path.join(raw_data_dir, 'steam_games.json')
    GAMES_PRICES_FILE = os.path.join(raw_data_dir, 'steam_games_prices.json')
    
    # Preferimos el metadata final, si no existe usamos el parcial
    GAMES_METADATA = os.path.join(raw_data_dir, 'steam_metadata_final.json')
    if not os.path.exists(GAMES_METADATA):
        GAMES_METADATA = os.path.join(raw_data_dir, 'steam_metadata_partial.json')
        
    GAMES_FINAL_LIST_FILE = os.path.join(output_dir, 'steam_final.json')

    print(f"Inputs: {GAMES_LIST_FILE}, {GAMES_PRICES_FILE}, {GAMES_METADATA}")
    print(f"Output: {GAMES_FINAL_LIST_FILE}")

    # Script que une los precios al juego correspondiente

    # 1. Deserializamos lista juegos (Recordar que aca las ids estan como un numero entero)
    if not os.path.exists(GAMES_LIST_FILE):
        print(f"Error: No existe {GAMES_LIST_FILE}")
        return

    with open(GAMES_LIST_FILE , 'r' , encoding = 'utf-8') as f:
        games = json.load(f)
        
    # 1.1 La lista de juegos no trae un indice por objeto, si no que trae el indice dentro
    # Debemos convertir el appid como indice para lograr O(1)

    games_dict = {str(game['appid']): game for game in games}

    # 1.2 Borramos appid que contiene adentro
    for key in games_dict.keys():
        games_dict[key].pop('appid',None)


    # 1.3 Creamos un diccionario dentro de cada juego con tag "prices" , "type" , "developer" y un atributo fecha actualizacion

    for game in games:
        game["prices"] = {}
        game["type"] = ""
        game["developer"] = ""
        game["date"] = str(date.today())

    # 2. Deserializamos lista precios jueos

    prices = { }
    
    if os.path.exists(GAMES_PRICES_FILE):
        try:
            with open(GAMES_PRICES_FILE , 'r' , encoding = 'utf-8') as f:
                prices = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error critico leyendo precios: {e}. Se omitiran los precios.")
            prices = {}
        

        # 2.1 Añadimos cada precio a la llave "prices"

        for key,values in prices.items():
            try:
                if key in games_dict:
                    games_dict[key]['prices'] = values
            except Exception:
                continue 
    else:
        print("Warning: No prices file found")

    # 3. Deserializamos lista metadatos juegos

    metadata = { }

    if os.path.exists(GAMES_METADATA):
        try:
            with open(GAMES_METADATA , 'r' , encoding = 'utf-8') as f:
                games_metadata_json = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error critico leyendo metadatos: {e}. Se omitiran metadatos.")
            games_metadata_json = {}

        # 3.1 Añadimos cada type y developer como atributo segun appid    
        errors_count = 0
        for key,values in games_metadata_json.items():
            try:
                if key in games_dict:
                    # Usamos .get() para evitar KeyError si falta type o developer
                    games_dict[key]["type"] = values.get('type', 'Unknown')
                    games_dict[key]["developer"] = values.get('developer', 'Unknown')
            except Exception as e:
                errors_count += 1
        
        if errors_count > 0:
            print(f"Hubo {errors_count} errores al unir metadatos (llaves faltantes o formato incorrecto).")
    else:
        print("Warning: No metadata file found")

    # 4. Creamos el json final (falta refactorizar)

    if not os.path.exists(GAMES_FINAL_LIST_FILE):
        try:
            with open(GAMES_FINAL_LIST_FILE , 'w' , encoding = 'utf-8') as f:
                json.dump(games_dict , f , indent = 4)
        except Exception as e:
            print(f"Algo ocurrio : {e}")
    else:
        try:
            print(f"El archivo {GAMES_FINAL_LIST_FILE} ya existe, debemos apendar")
            with open(GAMES_FINAL_LIST_FILE , 'r' , encoding = 'utf-8') as f:
                games_list_existing = json.load(f)
            
            # update devuelve None, modifica in-place
            games_list_existing.update(games_dict)
            
            with open(GAMES_FINAL_LIST_FILE , 'w' , encoding = 'utf-8') as f:
                json.dump(games_list_existing , f , indent = 4)
            
        except Exception as e:
            print(f"Algo ocurrio : {e}")

if __name__ == "__main__":
    run_transformation()