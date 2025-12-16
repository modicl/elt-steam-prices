import subprocess
import json
import re
import os
import platform
import time

# Detectar sistema operativo o leer variable de entorno
if platform.system() == "Windows":
    DEFAULT_PATH = r"C:\Users\Villa\Downloads\steamcmd\steamcmd.exe"
else:
    DEFAULT_PATH = "/steamcmd/steamcmd.sh"

STEAMCMD_PATH = os.getenv("STEAMCMD_PATH", DEFAULT_PATH)

def parse_vdf(text):
    """
    Parsea texto VDF (KeyValues) a un diccionario Python.
    Implementación más robusta para manejar anidamiento.
    """
    # Eliminamos comentarios
    text = re.sub(r'//.*', '', text)
    
    # Tokenizador: captura cadenas entre comillas, o llaves
    # El regex busca: "string" O { O }
    tokens = re.findall(r'"([^"]*)"|({)|(})', text)
    
    res = {}
    stack = [res]
    expect_key = True
    current_key = None

    for val, open_brace, close_brace in tokens:
        if open_brace:
            if current_key is None:
                # Caso raro: { sin clave previa (no debería pasar en VDF válido)
                continue
            new_dict = {}
            stack[-1][current_key] = new_dict
            stack.append(new_dict)
            expect_key = True
            current_key = None
        
        elif close_brace:
            if len(stack) > 1:
                stack.pop()
            expect_key = True
        
        elif val is not None: # Es una cadena (clave o valor)
            if expect_key:
                current_key = val
                expect_key = False
            else:
                # Es un valor
                stack[-1][current_key] = val
                expect_key = True # Esperamos la siguiente clave
                current_key = None
    
    return res

def find_developer_recursive(data):
    """Busca recursivamente cualquier clave que parezca developer."""
    if isinstance(data, dict):
        for k, v in data.items():
            # Normalizamos la clave a minúsculas
            key_lower = k.lower()
            
            # Lista de claves candidatas para desarrollador
            if key_lower in ['developer', 'dev', 'publisher']: 
                # A veces publisher es el mismo dev en juegos indies
                return v
            
            if isinstance(v, (dict, list)):
                res = find_developer_recursive(v)
                if res: return res
    return None

def extract_game_info(app_data):
    """Extrae type y developer de un diccionario de datos de app."""
    info = {"type": "Unknown", "developer": None}
    
    # 1. Buscar 'type'
    if 'common' in app_data and isinstance(app_data['common'], dict):
        info['type'] = app_data['common'].get('type', 'Unknown')
    
    if info['type'] == 'Unknown':
        info['type'] = app_data.get('type', 'Unknown')

    # 2. Buscar 'developer' con estrategia escalonada
    
    # Intento A: extended -> developer (Lo más común en juegos modernos)
    if 'extended' in app_data and isinstance(app_data['extended'], dict):
        info['developer'] = app_data['extended'].get('developer')
    
    # Intento B: common -> developer
    if not info['developer'] and 'common' in app_data and isinstance(app_data['common'], dict):
        info['developer'] = app_data['common'].get('developer')

    # Intento C: Búsqueda recursiva en todo el objeto
    if not info['developer']:
        info['developer'] = find_developer_recursive(app_data)
        
    # Intento D: Caso especial para juegos de Valve antiguos (gamedir)
    # Si sigue siendo null y el tipo es game, a veces 'gamedir' nos da una pista (ej: "cstrike")
    if not info['developer'] and 'config' in app_data:
         gamedir = app_data['config'].get('gamedir')
         if gamedir in ['cstrike', 'halflife', 'tf', 'dod']:
             info['developer'] = "Valve"

    return info

def get_app_info_batch(app_ids):
    ids_str = " ".join(map(str, app_ids))
    cmd = [
        STEAMCMD_PATH,
        "+login", "anonymous",
        "+app_info_request", ids_str,
        "+app_info_print", ids_str,
        "+quit"
    ]
    
    try:
        print(f"Ejecutando steamcmd para {len(app_ids)} apps...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, encoding='utf-8', errors='ignore')
        output = result.stdout
        
        # Parseamos TODO el output como un gran VDF si es posible, 
        # pero steamcmd escupe mucho texto basura antes y después.
        # Estrategia: Buscar bloques que parezcan VDF válidos de apps.
        
        parsed_results = {}
        
        # El output de app_info_print suele tener la estructura:
        # "appid"
        # {
        #   ...
        # }
        
        # Intentamos limpiar el output para dejar solo lo que parece VDF
        # Buscamos desde la primera aparición de una appid del lote
        
        for app_id in app_ids:
            s_appid = str(app_id)
            # Regex para encontrar: "10" seguido de salto de linea y {
            # Usamos re.MULTILINE
            pattern = re.compile(rf'^\s*"{s_appid}"\s*$', re.MULTILINE)
            match = pattern.search(output)
            
            if not match:
                continue
                
            start_pos = match.start()
            
            # Ahora intentamos extraer el bloque balanceado de llaves {}
            # Empezamos a buscar la primera { después del match
            brace_start = output.find('{', start_pos)
            if brace_start == -1:
                continue
                
            # Contamos llaves
            count = 0
            found = False
            end_pos = -1
            
            for i in range(brace_start, len(output)):
                if output[i] == '{':
                    count += 1
                    found = True
                elif output[i] == '}':
                    count -= 1
                
                if found and count == 0:
                    end_pos = i + 1
                    break
            
            if end_pos != -1:
                block_str = output[start_pos:end_pos]
                # Parseamos este bloque individualmente
                # El bloque es algo como: "10" { ... }
                # Nuestro parser espera contenido VDF.
                
                # Truco: envolvemos en llaves para que sea un dict válido
                # { "10" { ... } }
                wrapped_block = "{\n" + block_str + "\n}"
                try:
                    data = parse_vdf(wrapped_block)
                    if s_appid in data:
                        info = extract_game_info(data[s_appid])
                        if info['type'] != 'Unknown': # Filtramos basura
                            parsed_results[s_appid] = info
                except Exception as e:
                    print(f"Error parseando bloque de {s_appid}: {e}")

        return parsed_results

    except subprocess.TimeoutExpired:
        print("⏰ Timeout steamcmd")
        return {}
    except Exception as e:
        print(f"Error steamcmd: {e}")
        return {}

def run_extraction():
    # Configuración de directorios
    raw_data_dir = '/opt/airflow/raw_data'
    if not os.path.exists('/opt/airflow'):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        airflow_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        raw_data_dir = os.path.join(airflow_dir, 'raw_data')

    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)

    input_file = os.path.join(raw_data_dir, 'steam_games.json')
    output_file = os.path.join(raw_data_dir, 'steam_metadata_final.json')
    partial_file = os.path.join(raw_data_dir, 'steam_metadata_partial.json')

    if not os.path.exists(input_file):
        print("No input file")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        all_games = json.load(f)
        # Aseguramos que sean ints
        all_ids = [int(g['appid']) for g in all_games]

    BATCH_SIZE = 20 # Reducimos batch size para evitar buffer overflow en output
    results = {}
    
    if os.path.exists(partial_file):
        try:
            with open(partial_file, 'r') as f: results = json.load(f)
        except: pass

    total = len(all_ids)
    
    for i in range(0, total, BATCH_SIZE):
        batch = all_ids[i : i + BATCH_SIZE]
        
        # Check si ya están procesados
        if all(str(bid) in results for bid in batch):
            continue
            
        print(f"Procesando {i}/{total}...")
        data = get_app_info_batch(batch)
        if data:
            results.update(data)
            
        if i % 100 == 0:
            with open(partial_file, 'w') as f: json.dump(results, f, indent=4)
            
        time.sleep(1.5) # Pausa importante

    with open(output_file, 'w') as f: json.dump(results, f, indent=4)
    if os.path.exists(partial_file): os.remove(partial_file)

if __name__ == "__main__":
    run_extraction()