import os
import pandas as pd
from requests.auth import HTTPBasicAuth
import requests
import time
import urllib.parse
import logging



def construct_url_query(url, query, limit, offset):
    """ 
    Teniendo una query de base como input se anexan parámetros de limit y offset para la extracción de datos en la API. 
    Una vez estructurada la query, se transforma para poderla incorporar a la url y así hacer el request. 
    """
    full_query = f'{query} LIMIT {limit} OFFSET {offset}'
    encoded_query = urllib.parse.quote(full_query, safe='') #safe = '' codifica todo lo que no sea alfanumérico
    generated_url = f'{url}?$query={encoded_query}'
    return generated_url



def temp_initial_extract(url, user, password, query):
    """ 
    Teniendo la url de base que proporciona la API y el usuario y la contraseña necesarios, se hace una consulta a la API solicitando la información necesaria.
    Al tener un número máximo de registros por consulta se incorpora un listado al que se le agrega la totalidad de registros por consulta. 
    Se indica periodicamente la cantidad de registros incorporados y el acumulado.
    Se adicionan algunas verificaciones de conteo de requests y un tope máximo de requests para no colapsar el servidor y evitar un bloqueo a las consultas de manera preventiva.
    Se incorporan métodos para probar si las consultas han sido exitosas y en caso de presentarse un problema o exceder un límite de tiempo, mostrar los posibles errores, y reintentando en unos segundos
    """
    limit = 1000
    offset = 3138000
    sleep_time = 70
    request_count = 0
    max_requests = 59
    timeout = 60
    json_cons = []
    
    while True:
        if request_count >= max_requests:
            logging.warning(f'número máximo de consultas a la API alcanzado, reiniciando en {sleep_time} segundos...')
            time.sleep(sleep_time)
            request_count = 0
        
        # params = {'$limit': limit, '$offset': offset}
        generated_url = construct_url_query(url, query, limit, offset)
        
        try:
            response = requests.get(generated_url, auth=HTTPBasicAuth(user, password), timeout=timeout)
                    
            if response.status_code == 200:
                data = response.json()
            
                if not data:
                    logging.info('no hay más información por extraer, proceso exitoso.')
                    break
            
                json_cons.extend(data)
                logging.info(f'Se incorporan {len(data)} objetos json. Total de objetos json incorporados: {len(json_cons)}')
                request_count += 1
                offset += limit
            else:
                logging.error(f'Error en la conexión: {response.status_code} - {response.text}')
                break
        
        except requests.exceptions.ReadTimeout:
            logging.warning(f'Se ha alcanzado el límite de timeout en el offset: {offset}, esperando 20 segundos para continuar...')
            time.sleep(20)
        
        except Exception as e:
            logging.warning(f'Se presenta un error: {e}, esperando 20 segundos para continuar...')
            time.sleep(20)
            
    return json_cons



def initial_extract(url, user, password, query):
    """ 
    Teniendo la url de base que proporciona la API y el usuario y la contraseña necesarios, se hace una consulta a la API solicitando la información necesaria.
    Al tener un número máximo de registros por consulta se incorpora un listado al que se le agrega la totalidad de registros por consulta. 
    Se indica periodicamente la cantidad de registros incorporados y el acumulado.
    Se adicionan algunas verificaciones de conteo de requests y un tope máximo de requests para no colapsar el servidor y evitar un bloqueo a las consultas de manera preventiva.
    Se incorporan métodos para probar si las consultas han sido exitosas y en caso de presentarse un problema o exceder un límite de tiempo, mostrar los posibles errores, y reintentando en unos segundos
    """
    limit = 1000
    offset = 0
    sleep_time = 70
    request_count = 0
    max_requests = 59
    timeout = 60
    json_cons = []
    
    while True:
        if request_count >= max_requests:
            logging.warning(f'Número máximo de consultas a la API alcanzado, reiniciando en {sleep_time} segundos...')
            remaining = sleep_time
            
            while remaining > 0:
                time.sleep(5)
                remaining -= 5
                logging.info(f'Esperando {remaining} segundos para continuar...')
            
            request_count = 0
        
        generated_url = construct_url_query(url, query, limit, offset)
        
        try:
            response = requests.get(generated_url, auth=HTTPBasicAuth(user, password), timeout=timeout)
            # print(f'el user es {user}')
            # print(f'el password es {password}')
            # print(f'el url es {url}')
            # print(f'la query generada es {query}')
                    
            if response.status_code == 200:
                data = response.json()
            
                if not data:
                    logging.info('No hay más información por extraer, proceso exitoso.')
                    break
            
                json_cons.extend(data)
                logging.info(f'Se incorporan {len(data)} objetos json. Total de objetos json incorporados: {len(json_cons)}')
                request_count += 1
                offset += limit
            else:
                logging.error(f'Error en la conexión: {response.status_code} - {response.text}')
                break
        
        except requests.exceptions.ReadTimeout:
            logging.warning(f'Se ha alcanzado el límite de timeout en el offset: {offset}, esperando 20 segundos para continuar...')
            time.sleep(20)
        
        except Exception as e:
            logging.warning(f'Se presenta un error: {e}, esperando 20 segundos para continuar...')
            time.sleep(20)
            
    return json_cons



def raw_preprocess(df):
    """
    Preprocesamiento de la dataframe, ajustando formatos de algunas columnas
    """
    df['fechaobservacion'] = pd.to_datetime(df['fechaobservacion'])
    df['valorobservado'] = df['valorobservado'].astype(float)
    df['latitud'] = df['latitud'].astype(float)
    df['longitud'] = df['longitud'].astype(float)
    return df



def transform_data(df):
    """ 
    Se realiza un proceso de limpieza y transformación de datos, se incluyen columnas de mes de observación y de identificador único.
    """
    df = df.copy()

    dup = df.duplicated().sum()
    if dup > 0:
        logging.warning(f'Se encontraron {dup} filas duplicadas')
        df = df.drop_duplicates(keep='first')
    else:
        logging.info(f'No se encontraron filas duplicadas')
        
    na = df.isna().sum()
    
    if na.any():
        logging.warning(f'Se encontraron filas con valores nulos: {na}')
        df = df.dropna()
    else:
        logging.info(f'No se encontraron filas con valores nulos')

    df.loc[:, 'mesobservacion'] = df['fechaobservacion'].dt.month
    df.loc[:, 'identificador'] = df['codigoestacion'] + '_' + df['codigosensor'] + '_' + df['fechaobservacion'].astype(str)
        
    return df



def hist_raw_file_parquet(folder_path):
    """ 
    Devuelve el nombre del archivo dentro de la carpeta del proyecto donde se almacenarán los registros "raw" de las consultas a nivel histórico realizadas a la API en formato tabular.  
    """
    initial = 1
    output_raw = os.path.join(folder_path, 'data', 'raw_data', 'hist_data', f'raw_{initial}.parquet')

    while True:
        if os.path.exists(output_raw):
            initial += 1
            output_raw = os.path.join(folder_path, 'data', 'raw_data', 'hist_data', f'raw_{initial}.parquet')
        else:
            break
    
    return output_raw



def hist_clean_file_parquet(folder_path):
    """ 
    Devuelve el nombre del archivo dentro de la carpeta del proyecto donde se almacenarán los registros "clean" de las consultas a nivel histórico realizadas a la API en formato tabular.  
    """
    initial = 1
    output_raw = os.path.join(folder_path, 'data', 'clean_data', 'hist_data', f'clean_{initial}.parquet')

    while True:
        if os.path.exists(output_raw):
            initial += 1
            output_raw = os.path.join(folder_path, 'data', 'clean_data', 'hist_data', f'clean_{initial}.parquet')
        else:
            break
    
    return output_raw



def new_raw_file_parquet(folder_path, today):
    """ 
    Devuelve el nombre del archivo dentro de la carpeta del proyecto donde se almacenarán los registros "raw" de las consultas 
    que se hacen de manera automática a la API en formato tabular.  
    """
    initial = 1
    output_raw = os.path.join(folder_path, 'data', 'raw_data', 'new_data', f'{today}_raw.parquet')

    while True:
        if os.path.exists(output_raw):
            initial += 1
            output_raw = os.path.join(folder_path, 'data', 'raw_data', 'new_data', f'{today}_{initial}_raw.parquet')
        else:
            break
    
    return output_raw



def new_clean_file_parquet(folder_path, today):
    """ 
    Devuelve el nombre del archivo dentro de la carpeta del proyecto donde se almacenarán los registros "clean" de las consultas 
    que se hacen de manera automática a la API en formato tabular.  
    """
    initial = 1
    output_clean = os.path.join(folder_path, 'data', 'clean_data', 'new_data', f'{today}_clean.parquet')

    while True:
        if os.path.exists(output_clean):
            initial += 1
            output_clean = os.path.join(folder_path, 'data', 'clean_data', 'new_data', f'{today}_clean.parquet')
        else:
            break
    
    return output_clean


 
def hist_query_file_sql(folder_path): 
    """ 
    Devuelve el nombre del archivo dentro de la carpeta del proyecto donde se almacenarán las queries a nivel histórico realizadas a la API.
    """
    initial = 1
    output_raw = os.path.join(folder_path, 'queries_src', 'queries_hist_extraction', f'query{initial}.sql')

    while True:
        if os.path.exists(output_raw):
            initial += 1
            output_raw = os.path.join(folder_path, 'queries_src', 'queries_hist_extraction', f'query{initial}.sql')
        else:
            break
    
    return output_raw



def new_query_file_sql(folder_path, today):
    """ 
    Devuelve el nombre del archivo dentro de la carpeta del proyecto donde se almacenarán las queries que se incorporan de manera automática para hacer las consultas a la API.
    """
    initial = 1
    output_raw = os.path.join(folder_path, 'queries_src', 'queries_new_extraction', f'{today}_query.sql')

    while True:
        if os.path.exists(output_raw):
            initial += 1
            output_raw = os.path.join(folder_path, 'queries_src', 'queries_new_extraction', f'{today}_{initial}_query.sql')
        else:
            break
    
    return output_raw



def incorporate_query(query, query_path):
    """ 
    Incorporar una query como un archivo .sql y almacenarlo en una ubicación específica.
    """
    with open(query_path, 'w', encoding='utf-8') as f:
        f.write(query)
        logging.info('Se ha incorporado la query para esta consulta.')



def get_last_date_file(new_raw_folder):
    """ 
    Se verifica dentro de la carpeta "new_data" de "raw_data", el archivo más reciente. 
    De allí sacará la fecha del archivo y se tomará como fecha de inicio para la query de extracción que se actualizará automáticamente. 
    """
    raw_file_list = os.listdir(new_raw_folder)
    raw_file_list.sort(reverse=True)
    last_date = raw_file_list[0].split("_")[0]
    return last_date



def load_massive_files(csv_files, engine):
    """ 
    Cargar archivos masivamente cuando la dataframe es muy grande (más de 60 mil filas) de forma que se inserte la información desde un csv a la tabla del DataWarehouse.
    """
    for file in os.listdir(csv_files):
        if file.endswith('.csv'):
            
            csv_path = os.path.join(csv_files, file)
            logging.info(f'Leyendo archivo {file} para cargar en el Datawarehouse')
            
            conn = engine.raw_connection()
            
            try:
                cur = conn.cursor()
                cur.execute("CREATE TEMP TABLE tmp_hist_temp AS SELECT * FROM hist_temp LIMIT 0")
                logging.info(f'Se ha creado la tabla temporal para la carga del archivo {file}')
                
                
                with open(csv_path, 'r', encoding='utf-8') as f:
                    cur.copy_expert(f"""COPY tmp_hist_temp FROM STDIN WITH CSV HEADER DELIMITER ','""", f)
                    logging.info(f'Se ha copiado la información en la tabla temporal para la carga del archivo {file}')

                cur.execute("""INSERT INTO hist_temp 
                            SELECT * FROM  tmp_hist_temp
                            ON CONFLICT (identificador) DO NOTHING
                            """)
                logging.info(f'Se ha incorporado la información del archivo {file} en la tabla hist_temp del Datawarehouse')
                
                cur.execute("DROP TABLE IF EXISTS tmp_hist_temp")
                conn.commit()
                logging.info(f'Se han cargado todos los datos del archivo {file} en el Datawarehouse exitosamente')
                
                if os.path.exists(csv_path):
                    os.remove(csv_path)
                    logging.info(f"Archivo {file} eliminado después de carga.")
                    
            except Exception as e:
                logging.error(f'Se ha producido un error en la creación del cursor: {e}')
            
            finally:
                cur.close()
                conn.close()