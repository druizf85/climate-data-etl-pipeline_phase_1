import os
import sys
folder_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(folder_path)
import src.procedures as pr
import logging
import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


new_raw_folder = os.path.join(folder_path, 'data', 'raw_data', 'new_data')
new_clean_folder = os.path.join(folder_path, 'data', 'clean_data', 'new_data')

url = Variable.get('URL_TEMP')
user = Variable.get('USER')
password = Variable.get('PSSW')


today_dt = datetime.today().date()
today = str(today_dt)

yesterday_dt = today_dt - timedelta(days=1)
yesterday = str(yesterday_dt)

last_date = pr.get_last_date_file(new_raw_folder)

query = f'SELECT codigoestacion, codigosensor, fechaobservacion, valorobservado, nombreestacion, departamento, municipio, zonahidrografica, latitud, longitud, descripcionsensor, unidadmedida WHERE fechaobservacion BETWEEN "{last_date}T00:00:00"::floating_timestamp AND "{yesterday}T23:45:00"::floating_timestamp'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Log creado con éxito')

logging.info(query) #ELIMINAR ESTO DESPUÉS


# logging.error(
# logging.warning(

def extract_function():

    json_cons_new = pr.initial_extract(url, user, password, query)

    if len(json_cons_new) == 0:
        logging.error('No se encontraron resultados en la consulta')
        raise ValueError('No se encontraron resultados en la consulta')

    new_raw_file_parquet = pr.new_raw_file_parquet(folder_path, today)
    df_new_raw = pd.json_normalize(json_cons_new)
    df_preprocessed = pr.raw_preprocess(df_new_raw)
    
    try:
        df_preprocessed.to_parquet(new_raw_file_parquet, index=False)
        logging.info('Se ha incorporado el dataframe raw a la carpeta new_data')
    except Exception as e:
        logging.error(f'No se pudo incorporar la dataframe a un archivo .parquet: {e}')
            
    new_query_file_sql = pr.new_query_file_sql(folder_path, today)
    
    try:
        pr.incorporate_query(query, new_query_file_sql)
        logging.info(f'{today} -> Información desde: {df_preprocessed["fechaobservacion"].min()} - hasta: {df_preprocessed["fechaobservacion"].max()} -> Cantidad de registros {len(df_preprocessed)}')

    except Exception as e:
        logging.error(f'No se pudo incorporar la query a un archivo .sql: {e}')
    
    
def transform_function():
    
    raw_file_list = os.listdir(new_raw_folder)
    raw_file_list.sort(reverse=True)
    last_file_date = raw_file_list[0]
    df_preprocessed = pd.read_parquet(os.path.join(new_raw_folder, last_file_date))

    new_clean_file_parquet = pr.new_clean_file_parquet(folder_path, today)
    transformed_df = pr.transform_data(df_preprocessed)
    transformed_df.loc[:, 'batch'] = f'BATCH_{today}'
    transformed_df = transformed_df[['identificador', 'batch', 'codigoestacion', 'codigosensor', 'fechaobservacion', 'mesobservacion', 'valorobservado', 'nombreestacion', 'departamento', 'municipio', 'zonahidrografica', 'latitud', 'longitud', 'descripcionsensor', 'unidadmedida']]
    
    try:
        transformed_df.to_parquet(new_clean_file_parquet, index=False)
        logging.info('Se ha incorporado el dataframe clean a la carpeta new_data')
    except Exception as e:
        logging.error(f'No se pudo incorporar la dataframe a un archivo .parquet: {e}')



def load_function():
    
    import src.engine_creation as e
    
    user_postgre = Variable.get('USER_POSTGRE')
    password_postgre = Variable.get('PASS_POSTGRE')
    # host_postgre = Variable.get('HOST_POSTGRE')
    port_postgre = Variable.get('PORT_POSTGRE')
    database_postgre = Variable.get('DB_NAME_POSTGRE')

    clean_file_list = os.listdir(new_clean_folder)
    clean_file_list.sort(reverse=True)
    last_file = clean_file_list[0]
    transformed_df = pd.read_parquet(os.path.join(new_clean_folder, last_file))
    logging.warning(f'la cantidad de registros del dataframe es: {len(transformed_df)}')
    
    engine = e.engine_postgre_for_docker(user_postgre, password_postgre, port_postgre, database_postgre)
    csv_path = os.path.join(new_clean_folder, 'temp_df.csv')
    transformed_df.to_csv(csv_path, index=False)
    logging.info('dataframe transformada a formato csv')
    try:
        pr.load_massive_files(new_clean_folder, engine)
        logging.info(f'Dataframe {last_file} cargada exitosamente en el Datawarehouse mediante carga csv e inserción')
    except Exception as e:
        logging.error(f'Se presenta un error al cargar la dataframe en el Datawarehouse: {e}')
        
        
        
default_args = {'retries': 3, 'retry_delay': timedelta(minutes=30)}            
            
with DAG(dag_id='Temperature_DAG', 
         start_date=datetime(2025, 8, 1, 0, 0), 
         schedule='@daily',
         catchup=False,
         default_args = default_args) as dag:
    
    extract_task = PythonOperator(task_id='extract_task', python_callable=extract_function, execution_timeout=timedelta(minutes=60))
    transform_task = PythonOperator(task_id='transform_task', python_callable=transform_function, execution_timeout=timedelta(minutes=60))
    load_task = PythonOperator(task_id='load_task', python_callable=load_function, execution_timeout=timedelta(minutes=60))
    
    extract_task >> transform_task >> load_task
