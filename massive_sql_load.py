import src.engine_creation as e
from dotenv import load_dotenv
import os

load_dotenv()

user_postgre = os.getenv('USER_POSTGRE')
password_postgre = os.getenv('PASS_POSTGRE')
host_postgre = os.getenv('HOST_POSTGRE')
port_postgre = os.getenv('PORT_POSTGRE')
database_postgre = os.getenv('DB_NAME_POSTGRE')

folder_path = os.getcwd() 

engine = e.engine_postgre(user_postgre, password_postgre, host_postgre, port_postgre, database_postgre)

clean_hist_csv_files = os.path.join(folder_path, 'data', 'clean_data', 'hist_data', 'csv')
clean_new_csv_files = os.path.join(folder_path, 'data', 'clean_data', 'new_data', 'csv')

def load_files(csv_files):

    for file in os.listdir(csv_files):
        if file.endswith('.csv'):
            
            csv_path = os.path.join(csv_files, file)
            print(f'Leyendo archivo {file} para cargar en el Datawarehouse')
            
            conn = engine.raw_connection()
            
            try:
                cur = conn.cursor()
                cur.execute("CREATE TEMP TABLE tmp_hist_temp AS SELECT * FROM hist_temp LIMIT 0")
                print(f'Se ha creado la tabla temporal para la carga del archivo {file}')
                
                
                with open(csv_path, 'r', encoding='utf-8') as f:
                    cur.copy_expert(f"""COPY tmp_hist_temp FROM STDIN WITH CSV HEADER DELIMITER ','""", f)
                    print(f'Se ha copiado la información en la tabla temporal para la carga del archivo {file}')

                cur.execute("""INSERT INTO hist_temp 
                            SELECT * FROM  tmp_hist_temp
                            ON CONFLICT (identificador) DO NOTHING
                            """)
                print(f'Se ha incorporado la información del archivo {file} en la tabla hist_temp del Datawarehouse')
                
                cur.execute("DROP TABLE IF EXISTS tmp_hist_temp")
                
                conn.commit()
                
                print(f'Se han cargado todos los datos del archivo {file} en el Datawarehouse exitosamente')
            
            except Exception as e:
                print(f'Se ha producido un error en la creación del cursor: {e}')
            
            finally:
                cur.close()
                conn.close()
                                
load_files(clean_new_csv_files)            