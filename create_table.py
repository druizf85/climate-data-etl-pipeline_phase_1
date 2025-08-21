import pandas as pd
import os
from dotenv import load_dotenv
import src.engine_creation as e
from sqlalchemy import text 

load_dotenv()

user_postgre = os.getenv('USER_POSTGRE')
password_postgre = os.getenv('PASS_POSTGRE')
host_postgre = os.getenv('HOST_POSTGRE')
port_postgre = os.getenv('PORT_POSTGRE')
database_postgre = os.getenv('DB_NAME_POSTGRE')

engine = e.engine_postgre(user_postgre, password_postgre, host_postgre, port_postgre, database_postgre)

query_create = """

CREATE TABLE IF NOT EXISTS hist_temp (

identificador VARCHAR(100) PRIMARY KEY,
batch VARCHAR(100) NOT NULL,
codigoestacion VARCHAR(100) NOT NULL,
codigosensor VARCHAR(100) NOT NULL,
fechaobservacion TIMESTAMP NOT NULL,
mesobservacion INT NOT NULL,
valorobservado FLOAT NOT NULL,
nombreestacion VARCHAR(100) NOT NULL,
departamento VARCHAR(100) NOT NULL,
municipio VARCHAR(100) NOT NULL,
zonahidrografica VARCHAR(100) NOT NULL,
latitud FLOAT NOT NULL,
longitud FLOAT NOT NULL,
descripcionsensor VARCHAR(100) NOT NULL,
unidadmedida VARCHAR(100) NOT NULL
)

"""

try:
    with engine.begin() as conn:
        conn.execute(text(query_create))
        print('Tabla hist_temp creada con éxito')
except Exception as e:
    print(f'Se presenta un error en la creación de la tabla hist_temp: {e}')