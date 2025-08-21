from sqlalchemy import create_engine
import sqlite3
import os

# PostgreSQL
def engine_postgre(user, password, host, port, database):
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    return engine

def engine_postgre_for_docker(user, password, port, database):
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@host.docker.internal:{port}/{database}")
    return engine

# SQL Server - Autenticación Windows (Trusted Connection)
def engine_sql_server_trusted(server, database):
    engine = create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
    return engine

# SQL Server - Autenticación SQL (usuario y contraseña)
def engine_sql_server(user, password, server, database):
    engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")
    return engine

# MySQL
def engine_my_sql(user, password, host, port, database):
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
    return engine

# SQLite
def engine_sqlite(folder_path, db_name):
    db_path = os.path.join(folder_path, db_name + '.db')
    engine= sqlite3.connect(db_path)
    return engine