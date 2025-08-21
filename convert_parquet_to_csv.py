import duckdb
import os

folder_path = os.getcwd()

parquet_files = os.path.join(folder_path, 'data', 'clean_data', 'hist_data')
parquet_files_new = os.path.join(folder_path, 'data', 'clean_data', 'new_data')

def convert_parquet_csv(parquet_files_variable):
    
    csv_folder = os.path.join(parquet_files_variable, 'csv')
    os.makedirs(csv_folder, exist_ok=True)  # Crear si no existe
    
    for file in os.listdir(parquet_files_variable):
        if file.endswith('.parquet'):
            parquet_file = os.path.join(parquet_files_variable, file)
            filename = os.path.splitext(os.path.basename(file))[0] + '.csv'
            csv_file = os.path.join(csv_folder, filename)
            
            try:
                duckdb.sql(f"""COPY (SELECT * FROM read_parquet('{parquet_file}')) TO '{csv_file}' (HEADER, DELIMITER ',');""")
            except Exception as e:
                print(f'no se logra ejecutar el comando por un error: {e}')
            print(f'archivo {file} actualizado con éxito en {csv_file}')


filename = '2025-08-10_clean.parquet'
parquet_file = os.path.join(folder_path, 'data', 'clean_data', 'new_data', filename)

def convert_parquet_csv_directly(parquet_files, parquet_file):
    
    csv_folder = os.path.join(parquet_files, 'csv')
    os.makedirs(csv_folder, exist_ok=True)  # Crear si no existe
    
    filename_csv = os.path.splitext(os.path.basename(filename))[0] + '.csv'
    csv_file = os.path.join(csv_folder, filename_csv)
    
    try:
        duckdb.sql(f"""COPY (SELECT * FROM read_parquet('{parquet_file}')) TO '{csv_file}' (HEADER, DELIMITER ',');""")
    except Exception as e:
        print(f'no se logra ejecutar el comando por un error: {e}')
    print(f'archivo {filename} actualizado con éxito en {csv_file}')

convert_parquet_csv_directly(parquet_files_new, parquet_file)