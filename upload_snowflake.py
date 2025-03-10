import os
import glob
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Configura tu conexión a Snowflake (rellena los datos correspondientes)

con_vx = snowflake.connector.connect(
    user='SZULUAGAQ',
    password='Levelupfinance.2025',
    account='VBJUNST-WCB45803',
    role='ACCOUNTADMIN',
    warehouse='SANDBOX_WH',
    database='DIMS',
    schema='PUBLIC'
)

def create_table_from_df(con, table_name, df):
    """
    Crea o reemplaza una tabla en Snowflake usando el esquema del DataFrame.
    Se asume que todas las columnas serán de tipo VARCHAR.
    """
    # Limpia y formatea los nombres de las columnas: quitar espacios, comillas, etc.
    cols = [col.strip().replace('"', '').replace("'", "") for col in df.columns]
    # Actualiza los nombres de columna en el DataFrame a mayúsculas
    df.columns = [col.upper() for col in cols]
    # Construye la definición de columnas (todas VARCHAR)
    col_defs = ", ".join([f"{col} VARCHAR" for col in df.columns])
    create_stmt = f"CREATE OR REPLACE TABLE {table_name} ({col_defs})"
    print("Sentencia SQL para crear la tabla:", create_stmt)
    cur = con.cursor()
    cur.execute(create_stmt)
    cur.close()
    print(f"Tabla {table_name} creada (o reemplazada).")

# Lista para almacenar los resultados de la carga
load_results = []

# Directorio donde se encuentran los archivos CSV (dentro de tu repositorio)
csv_dir = 'dbt_sandbox/seeds/dims'
# Buscar todos los archivos CSV en esa carpeta
csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))

for csv_file in csv_files:
    # Derivar el nombre de la tabla a partir del nombre del archivo (sin extensión) y convertirlo a mayúsculas
    base_name = os.path.splitext(os.path.basename(csv_file))[0]
    table_name = base_name.upper()
    
    print(f"\nCargando el archivo {csv_file} en la tabla {table_name}...")
    try:
        # Leer el archivo CSV (ajusta el delimitador si es necesario)
        df = pd.read_csv(csv_file, delimiter=',')
        # Limpieza de los nombres de columna: quitar espacios, comillas y convertir a mayúsculas
        df.columns = [col.strip().replace('"', '').replace("'", "").upper() for col in df.columns]
        
        # Crear la tabla en Snowflake usando el esquema del DataFrame
        create_table_from_df(con_vx, table_name, df)
        
        # Cargar el DataFrame en la tabla usando write_pandas
        # En esta versión, write_pandas devuelve 4 valores: (success, num_chunks, num_rows, extra)
        success, num_chunks, num_rows, extra = write_pandas(con_vx, df, table_name)
        load_results.append([table_name, success, num_rows])
        print(f"Tabla {table_name} cargada con éxito, filas insertadas: {num_rows}")
    except Exception as e:
        print(f"Error al cargar la tabla {table_name}: {e}")
        load_results.append([table_name, False, 0])

# Mostrar un resumen de la carga
print("\nResumen de la carga:")
for result in load_results:
    print(f"Tabla: {result[0]}, Éxito: {result[1]}, Filas cargadas: {result[2]}")
