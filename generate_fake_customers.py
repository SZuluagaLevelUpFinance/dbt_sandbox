import os
import csv
import random
from faker import Faker

fake = Faker()

# Cantidad de clientes a generar
num_customers = 10000

# Rutas de archivos (con prefijo "dbt_sandbox/")
output_file = 'dbt_sandbox/seeds/dims/dim_customers_v2.csv'
cust_type_file = 'dbt_sandbox/seeds/dims/dim_cust_type.csv'
country_file = 'dbt_sandbox/seeds/dims/dim_country.csv'

# Asegurarse de que la carpeta de salida exista
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# --- Cargar los country_ids desde el archivo dim_country.csv ---
country_ids = []
with open(country_file, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Limpiar las claves en caso de espacios adicionales
        row = {k.strip(): v for k, v in row.items()}
        try:
            cid = int(row['country_id'])
        except ValueError:
            cid = row['country_id']
        country_ids.append(cid)

# --- Cargar los cust_type_ids desde el archivo dim_cust_type.csv usando la primera columna ---
cust_type_ids = []
with open(cust_type_file, mode='r', encoding='utf-8-sig') as file:
    # Se elimina el parámetro delimiter='\t' para usar el delimitador por defecto (coma)
    reader = csv.reader(file)
    header = next(reader)  # Omitir el encabezado
    for row in reader:
        if not row:
            continue  # Saltar filas vacías, si las hay
        try:
            ctid = int(row[0])
        except ValueError:
            ctid = row[0]
        cust_type_ids.append(ctid)

# --- Generar el archivo de clientes usando Faker para el nombre y los IDs obtenidos ---
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Escribir el encabezado
    writer.writerow(["customer_id", "customer_name", "country_id", "cust_type_id"])
    
    for i in range(1, num_customers + 1):
        customer_name = fake.name()
        chosen_country = random.choice(country_ids)
        chosen_cust_type = random.choice(cust_type_ids)
        writer.writerow([i, customer_name, chosen_country, chosen_cust_type])

print(f"Archivo generado: {output_file} con {num_customers} clientes.")
