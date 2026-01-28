import boto3
from dotenv import load_dotenv
import os
import faker

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_KEY'),
    aws_session_token=os.getenv('SESSION_TOKEN'),
    region_name=os.getenv('REGION')
)

# Probar la conexión listando los buckets
s3 = session.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)
    

# Crear bucket si no existe
bucket_name = 'gestion-practicas-bucket'

existing_buckets = [b.name for b in s3.buckets.all()]

if bucket_name not in existing_buckets:
    s3.create_bucket(
        Bucket=bucket_name
    )
    print(f'\nBucket {bucket_name} creado.')
else:
    print(f'\nBucket {bucket_name} ya existe.')
    
    
# Crear carpeta dentro del bucket
folder_name = 'gestion/'

bucket = s3.Bucket(bucket_name)
folder_exists = False
for obj in bucket.objects.filter(Prefix=folder_name):
    folder_exists = True
    break

if not folder_exists:
    s3.Object(bucket_name, folder_name).put()
    print(f'\nCarpeta {folder_name} creada en el bucket {bucket_name}.')
else:
    print(f'\nCarpeta {folder_name} ya existe en el bucket {bucket_name}.')
    
# Función para generar datos sintéticos, flag para indicar si se deben generar o no
def generar_datos_y_guardar_en_s3(generar=False, num_registros=100):
    if not generar:
        print("Generación de datos sintéticos desactivada.")
        return
    
    fake = faker.Faker('es_ES')
    
    estudiantes = []
    for _ in range(num_registros):
        id_estudiante = fake.random_int(min=1, max=1000)
        dni = fake.random_int(min=10000000, max=99999999)
        nombre_completo = fake.name()
        fecha_nacimiento = fake.date_of_birth(minimum_age=18, maximum_age=30)
        email = fake.email()
        telefono = fake.phone_number()
        direccion = fake.address().replace('\n', ', ')
        nacionalidad = fake.country()
        id_centro = fake.random_int(min=1, max=50)
        titulacion = fake.word().capitalize()
        curso_academico = f"{fake.random_int(min=2018, max=2023)}-{fake.random_int(min=2019, max=2024)}"
        
        
        registro = (
            id_estudiante,
            dni,
            nombre_completo,
            fecha_nacimiento,
            email,
            telefono,
            direccion,
            nacionalidad,
            id_centro,
            titulacion,
            curso_academico
        )
        estudiantes.append(registro)
    
    # Guardar los datos en un archivo CSV
    
    import csv
    import io
    
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    
    writer.writerow([
        'id_estudiante', 'dni', 'nombre_completo', 'fecha_nacimiento',
        'email', 'telefono', 'direccion', 'nacionalidad', 'id_centro', 'titulacion', 'curso_academico'
    ])
    writer.writerows(estudiantes)
    
    csv_content = buffer.getvalue()
    buffer.close()
    
    # Comprobar el contenido generado
    print("\nContenido del archivo CSV generado:")
    print(csv_content)
        
    # Subir el archivo CSV al bucket S3 en una subcarpeta específica
    s3.Object(bucket_name, f'{folder_name}csv/datos_practicas.csv').put(Body=csv_content)
    print(f'\nArchivo datos_practicas.csv subido a {folder_name}csv/ en el bucket {bucket_name}.')
    
# Llamar a la función para generar datos y guardarlos en S3
generar_datos_y_guardar_en_s3(generar=True, num_registros=100)


# Athena

import time

athena = session.client('athena')

database_name = 'gestion_practicas_db'

output_location = f's3://{bucket_name}/resultados_estudiantes/'

# Crear base de datos
db_execution = athena.start_query_execution(
    QueryString=f'''
    CREATE DATABASE IF NOT EXISTS {database_name}
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que termine la creación de la base de datos
db_result = athena.get_query_execution(QueryExecutionId=db_execution['QueryExecutionId'])
while db_result['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    db_result = athena.get_query_execution(QueryExecutionId=db_execution['QueryExecutionId'])

print(f"Base de datos {database_name} creada/verificada")

table_name = 'estudiantes_practicas'

# Eliminar la tabla si ya existe
drop_execution = athena.start_query_execution(
    QueryString=f'''
    DROP TABLE IF EXISTS {database_name}.{table_name}
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que termine el DROP
drop_result = athena.get_query_execution(QueryExecutionId=drop_execution['QueryExecutionId'])
while drop_result['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    drop_result = athena.get_query_execution(QueryExecutionId=drop_execution['QueryExecutionId'])

print(f"Tabla {table_name} eliminada (si existía)")

# Definir la consulta para crear la tabla
create_table_query = f'''
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
    id_estudiante INT,
    dni INT,
    nombre_completo STRING,
    fecha_nacimiento STRING,
    email STRING,
    telefono STRING,
    direccion STRING,
    nacionalidad STRING,
    id_centro INT,
    titulacion STRING,
    curso_academico STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"',
    'escapeChar' = '\\\\'
)
LOCATION 's3://{bucket_name}/{folder_name}csv/'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'has_encrypted_data'='false'
);
'''

# Ejecutar la consulta para crear la tabla
create_execution = athena.start_query_execution(
    QueryString=create_table_query,
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que termine la creación de la tabla
create_result = athena.get_query_execution(QueryExecutionId=create_execution['QueryExecutionId'])
while create_result['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    create_result = athena.get_query_execution(QueryExecutionId=create_execution['QueryExecutionId'])

print(f"Tabla {table_name} creada exitosamente")

# Consultar los datos para verificar que se han cargado correctamente
query_execution = athena.start_query_execution(
    QueryString=f'''
    SELECT * FROM {database_name}.{table_name} LIMIT 10
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que la consulta termine
result = athena.get_query_execution(QueryExecutionId=query_execution['QueryExecutionId'])
while result['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result = athena.get_query_execution(QueryExecutionId=query_execution['QueryExecutionId'])

# Verificar si la consulta falló
if result['QueryExecution']['Status']['State'] == 'FAILED':
    print(f"Error en la consulta CSV: {result['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
else:
    print(f"Consulta CSV completada exitosamente")

# Obtener los resultados de la consulta
result = athena.get_query_results(QueryExecutionId=query_execution['QueryExecutionId'])

print("\nResultados de la consulta:")

for row in result['ResultSet']['Rows']:
    # Cada fila es una lista de diccionarios {'VarCharValue': valor}
    values = [col.get('VarCharValue', '') for col in row['Data']]
    print(values)


# Eliminar la tabla y la base de datos (opcional)
# athena.start_query_execution(
#     QueryString=f'''
#     DROP TABLE IF EXISTS {database_name}.{table_name}
#     ''',
#     ResultConfiguration={'OutputLocation': output_location}
# )


# Replicar lo mismo pero con formato JSON


def generar_datos_json_y_guardar_en_s3(generar=False, num_registros=100):
    if not generar:
        print("Generación de datos sintéticos en JSON desactivada.")
        return
    
    fake = faker.Faker('es_ES')
    
    estudiantes = []
    for _ in range(num_registros):
        estudiante = {
            "id_estudiante": fake.random_int(min=1, max=1000),
            "dni": fake.random_int(min=10000000, max=99999999),
            "nombre_completo": fake.name(),
            "fecha_nacimiento": str(fake.date_of_birth(minimum_age=18, maximum_age=30)),
            "email": fake.email(),
            "telefono": fake.phone_number(),
            "direccion": fake.address().replace('\n', ', '),
            "nacionalidad": fake.country(),
            "id_centro": fake.random_int(min=1, max=50),
            "titulacion": fake.word().capitalize(),
            "curso_academico": f"{fake.random_int(min=2018, max=2023)}-{fake.random_int(min=2019, max=2024)}"
        }
        estudiantes.append(estudiante)
    
    import json
    import io
    
    buffer = io.StringIO()
    for estudiante in estudiantes:
        buffer.write(json.dumps(estudiante) + '\n')
    
    json_content = buffer.getvalue()
    buffer.close()
    
    # Comprobar el contenido generado
    print("\nContenido del archivo JSON generado:")
    print(json_content)
        
    # Subir el archivo JSON al bucket S3 en una subcarpeta específica
    s3.Object(bucket_name, f'{folder_name}json/datos_practicas.json').put(Body=json_content)
    print(f'\nArchivo datos_practicas.json subido a {folder_name}json/ en el bucket {bucket_name}.')


# Llamar a la función para generar datos JSON y guardarlos en S3
generar_datos_json_y_guardar_en_s3(generar=True, num_registros=100)

# Athena

table_name_json = 'estudiantes_practicas_json'

# Eliminar la tabla si ya existe
drop_execution_json = athena.start_query_execution(
    QueryString=f'''
    DROP TABLE IF EXISTS {database_name}.{table_name_json}
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que termine el DROP
drop_result_json = athena.get_query_execution(QueryExecutionId=drop_execution_json['QueryExecutionId'])
while drop_result_json['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    drop_result_json = athena.get_query_execution(QueryExecutionId=drop_execution_json['QueryExecutionId'])

print(f"Tabla {table_name_json} eliminada (si existía)")

# Definir la consulta para crear la tabla en formato JSON

create_table_query_json = f'''
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name_json} (
    id_estudiante INT,
    dni INT,
    nombre_completo STRING,
    fecha_nacimiento STRING,
    email STRING,
    telefono STRING,
    direccion STRING,
    nacionalidad STRING,
    id_centro INT,
    titulacion STRING,
    curso_academico STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{bucket_name}/{folder_name}json/'
TBLPROPERTIES (
    'has_encrypted_data'='false'
);
''' 

# Ejecutar la consulta para crear la tabla JSON
create_execution_json = athena.start_query_execution(
    QueryString=create_table_query_json,
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que termine la creación de la tabla JSON
create_result_json = athena.get_query_execution(QueryExecutionId=create_execution_json['QueryExecutionId'])
while create_result_json['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    create_result_json = athena.get_query_execution(QueryExecutionId=create_execution_json['QueryExecutionId'])

print(f"Tabla {table_name_json} creada exitosamente")

# Consultar los datos para verificar que se han cargado correctamente
query_execution_json = athena.start_query_execution(
    QueryString=f'''
    SELECT * FROM {database_name}.{table_name_json} LIMIT 10
    ''',
    ResultConfiguration={'OutputLocation': output_location}
) 

# Esperar a que la consulta termine
result_json = athena.get_query_execution(QueryExecutionId=query_execution_json['QueryExecutionId'])
while result_json['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result_json = athena.get_query_execution(QueryExecutionId=query_execution_json['QueryExecutionId'])

# Verificar si la consulta falló
if result_json['QueryExecution']['Status']['State'] == 'FAILED':
    print(f"Error en la consulta JSON: {result_json['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
else:
    print(f"Consulta JSON completada exitosamente")
    
# Obtener los resultados de la consulta JSON
result_json = athena.get_query_results(QueryExecutionId=query_execution_json['QueryExecutionId'])

print("\nResultados de la consulta JSON:")
for row in result_json['ResultSet']['Rows']:
    values = [col.get('VarCharValue', '') for col in row['Data']]
    print(values)