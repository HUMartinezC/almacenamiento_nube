import boto3
from dotenv import load_dotenv
import os
import faker
import json

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_KEY'),
    aws_session_token=os.getenv('SESSION_TOKEN'),
    region_name=os.getenv('REGION')
)


def create_bucket_with_region(s3_resource, bucket_name, region):
    if not region or region == 'us-east-1':
        s3_resource.create_bucket(Bucket=bucket_name)
        return
    s3_resource.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': region}
    )

# Crear carpeta local para descargas
download_folder = './descargas'
if not os.path.exists(download_folder):
    os.makedirs(download_folder)
    print(f'Carpeta {download_folder} creada para descargas.')

# Probar la conexión listando los buckets
s3 = session.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)
    

# Crear bucket si no existe
bucket_name = 'gestion-practicas-bucket'

existing_buckets = [b.name for b in s3.buckets.all()]

if bucket_name not in existing_buckets:
    create_bucket_with_region(s3, bucket_name, os.getenv('REGION'))
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
        
    # Subir el archivo CSV al bucket S3 en una subcarpeta específica
    s3.Object(bucket_name, f'{folder_name}csv/datos_practicas.csv').put(Body=csv_content)
    print(f'\nArchivo datos_practicas.csv subido a {folder_name}csv/ en el bucket {bucket_name}.')
    
    # Descargar el archivo para verificar que se ha subido correctamente
    local_file = os.path.join(download_folder, 'datos_practicas.csv')
    bucket.download_file(f'{folder_name}csv/datos_practicas.csv', local_file)
    print(f"Archivo descargado para verificación: {local_file}")
    
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
        
    # Subir el archivo JSON al bucket S3 en una subcarpeta específica
    s3.Object(bucket_name, f'{folder_name}json/datos_practicas.json').put(Body=json_content)
    print(f'\nArchivo datos_practicas.json subido a {folder_name}json/ en el bucket {bucket_name}.')
    
    # Descargar el archivo para verificar que se ha subido correctamente
    local_file = os.path.join(download_folder, 'datos_practicas.json')
    bucket.download_file(f'{folder_name}json/datos_practicas.json', local_file)
    print(f"Archivo descargado para verificación: {local_file}")


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
    
 # Descargar objeto en la carpeta JSON para verificar que el archivo se ha subido correctamente
 
json_objects = list(bucket.objects.filter(Prefix=f'{folder_name}json/'))
for obj in json_objects:
    local_file = os.path.join(download_folder, obj.key.split('/')[-1])
    bucket.download_file(obj.key, local_file)
    print(f"Descargado: {local_file}")
    
# Eliminar la tabla y la base de datos (opcional)
# athena.start_query_execution(
#     QueryString=f'''
#     DROP TABLE IF EXISTS {database_name}.{table_name_json}
#     ''',
#     ResultConfiguration={'OutputLocation': output_location}
# )


# Crear S3 Estándar - Acceso poco frecuente, crear un cubo y añadir un objeto y obtener le objeto 

# Crear bucket con clase de almacenamiento estándar - acceso poco frecuente
bucket_name_ia = 'gestion-practicas-poco-frecuente'
existing_buckets = [b.name for b in s3.buckets.all()]
if bucket_name_ia not in existing_buckets:
    create_bucket_with_region(s3, bucket_name_ia, os.getenv('REGION'))
    print(f'\nBucket {bucket_name_ia} creado con clase de almacenamiento IA.')
else:
    print(f'\nBucket {bucket_name_ia} ya existe.')
    
json_content = '''
{
    "id_estudiante": 1,
    "dni": 12345678,
    "nombre_completo": "Juan Pérez",
    "fecha_nacimiento": "1995-05-15",
    "email": "juan.perez@example.com"
}
'''
    
# Subir un objeto al bucket con clase de almacenamiento IA
s3.Object(bucket_name_ia, 'ejemplo/datos_practicas_ia.json').put(Body=json_content, StorageClass='STANDARD_IA')
print(f'\nArchivo datos_practicas_ia.json subido a ejemplo/ en el bucket {bucket_name_ia} con clase de almacenamiento IA.')

# Crear S3 Intelligent-Tiering, crear un cubo y añadir un objeto y obtener le objeto 
bucket_name_it = 'gestion-practicas-intelligent-tiering'
if bucket_name_it not in existing_buckets:
    create_bucket_with_region(s3, bucket_name_it, os.getenv('REGION'))
    print(f'\nBucket {bucket_name_it} creado con clase de almacenamiento Intelligent-Tiering.')
else:
    print(f'\nBucket {bucket_name_it} ya existe.')
    
# Subir un objeto al bucket con clase de almacenamiento Intelligent-Tiering
s3.Object(bucket_name_it, 'ejemplo/datos_practicas_it.json').put(Body=json_content, StorageClass='INTELLIGENT_TIERING')
print(f'\nArchivo datos_practicas_it.json subido a ejemplo/ en el bucket {bucket_name_it} con clase de almacenamiento Intelligent-Tiering.')

# Crear S3 Glacier, crear un cubo y añadir un objeto y obtener le objeto 
bucket_name_glacier = 'gestion-practicas-glacier'
if bucket_name_glacier not in existing_buckets:
    create_bucket_with_region(s3, bucket_name_glacier, os.getenv('REGION'))
    print(f'\nBucket {bucket_name_glacier} creado con clase de almacenamiento Glacier.')
else:
    print(f'\nBucket {bucket_name_glacier} ya existe.')
    
# Subir un objeto al bucket con clase de almacenamiento Glacier
s3.Object(bucket_name_glacier, 'ejemplo/datos_practicas_glacier.json').put(Body=json_content, StorageClass='GLACIER')
print(f'\nArchivo datos_practicas_glacier.json subido a ejemplo/ en el bucket {bucket_name_glacier} con clase de almacenamiento Glacier.')


# Crear S3 Glacier Deep Archive, crear un cubo y añadir un objeto y obtener le objeto
bucket_name_deep_archive = 'gestion-practicas-deep-archive'
if bucket_name_deep_archive not in existing_buckets:
    create_bucket_with_region(s3, bucket_name_deep_archive, os.getenv('REGION'))
    print(f'\nBucket {bucket_name_deep_archive} creado con clase de almacenamiento Glacier Deep Archive.')
else:
    print(f'\nBucket {bucket_name_deep_archive} ya existe.')
    
# Subir un objeto al bucket con clase de almacenamiento Glacier Deep Archive
s3.Object(bucket_name_deep_archive, 'ejemplo/datos_practicas_deep_archive.json').put(Body=json_content, StorageClass='DEEP_ARCHIVE')
print(f'\nArchivo datos_practicas_deep_archive.json subido a ejemplo/ en el bucket {bucket_name_deep_archive} con clase de almacenamiento Glacier Deep Archive.')

# Hablitar el control de versiones de S3 mediante comandos y mostrar un ejemplo de un objeto modificado y mostrar dos versiones 
versioning_bucket_name = 'gestion-practicas-versioning'
if versioning_bucket_name not in existing_buckets:
    create_bucket_with_region(s3, versioning_bucket_name, os.getenv('REGION'))
    print(f'\nBucket {versioning_bucket_name} creado para control de versiones.')
else:
    print(f'\nBucket {versioning_bucket_name} ya existe.')
    
# Habilitar el control de versiones en el bucket
versioning = s3.BucketVersioning(versioning_bucket_name)
versioning.enable()
print(f'Control de versiones habilitado en el bucket {versioning_bucket_name}.')

# Subir un objeto al bucket con control de versiones
s3.Object(versioning_bucket_name, 'ejemplo/datos_practicas_versioning.json').put(Body=json_content)
print(f'\nArchivo datos_practicas_versioning.json subido a ejemplo/ en el bucket {versioning_bucket_name} con control de versiones.')

# Modificar el objeto para crear una nueva versión
json_content_modificado = '''
{
    "id_estudiante": 1,
    "dni": 12345678,
    "nombre_completo": "Juan Pérez Modificado",
    "fecha_nacimiento": "1995-05-15",
    "email": "juan.perez.modificado@example.com"
}
'''
s3.Object(versioning_bucket_name, 'ejemplo/datos_practicas_versioning.json').put(Body=json_content_modificado)
print(f'\nArchivo datos_practicas_versioning.json modificado para crear una nueva versión en el bucket {versioning_bucket_name}.')

# Listar las versiones del objeto
bucket = s3.Bucket(versioning_bucket_name)
print(f'\nVersiones del objeto datos_practicas_versioning.json en el bucket {versioning_bucket_name}:')
for obj_version in bucket.object_versions.filter(Prefix='ejemplo/datos_practicas_versioning.json'):
    print(f'Versión ID: {obj_version.id}, Última modificación: {obj_version.last_modified}, Tamaño: {obj_version.size} bytes')
    
    
# Realizar 3 consultas diferentes sobre el objeto .csv del S3 usando AWS Athena

# Consulta 1: Contar el número de estudiantes
query_execution_count = athena.start_query_execution(
    QueryString=f'''
    SELECT COUNT(*) AS total_estudiantes FROM {database_name}.{table_name}
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)
# Esperar a que la consulta termine
result_count = athena.get_query_execution(QueryExecutionId=query_execution_count['QueryExecutionId'])
while result_count['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result_count = athena.get_query_execution(QueryExecutionId=query_execution_count['QueryExecutionId'])
    if result_count['QueryExecution']['Status']['State'] == 'FAILED':
        print(f"Error en la consulta de conteo: {result_count['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
        
    else:
        print(f"Consulta de conteo completada exitosamente")
        
# Consulta 2: Listar los estudiantes con una titulación específica
titulacion_especifica = 'Ingeniería'
query_execution_titulacion = athena.start_query_execution(
    QueryString=f'''
    SELECT nombre_completo, email FROM {database_name}.{table_name} WHERE titulacion = '{titulacion_especifica}'
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)
# Esperar a que la consulta termine
result_titulacion = athena.get_query_execution(QueryExecutionId=query_execution_titulacion['QueryExecutionId'])
while result_titulacion['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result_titulacion = athena.get_query_execution(QueryExecutionId=query_execution_titulacion['QueryExecutionId'])
    if result_titulacion['QueryExecution']['Status']['State'] == 'FAILED':
        print(f"Error en la consulta de titulación: {result_titulacion['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
        
    else:
        print(f"Consulta de titulación completada exitosamente")
        
# Consulta 3: Listar los estudiantes nacidos después de una fecha específica
fecha_especifica = '2000-01-01'
query_execution_fecha = athena.start_query_execution(
    QueryString=f'''
    SELECT nombre_completo, fecha_nacimiento FROM {database_name}.{table_name} WHERE fecha_nacimiento > '{fecha_especifica}'
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)
# Esperar a que la consulta termine
result_fecha = athena.get_query_execution(QueryExecutionId=query_execution_fecha['QueryExecutionId'])
while result_fecha['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result_fecha = athena.get_query_execution(QueryExecutionId=query_execution_fecha['QueryExecutionId'])
    if result_fecha['QueryExecution']['Status']['State'] == 'FAILED':
        print(f"Error en la consulta de fecha: {result_fecha['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
        
    else:
        print(f"Consulta de fecha completada exitosamente")
        
# Descargar los resultados de las 3 consultas para verificación
athena_results_bucket = s3.Bucket(bucket_name)
for obj in athena_results_bucket.objects.filter(Prefix='resultados_estudiantes/'):
    local_file = os.path.join(download_folder, obj.key.split('/')[-1])
    athena_results_bucket.download_file(obj.key, local_file)
    print(f"Archivo de resultados descargado para verificación: {local_file}")


# Crear otra base de datos pero usando una fuente de datos de tipo JSON y aplicale 3 querys.

db_name = 'gestion_practicas_json_db'

# Crear base de datos JSON
db_execution_json = athena.start_query_execution(
    QueryString=f'''
    CREATE DATABASE IF NOT EXISTS {db_name}
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que termine la creación de la base de datos JSON
db_result_json = athena.get_query_execution(QueryExecutionId=db_execution_json['QueryExecutionId'])
while db_result_json['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    db_result_json = athena.get_query_execution(QueryExecutionId=db_execution_json['QueryExecutionId'])


# Leer el archivo fuente_json.json
with open('fuente_json.json', 'r', encoding='utf-8') as file:
    datos_json = json.load(file)

# Convertir a formato JSONL (una línea por documento)
jsonl_content = '\n'.join([json.dumps(registro) for registro in datos_json])

# Subir el archivo JSONL al bucket S3
s3.Object(bucket_name, f'{folder_name}fuentes_json/fuente_json.jsonl').put(Body=jsonl_content)
print(f'\nArchivo fuente_json.jsonl subido a {folder_name}fuentes_json/ en el bucket {bucket_name}.')

# Crear tabla externa en Athena desde el archivo JSON
table_name_fuentes = 'estudiantes_fuentes_json'

create_table_query_fuentes = f'''
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name_fuentes} (
    id INT,
    titulo STRING,
    autor STRING,
    anio_publicacion INT,
    genero STRING,
    disponible BOOLEAN
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{bucket_name}/{folder_name}fuentes_json/'
TBLPROPERTIES ('has_encrypted_data'='false');
'''

create_execution_fuentes = athena.start_query_execution(
    QueryString=create_table_query_fuentes,
    ResultConfiguration={'OutputLocation': output_location}
)

# Esperar a que termine la creación de la tabla
create_result_fuentes = athena.get_query_execution(QueryExecutionId=create_execution_fuentes['QueryExecutionId'])
while create_result_fuentes['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    create_result_fuentes = athena.get_query_execution(QueryExecutionId=create_execution_fuentes['QueryExecutionId'])

print(f"Tabla {table_name_fuentes} creada exitosamente en {db_name}")

# Realizar 3 consultas diferentes sobre la tabla creada desde el JSON usando AWS Athena

# Primera consulta: Contar el número de libros
query_execution_count_json = athena.start_query_execution(
    QueryString=f'''
    SELECT COUNT(*) AS total_estudiantes FROM {db_name}.{table_name_fuentes}
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)
# Esperar a que la consulta termine
result_count_json = athena.get_query_execution(QueryExecutionId=query_execution_count_json['QueryExecutionId'])
while result_count_json['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result_count_json = athena.get_query_execution(QueryExecutionId=query_execution_count_json['QueryExecutionId'])
    if result_count_json['QueryExecution']['Status']['State'] == 'FAILED':
        print(f"Error en la consulta de conteo JSON: {result_count_json['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
        
    else:
        print(f"Consulta de conteo JSON completada exitosamente")
        

# Segunda consulta: Consultar los autores y buscar Miguel de Cervantes
query_execution_autores = athena.start_query_execution(
    QueryString=f'''
    SELECT autor FROM {db_name}.{table_name_fuentes} WHERE autor LIKE '%Miguel de Cervantes%'
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)
# Esperar a que la consulta termine
result_autores = athena.get_query_execution(QueryExecutionId=query_execution_autores['QueryExecutionId'])
while result_autores['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result_autores = athena.get_query_execution(QueryExecutionId=query_execution_autores['QueryExecutionId'])
    if result_autores['QueryExecution']['Status']['State'] == 'FAILED':
        print(f"Error en la consulta de autores: {result_autores['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
        
    else:
        print(f"Consulta de autores completada exitosamente")


# Tercera consulta: Consultar los libros disponibles
query_execution_disponibles = athena.start_query_execution(
    QueryString=f'''
    SELECT titulo FROM {db_name}.{table_name_fuentes} WHERE disponible = true
    ''',
    ResultConfiguration={'OutputLocation': output_location}
)
# Esperar a que la consulta termine
result_disponibles = athena.get_query_execution(QueryExecutionId=query_execution_disponibles['QueryExecutionId'])
while result_disponibles['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
    time.sleep(1)
    result_disponibles = athena.get_query_execution(QueryExecutionId=query_execution_disponibles['QueryExecutionId'])
    if result_disponibles['QueryExecution']['Status']['State'] == 'FAILED':
        print(f"Error en la consulta de libros disponibles: {result_disponibles['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')}")
        
    else:
        print(f"Consulta de libros disponibles completada exitosamente")
        


# Eliminar todos los buckets creados (opcional)
# for bucket_name in [bucket_name, bucket_name_ia, bucket_name_it, bucket_name_glacier, bucket_name_deep_archive, versioning_bucket_name]:
#     bucket = s3.Bucket(bucket_name)
#     bucket.object_versions.all().delete()
#     bucket.objects.all().delete()
#     bucket.delete()
    
# Inicializamos Glue para eliminar las bases de datos y tablas creadas (opcional)
# glue = session.client('glue')

# Eliminar tablas y bases de datos en Glue
# for db in [database_name, db_name]:
#     try:
#         tables = glue.get_tables(DatabaseName=db)['TableList']
#         for table in tables:
#             glue.delete_table(DatabaseName=db, Name=table['Name'])
#             print(f'Tabla {table["Name"]} eliminada de la base de datos {db}.')
#         glue.delete_database(Name=db)
#         print(f'Base de datos {db} eliminada.')
#     except glue.exceptions.EntityNotFoundException:
#         print(f'Base de datos {db} no encontrada, no se eliminó.')