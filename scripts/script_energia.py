import os
import boto3
from dotenv import load_dotenv
import time
import csv
import io

load_dotenv()

def get_s3_client():
    """Carga las variables de entorno y devuelve un cliente de S3."""
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
            region_name=os.getenv('AWS_REGION', 'us-east-1') 
        )
        return s3_client
    except Exception as e:
        print(f"Error al configurar el cliente S3: {e}")
        return None
    
def crear_bucket(s3_client, bucket_name="energia-espana"):
    """
    Verifica si un bucket existe en la cuenta de AWS y, en caso de no existir,
    lo crea utilizando el cliente de S3 que se recibe por parametro.
    """
    if s3_client is None:
        print("Error: El cliente S3 proporcionado no es valido.")
        return False

    try:
        # Intentamos verificar si el bucket ya existe y si tenemos acceso
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"El bucket '{bucket_name}' ya existe y esta listo para usarse.")
        return True
        
    except Exception as e:
        # Extraemos el error buscando dentro de la respuesta de boto3
        # hasattr comprueba si el error tiene el atributo 'response'
        if hasattr(e, 'response') and e.response.get('Error', {}).get('Code') == '404':
            print(f"El bucket '{bucket_name}' no existe. Creandolo ahora...")
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' creado con exito.")
                return True
            except Exception as ex:
                print(f"Error critico al intentar crear el bucket '{bucket_name}': {ex}")
                return False
        else:
            # Captura otros errores
            print("No se pudo verificar el bucket por un problema de acceso u otro error.")
            print(f"Detalles: {e}")
            return False

def extraer_fecha_csv(fila, cabecera, columna_fecha):
    """
    Extrae ano, mes y dia de una cadena de fecha basandose en el indice de la cabecera.
    """
    try:
        indice = cabecera.index(columna_fecha)
        fecha_texto = fila[indice]
        partes_fecha = fecha_texto.split(" ")[0].split("-")
        
        ano = partes_fecha[0]
        mes = partes_fecha[1]
        dia = partes_fecha[2]
        return ano, mes, dia
    except (ValueError, IndexError):
        return None

def stream_particionado_s3(s3_client, bucket_name, ruta_local, carpeta_s3, columna_fecha):
    """
    Lee un archivo CSV local, agrupa registros por fecha en memoria
    y los sube estructurados en carpetas (raw/carpeta_s3/year=.../month=.../day=...).
    """
    if not os.path.exists(ruta_local):
        print(f"Error: El archivo local no existe en {ruta_local}")
        return

    print(f"Iniciando particionamiento de: {ruta_local}")
    
    with open(ruta_local, mode='r', encoding='utf-8') as archivo:
        lector_csv = csv.reader(archivo)
        cabecera = next(lector_csv)
        
        datos_por_dia = {}
        
        for fila in lector_csv:
            componentes = extraer_fecha_csv(fila, cabecera, columna_fecha)
            if not componentes:
                continue
                
            ano, mes, dia = componentes
            clave_fecha = (ano, mes, dia)
            
            if clave_fecha not in datos_por_dia:
                datos_por_dia[clave_fecha] = []
                
            datos_por_dia[clave_fecha].append(fila)
            
        contador_archivos = 0
        for (ano, mes, dia), filas_dia in datos_por_dia.items():
            buffer_memoria = io.StringIO()
            escritor_memoria = csv.writer(buffer_memoria)
            
            escritor_memoria.writerow(cabecera)
            escritor_memoria.writerows(filas_dia)
            
            nombre_archivo_s3 = f"raw/{carpeta_s3}/year={ano}/month={mes}/day={dia}/data.csv"
            
            try:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=nombre_archivo_s3,
                    Body=buffer_memoria.getvalue().encode('utf-8')
                )
                contador_archivos += 1
                if contador_archivos % 500 == 0:
                    print(f"  Progreso [{carpeta_s3}]: {contador_archivos} particiones subidas.")
            except Exception as e:
                print(f"Error al subir la particion {nombre_archivo_s3}: {e}")
                
    print(f"Finalizado: Se han creado {contador_archivos} archivos diarios en raw/{carpeta_s3}/")

def ejecutar_pipeline():
    """
    Orquesta la conexion a AWS y la subida masiva de datos particionados a la capa Raw.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME", "energia-espana")
    
    # Obtencion del cliente a traves de tu metodo personalizado
    s3_client = get_s3_client()
    if not s3_client:
        print("No se pudo establecer la conexion a S3. Abortando.")
        return
    
    # Validacion de infraestructura utilizando tu funcion
    if not crear_bucket(s3_client, bucket_name):
        print("Error critico: No se pudo verificar ni crear el bucket S3.")
        return

    # Calculo de las rutas relativas hacia los datos
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_energia = os.path.join(directorio_actual, "..", "data", "energy_dataset.csv")
    ruta_clima = os.path.join(directorio_actual, "..", "data", "weather_features.csv")

    print("Comenzando el volcado de datos crudos al Data Lake...")
    
    # Procesamiento del dataset de energia
    stream_particionado_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        ruta_local=ruta_energia,
        carpeta_s3="energia",
        columna_fecha="time"
    )
    
    # Procesamiento del dataset de meteorologia
    stream_particionado_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        ruta_local=ruta_clima,
        carpeta_s3="meteorologia",
        columna_fecha="dt_iso"
    )

    print("Proceso de ingesta finalizado. Capa Raw estructurada correctamente.")

def preparar_catalogo_glue(glue_client, db_name):
    """
    Crea la base de datos virtual en el catalogo de AWS Glue si no existe.
    """
    try:
        glue_client.get_database(Name=db_name)
        print(f"Base de datos virtual '{db_name}' lista.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Creando base de datos virtual '{db_name}'...")
        glue_client.create_database(
            DatabaseInput={"Name": db_name, "Description": "Catalogo de datos de energia y clima"}
        )

def desplegar_y_ejecutar_crawler(glue_client, crawler_name, role_arn, db_name, bucket_name):
    """
    Configura y lanza un Crawler para que mapee los archivos CSV de la capa Raw.
    Espera activamente a que el escaneo finalice.
    """
    target_path = f"s3://{bucket_name}/raw/"
    
    try:
        glue_client.get_crawler(Name=crawler_name)
        print(f"El crawler '{crawler_name}' ya esta configurado.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Creando Crawler '{crawler_name}' apuntando a {target_path}...")
        glue_client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": target_path}]}
        )

    print("Iniciando escaneo de archivos (esto puede tardar 1-2 minutos)...")
    glue_client.start_crawler(Name=crawler_name)

    while True:
        respuesta = glue_client.get_crawler(Name=crawler_name)
        estado = respuesta['Crawler']['State']
        if estado == 'READY':
            print("El Crawler ha finalizado con exito. Tablas creadas en el catalogo.")
            break
        print("  ...Crawler trabajando...")
        time.sleep(15)

def ejecutar_consulta_athena(athena_client, query, db_name, output_location):
    """
    Envia una instruccion SQL a Athena y espera a que termine de procesarse.
    """
    print("Enviando orden a Amazon Athena...")
    respuesta = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": db_name},
        ResultConfiguration={"OutputLocation": output_location}
    )
    
    execution_id = respuesta['QueryExecutionId']
    
    while True:
        info = athena_client.get_query_execution(QueryExecutionId=execution_id)
        estado = info['QueryExecution']['Status']['State']
        
        if estado == 'SUCCEEDED':
            print("Consulta ejecutada con exito en Athena.")
            break
        elif estado in ['FAILED', 'CANCELLED']:
            razon = info['QueryExecution']['Status'].get('StateChangeReason', 'Motivo desconocido')
            print(f"Error en Athena: {razon}")
            break
            
        print("  ...Athena procesando datos...")
        time.sleep(5)

def vaciar_carpeta_s3(s3_client, bucket, prefijo):
    """
    Elimina fisicamente todos los archivos antiguos de una ruta en S3 
    para dejar espacio limpio para el nuevo procesamiento de Athena.
    """
    print(f"Limpiando datos antiguos en s3://{bucket}/{prefijo}...")
    paginator = s3_client.get_paginator('list_objects_v2')
    for pagina in paginator.paginate(Bucket=bucket, Prefix=prefijo):
        if 'Contents' in pagina:
            objetos_a_borrar = [{'Key': obj['Key']} for obj in pagina['Contents']]
            # Borrado masivo
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objetos_a_borrar})

def procesar_data_lake():
    """
    Ejecuta el flujo ETL completo: actualizacion de metadatos (Crawler) 
    y transformacion SQL Serverless (Athena CTAS) usando un rol existente.
    """
    
    # Variables de entorno
    bucket_name = os.getenv("S3_BUCKET_NAME", "energia-espana")
    db_name = os.getenv("GLUE_DATABASE_NAME", "db_energia_espana")
    athena_out = os.getenv("ATHENA_RESULTS_LOCATION")
    role_arn = os.getenv("AWS_GLUE_ROLE_ARN")
    
    # Credenciales explicitas para Boto3
    aws_access = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_token = os.getenv("AWS_SESSION_TOKEN")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    
    if not role_arn:
        print("Error: AWS_GLUE_ROLE_ARN no esta definido en tu archivo .env.")
        return

    # Inicializacion de clientes nativos Boto3
    glue_client = boto3.client(
        "glue", aws_access_key_id=aws_access, aws_secret_access_key=aws_secret, 
        aws_session_token=aws_token, region_name=aws_region
    )
    
    athena_client = boto3.client(
        "athena", aws_access_key_id=aws_access, aws_secret_access_key=aws_secret, 
        aws_session_token=aws_token, region_name=aws_region
    )
    
    # Nuevo cliente S3 para limpiar carpetas
    s3_client = boto3.client(
        "s3", aws_access_key_id=aws_access, aws_secret_access_key=aws_secret, 
        aws_session_token=aws_token, region_name=aws_region
    )

    print("\n--- PASO 1: PREPARACION DE METADATOS (AWS GLUE) ---")
    preparar_catalogo_glue(glue_client, db_name)
    crawler_name = "ExploradorRawEnergia"
    desplegar_y_ejecutar_crawler(glue_client, crawler_name, role_arn, db_name, bucket_name)

    print("\n--- PASO 2: PROCESAMIENTO Y TRANSFORMACION (AMAZON ATHENA) ---")
    
    # 2.1 Limpiar la carpeta fisica y la tabla logica
    prefijo_salida = "processed/dataset_unificado/"
    ruta_salida_parquet = f"s3://{bucket_name}/{prefijo_salida}"
    
    vaciar_carpeta_s3(s3_client, bucket_name, prefijo_salida)
    
    query_limpieza = f"DROP TABLE IF EXISTS {db_name}.dataset_unificado"
    ejecutar_consulta_athena(athena_client, query_limpieza, db_name, athena_out)

    # 2.2 Ejecutar la consulta final con el TRIM y la deduplicacion
    query_transformacion = f"""
    CREATE TABLE {db_name}.dataset_unificado
    WITH (
        format = 'PARQUET',
        external_location = '{ruta_salida_parquet}'
    ) AS
    WITH ClimaPivot AS (
        SELECT 
            dt_iso,
            MAX(CASE WHEN TRIM(city_name) = 'Madrid' THEN temp END) AS temp_madrid,
            MAX(CASE WHEN TRIM(city_name) = 'Barcelona' THEN temp END) AS temp_barcelona,
            MAX(CASE WHEN TRIM(city_name) = 'Valencia' THEN temp END) AS temp_valencia,
            MAX(CASE WHEN TRIM(city_name) = 'Seville' THEN temp END) AS temp_seville,
            MAX(CASE WHEN TRIM(city_name) = 'Bilbao' THEN temp END) AS temp_bilbao
        FROM meteorologia
        GROUP BY dt_iso
    ),
    EnergiaUnica AS (
        SELECT DISTINCT * FROM energia
    )
    SELECT 
        e.*, 
        c.temp_madrid, 
        c.temp_barcelona, 
        c.temp_valencia, 
        c.temp_seville, 
        c.temp_bilbao
    FROM EnergiaUnica e
    JOIN ClimaPivot c ON e.time = c.dt_iso;
    """
    
    print("Generando la capa Processed en formato Parquet...")
    ejecutar_consulta_athena(athena_client, query_transformacion, db_name, athena_out)
    
    print(f"\nProceso completado. Tu dataset unificado esta listo en: {ruta_salida_parquet}")


if __name__ == "__main__":
    ejecutar_pipeline()



    procesar_data_lake()