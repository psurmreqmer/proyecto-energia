import os
import boto3
from dotenv import load_dotenv

# Importacion de los metodos de logica en la nube actualizados
from funciones_procesado import (
    preparar_catalogo_glue,
    desplegar_y_ejecutar_crawler,
    ejecutar_consulta_athena
)

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
    load_dotenv()
    
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
    procesar_data_lake()