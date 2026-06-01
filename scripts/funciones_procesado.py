import time

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