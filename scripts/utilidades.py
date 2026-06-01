import os
import io
import csv
import boto3

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