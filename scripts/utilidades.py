import boto3

"""
Verifica si un bucket existe en la cuenta de AWS y, en caso de no existir,
lo crea utilizando el cliente de S3 que se recibe por parametro.
"""
def crear_bucket(s3_client, bucket_name="energia-espana"):

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
            print(f"No se pudo verificar el bucket por un problema de acceso u otro error.")
            print(f"Detalles: {e}")
            return False