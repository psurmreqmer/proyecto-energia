import os
import boto3
from dotenv import load_dotenv

"""
Carga las variables de entorno y devuelve un cliente de S3.
"""
def get_s3_client():

    load_dotenv()
    
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

# ==========================================
# TEST DE CONEXIÓN
# ==========================================
if __name__ == "__main__":
    print("Probando conexión a AWS S3...")
    cliente = get_s3_client()
    
    if cliente:
        try:
            response = cliente.list_buckets()
            print("¡Conexión exitosa! Buckets disponibles en tu cuenta:")
            for bucket in response['Buckets']:
                print(f"  - {bucket['Name']}")
        except Exception as e:
            print(f"Error al conectar con S3.\nDetalles: {e}")