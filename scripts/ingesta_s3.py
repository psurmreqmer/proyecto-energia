import os
from dotenv import load_dotenv

# Importaciones de modulos propios
from conexion import get_s3_client
from utilidades import crear_bucket, stream_particionado_s3

def ejecutar_pipeline():
    """
    Orquesta la conexion a AWS y la subida masiva de datos particionados a la capa Raw.
    """
    load_dotenv()
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

if __name__ == "__main__":
    ejecutar_pipeline()