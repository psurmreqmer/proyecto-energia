import os
import io
import pandas as pd
from dotenv import load_dotenv

# Importamos los metodos de nuestros otros archivos
from conexion import get_s3_client
from utilidades import crear_bucket

def subir_archivo_crudo(s3_client, bucket_name, ruta_local, s3_key):
    """
    Sube un archivo local directamente a S3 sin modificaciones para formar la capa Raw.
    """
    print(f"Subiendo archivo crudo a S3: {s3_key}")
    try:
        # upload_file es mas eficiente para archivos grandes desde disco
        s3_client.upload_file(ruta_local, bucket_name, s3_key)
        print(f"Archivo crudo subido exitosamente: {s3_key}")
    except Exception as e:
        print(f"Error al subir el archivo crudo {ruta_local}: {e}")

def procesar_y_subir_parquet(s3_client, bucket_name, energy_file, weather_file):
    """
    Limpia, unifica y sube los datos a S3 en formato Parquet particionado (Capa Processed).
    """
    print("Iniciando procesamiento de datos en memoria para la capa procesada...")
    try:
        df_energy = pd.read_csv(energy_file)
        df_weather = pd.read_csv(weather_file)
    except Exception as e:
        print(f"Error al leer los archivos CSV locales: {e}")
        return

    # Limpieza y unificacion
    df_energy['time'] = pd.to_datetime(df_energy['time'], utc=True)
    df_weather['dt_iso'] = pd.to_datetime(df_weather['dt_iso'], utc=True)

    df_energy.drop_duplicates(subset=['time'], keep='first', inplace=True)
    df_weather.drop_duplicates(subset=['dt_iso', 'city_name'], keep='first', inplace=True)

    df_weather_pivot = df_weather.pivot(index='dt_iso', columns='city_name', values=['temp', 'humidity', 'wind_speed'])
    df_weather_pivot.columns = ['_'.join(col).strip() for col in df_weather_pivot.columns.values]
    df_weather_pivot.reset_index(inplace=True)

    df_master = pd.merge(df_energy, df_weather_pivot, left_on='time', right_on='dt_iso', how='inner')
    df_master.drop(columns=['dt_iso'], inplace=True)

    # Variables de particionado
    df_master['year'] = df_master['time'].dt.year
    df_master['month'] = df_master['time'].dt.month
    df_master['day'] = df_master['time'].dt.day

    grupos = df_master.groupby(['year', 'month', 'day'])
    archivos_subidos = 0

    print("Particionando datos y subiendo a la carpeta 'processed/'...")
    for (year, month, day), grupo in grupos:
        grupo_limpio = grupo.drop(columns=['year', 'month', 'day'])
        
        parquet_buffer = io.BytesIO()
        grupo_limpio.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        # Ruta estructurada de la capa procesada
        s3_key = f"processed/year={year}/month={month:02d}/day={day:02d}/daily_data.parquet"
        
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            archivos_subidos += 1
            if archivos_subidos % 100 == 0:
                print(f"  Progreso: {archivos_subidos} dias particionados y subidos...")
        except Exception as e:
            print(f"Error subiendo la particion {s3_key}: {e}")

    print(f"Capa procesada completada: {archivos_subidos} archivos Parquet subidos.")

def orquestar_ingesta(energy_file, weather_file):
    """
    Controlador principal que orquesta todo el flujo de ingesta de datos.
    """
    load_dotenv()
    bucket_name = os.getenv('S3_BUCKET_NAME', 'energia-espana')

    print("Iniciando pipeline de ingesta de datos a S3...")
    
    # 1. Obtener conexion
    s3_client = get_s3_client()
    if not s3_client:
        print("No se pudo establecer la conexion a S3. Abortando.")
        return

    # 2. Asegurar infraestructura
    bucket_listo = crear_bucket(s3_client, bucket_name)
    if not bucket_listo:
        print("Hubo un problema al preparar el bucket. Abortando.")
        return

    # 3. Subir Capa Raw (Crudos)
    print("\n--- PASO 1: CAPA RAW ---")
    subir_archivo_crudo(s3_client, bucket_name, energy_file, "raw/energy/energy_dataset.csv")
    subir_archivo_crudo(s3_client, bucket_name, weather_file, "raw/weather/weather_features.csv")

    # 4. Procesar y subir Capa Processed (Parquet)
    print("\n--- PASO 2: CAPA PROCESADA ---")
    procesar_y_subir_parquet(s3_client, bucket_name, energy_file, weather_file)

    print("\nPipeline de ingesta finalizado con exito.")

# ==========================================
# EJECUCION PRINCIPAL Y GESTION DE RUTAS
# ==========================================
if __name__ == "__main__":
    # 1. Obtener la ruta absoluta del directorio donde se encuentra ESTE script (scripts/)
    directorio_script = os.path.dirname(os.path.abspath(__file__))
    
    # 2. Construir la ruta relativa hacia la carpeta data/
    ruta_energy = os.path.join(directorio_script, "..", "data", "energy_dataset.csv")
    ruta_weather = os.path.join(directorio_script, "..", "data", "weather_features.csv")
    
    # 3. Validar que los archivos existan antes de llamar a AWS
    if not os.path.exists(ruta_energy) or not os.path.exists(ruta_weather):
        print("Error: No se encontraron los archivos CSV.")
        print(f"Ruta comprobada para energia: {ruta_energy}")
        print(f"Ruta comprobada para clima: {ruta_weather}")
    else:
        # 4. Ejecutar orquestador
        orquestar_ingesta(ruta_energy, ruta_weather)