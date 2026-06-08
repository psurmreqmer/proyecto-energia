2.1. Requisitos Previos 

Entorno: Configurar un entorno virtual de Python e instalar las librerías especificadas en el archivo requirements.txt 

Credenciales: Disponer de un archivo .env en la raíz del proyecto con las claves de acceso a AWS necesarias para la ingesta y el procesamiento en la nube. El proyecto incluye una plantilla con los datos necesarios a rellenar llamado  .env.plantilla

2.2. Secuencia de Trabajo 

Orquestación del Data Lake y ETL (pipeline_aws.py):
Este script centralizado ejecuta el flujo completo en la nube:
Ingesta: Carga los archivos crudos (energy_dataset.csv y weather_features.csv) a S3, organizándolos en una estructura particionada.
Catalogación: Lanza el Crawler de AWS Glue para registrar los datos en el catálogo.
Transformación: Ejecuta las sentencias SQL en Amazon Athena para limpiar, consolidar y generar la tabla dataset_unificado en formato Parquet, optimizada para el modelado.
*Nota: este punto puede requerir de bastante tiempo mientras se ejecuta el Script

Desarrollo y Modelado (cuadernos de hitos):
Hito 1
Hito 2
Hito 3
En este punto se recomienda ejecutar el cuaderno proyecto.ipynb, pero en su defecto se pueden ejecutar los cuadernos en orden.

Despliegue e Interfaz (Hugging Face):
Una vez validados los modelos en el Hito 3, se utiliza el script de despliegue en Hugging Face Spaces. Este componente carga el modelo para permitir la inferencia en tiempo real a través de una interfaz gráfica desarrollada en Gradio. 

Enlade a Hugging Face: https://huggingface.co/spaces/iespsurmreqmer/proyecto_energia
