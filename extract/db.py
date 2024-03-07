import mysql.connector
import json
from datetime import datetime
from mysql.connector import Error  # Asegúrate de importar Error
import pandas as pd
import os
#import from .env variables
from dotenv import load_dotenv
load_dotenv(".env")

GCS_PASSWORD = os.getenv("GCS_PASSWORD")
GCS_USER_ROOT=os.getenv("GCS_USER_ROOT")
GCS_DATABASE=os.getenv("GCS_DATABASE")
GCS_HOST=os.getenv("GCS_HOST")
# Reemplaza 'your_username', 'your_password', 'your_host', 'your_database' con tus propios detalles de la base de datos.

# Función para cargar los datos en la base de datos

# Función para recuperar datos de la base de datos
def fetch_data(query):
    conn = None  # Inicializar conn como None
    try:
        # Establecer la conexión
        conn = mysql.connector.connect(
            user=GCS_USER_ROOT,
            password=GCS_PASSWORD,
            host=GCS_HOST,
            database=GCS_DATABASE
        )

        # Ejecutar consulta y devolver resultado como DataFrame
        df = pd.read_sql_query(query, conn)
        return df

    except Error as err:
        print(f"Error: '{err}'")
    finally:
        if conn and conn.is_connected():  # Verificar si conn no es None y está conectado antes de cerrar
            conn.close()

# Example usage
if __name__ == "__main__":
    query = "SELECT * FROM `slowpoke-v1`.dolar;"
    print(fetch_data(query))