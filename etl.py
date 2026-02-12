import logging
import os
from azure.storage.blob import BlobServiceClient
import pandas as pd
import pymssql
from io import StringIO


def import_data(conn_string: str, container: str, file_path: str) -> str:
    downloaded_blob = ''
    try:
        # Crear cliente del servicio Blob
        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        container_client = blob_service_client.get_container_client(container)

        # Descargar el blob
        blob_client = container_client.get_blob_client(file_path)
        logging.info(f"Conectado al archivo: {file_path}")

        # Leer el contenido del archivo
        downloaded_blob = blob_client.download_blob().content_as_text()

        return downloaded_blob

    except Exception as e:
        logging.error(f"Error al conectarse al Blob Storage: {e}")
        return downloaded_blob

def transform_data(data: str, HotelName: str) -> pd.DataFrame:

    df = pd.read_csv(filepath_or_buffer=StringIO(data),
                            sep = '|',
                            dtype = {"Estado": "string",
                                "Pais": "string",
                                "Ciudad": "string",
                                "Vendedor": "string",
                                "Categoria": "string",
                                "Tipo": "string"  ,
                                "Descripcion": "string",
                                "Monto": "float"                            
                                },
                            parse_dates = ["Fecha"])


    fecha_filtro = pd.Timestamp("2026-01-27")

    df = df[(df["Estado"] == "Completado") & (df["Fecha"] >= fecha_filtro)]
    df["Monto"] = pd.to_numeric(df["Monto"], errors="coerce").fillna(0)

    df["Hotel"] = HotelName

    df = df[['Hotel', 'Fecha', 'Estado', 'Pais', 'Ciudad', 'Vendedor', 'Categoria', 'Tipo', 'Descripcion', 'Monto']]
    df['FechaCarga'] = pd.Timestamp.now()

    return df


def load_data_to_azure_sql(df: pd.DataFrame, server: str, database: str, username: str, password: str):

    conn = None
    try:
        logging.info("Conectando a Azure SQL Database...")
        conn = pymssql.connect(server = server, user = username, password = password, database = database, port=1433,          # obligatorio para Azure SQL
    login_timeout=10,
    timeout=30)
        logging.info("Conexión exitosa")

        # Ejecución de una consulta
        cursor = conn.cursor()

        data = list(df.itertuples(index=False, name=None))

        # Inserta los datos masivamente
        logging.info("Cargando datos a la tabla ventas...")
        cursor.executemany(
                            """ 
                                INSERT INTO Ventas (Hotel, Fecha, Estado, Pais, Ciudad, Vendedor, Categoria, Tipo, Descripcion, Monto, FechaCarga)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            data
        )
            
        conn.commit()

        logging.info("Carga exitosa de los datos a la tabla Ventas")
    except Exception as e:
        logging.error(f"Error al conectar: {e}")
    finally:
        if conn:
            conn.close()

def move_to_proccesed_folder(conn_string: str, container_source: str, file_path: str, container_sink: str):

    blob_service_client = BlobServiceClient.from_connection_string(conn_string)
    source_blob_client = blob_service_client.get_blob_client(container = container_source, blob = file_path)
    source_blob_url = source_blob_client.url

    sink_blob_client = blob_service_client.get_blob_client(container = container_sink, blob = file_path)

    logging.info(f"Iniciando la copia del blob '{container_source}/{file_path}'... a ... '{container_sink}/{file_path}'")
    sink_blob_client.start_copy_from_url(source_blob_url)

    copy_status = sink_blob_client.get_blob_properties().copy.status

    if copy_status == "success":
        logging.info(f"El archivo se ha movido de manera correcta al folder de procesados.")
        source_blob_client.delete_blob()
    else:
        logging.error(f"La copia del archivo '{container_source}/{file_path}' falló. Estado: {copy_status}")

def run_etl_pipeline(connection_string: str, folder_source: str, folder_sink: str, 
                     server: str, database: str, username: str, password: str, 
                     hotel_name: str):
    """
    Ejecuta el pipeline ETL completo: import, transform, load y move
    """
    file_name = hotel_name + '.csv'
    
    data_text = import_data(connection_string, folder_source, file_name)
    logging.info("paso 1")
    df = transform_data(data_text, hotel_name)
    logging.info("paso 2")
    load_data_to_azure_sql(df, server, database, username, password)
    logging.info("paso 3")
    move_to_proccesed_folder(connection_string, folder_source, file_name, folder_sink)
    logging.info("paso 4")
