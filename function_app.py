import os
import logging
import datetime
from typing import Union
from urllib.parse import quote
import pysftp
from ftplib import FTP_TLS,FTP
import ssl
import subprocess  # Importante: para ejecutar comandos de consola
from ssl import SSLSocket
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import tempfile
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential, AzureCliCredential
from azure.mgmt.sql import SqlManagementClient
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas, BlobClient 
import pyodbc 
from dotenv import load_dotenv
import urllib.parse
import base64


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
load_dotenv()


@app.route(route="restore_file")
def restore_file(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # --- Obtener parámetros de la solicitud ---
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                "Formato de solicitud incorrecto.  Se espera un JSON con 'blob_full_path', 'storage_container_name' y, opcionalmente, 'blob_path_prefix'.",
                status_code=400
            )

        blob_full_path = req_body.get('blob_full_path') 
        storage_container_name = req_body.get('storage_container_name') 
        blob_path_prefix = req_body.get('blob_path_prefix', '') 
       
        if not blob_full_path or not storage_container_name:
            return func.HttpResponse(
                "Faltan parámetros obligatorios. Se requiere 'blob_full_path' y 'storage_container_name'.",
                status_code=400
            )
        # ---  Configuración (Variables de Entorno) ---

        storage_account_name = os.environ["STORAGE_ACCOUNT_NAME"]
        storage_container_name = os.environ["STORAGE_CONTAINER_NAME"]
        blob_name = os.environ["BLOB_NAME"]
        managed_instance_fqdn = os.environ["MANAGED_INSTANCE_FQDN"] 
        target_database_name = os.environ["TARGET_DATABASE_NAME"]
        sql_admin_login = os.environ["SQL_ADMIN_LOGIN"] 
        sql_admin_password = os.environ["SQL_ADMIN_PASSWORD"]
        subscription_id = os.environ['SUBSCRIPTION_ID']
        resource_group_name = os.environ['RESOURCE_GROUP_NAME']
        managed_instance_name = os.environ['MANAGED_INSTANCE_NAME']
        location = os.environ["LOCATION"]
        storage_connection_string = os.environ["STORAGE_CONNECTION_STRING"]

           # Blob de la Ruta Completa ---

        blob_name = os.path.basename(blob_full_path)

         # --- Cadena de Conexión y SAS  ---
        parts = storage_connection_string.split(";")
        account_name = ""
        account_key = ""
        for part in parts:
            if part.startswith("AccountName="):
                account_name = part.split("=")[1]
            elif part.startswith("AccountKey="):
                account_key = part.split("=")[1]

        if not account_name or not account_key:
            return func.HttpResponse("Error: La cadena de conexión no tiene el formato correcto (AccountName y AccountKey).", status_code=500)

        account_key = account_key.strip()
        try:
            account_key_bytes = base64.b64decode(account_key + '=' * (-len(account_key) % 4))
            account_key = base64.b64encode(account_key_bytes).decode('utf-8')
        except Exception:
            logging.warning("La clave de la cuenta de almacenamiento podría no estar en Base64. Continuando...")
            pass

        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        blob_client = blob_service_client.get_blob_client(container=storage_container_name, blob=blob_full_path) # Se usa la ruta completa para obtener el blob

        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=storage_container_name,
            blob_name= blob_full_path,  # Usar la ruta completa para el SAS
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        )

        blob_url_with_sas = blob_client.url + "?" + sas_token


        
        # --- Restaurar la base de datos con T-SQL (usando pyodbc) ---

      
        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"  
            f"Server={managed_instance_fqdn};"
            f"Database=master;"  # 'master'
            f"Uid={sql_admin_login};"
            f"Pwd={sql_admin_password};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        
        try:
            # AUTOCOMMIT=TRUE
            with pyodbc.connect(conn_str, autocommit=True) as conn: 
                with conn.cursor() as cursor:
                    
                    blob_url_base = f"https://{storage_account_name}.blob.core.windows.net/{storage_container_name}/{blob_full_path}"

                    # **ELIMINAR LA BASE DE DATOS EXISTENTE**
                    drop_sql = f"DROP DATABASE IF EXISTS [{target_database_name}]"
                    cursor.execute(drop_sql)
                  
                    # Restaurar la base de datos
                    restore_sql = f"""
                    RESTORE DATABASE [{target_database_name}]
                    FROM URL = '{blob_url_base}'
                    """
                    cursor.execute(restore_sql)

            logging.info(f"Restauración de '{target_database_name}' iniciada.")
            return func.HttpResponse(
                f"Restauración de la base de datos '{target_database_name}' iniciada.",
                status_code=200
            )

        except pyodbc.Error as ex:
            logging.exception(f"Error de pyodbc: {ex}")
            return func.HttpResponse(f"Error de pyodbc: {ex}", status_code=500)

    except Exception as e:
        logging.exception(f"Error general: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)


@app.route(route="ingest_file")
def ingest_file(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function entry point."""

    logging.info('Python HTTP trigger function processed a request.')
    connection_string = os.environ.get('AzureWebJobsStorage') 
    partition_date = datetime.date.today()
    formatted_date = partition_date.strftime("%d-%m-%Y")
    input_path = os.path.join('/data/files/input/', f"partition_date={formatted_date}")

    try:
        req_body = req.get_json() 
    except ValueError:
        pass  

    nombre_archivo = req_body.get('origen') if req_body else None

    if not nombre_archivo:
        return func.HttpResponse(
             "El parámetro 'nombre_archivo' es requerido en el cuerpo de la solicitud (formato JSON).",
             status_code=400  # Bad Request
        )
    
    if not nombre_archivo.strip(): 
        return func.HttpResponse(
             "El parámetro 'nombre_archivo' no puede estar vacío.",
             status_code=400  # Bad Request
        )

    logging.info(f"Nombre del archivo a procesar: {nombre_archivo}")

    if nombre_archivo == 'SCS':

        sftp_host = os.environ.get('sftp_host_scs')
        sftp_username = os.environ.get('sftp_username_scs')
        sftp_password = os.environ.get('sftp_password_scs')
        sftp_path = os.environ.get('sftp_path_scs')
        container_name = 'scs'
        sftp_port = 22
        use_sftp = True

    elif nombre_archivo == 'PCMI':

        sftp_host = os.environ.get('sftp_host_pcmi')
        sftp_username = os.environ.get('sftp_username_pcmi')
        sftp_password = os.environ.get('sftp_password_pcmi')
        sftp_path = os.environ.get('sftp_path_pcmi')
        container_name = 'pcmi'
        sftp_port = 21
        use_sftp = False

    if not all([sftp_host, sftp_username, sftp_password, sftp_path, connection_string, container_name]):
      logging.error("Missing required environment variables.")
      return "Missing required environment variables", 400

    message, status_code = download_and_upload_file(sftp_host, sftp_username, sftp_password, sftp_port, sftp_path, connection_string, container_name, input_path, use_sftp)

    return func.HttpResponse(
             message,
             status_code=status_code
        )





class ReusedSslSocket(SSLSocket):
    def unwrap(self):
        pass

class FTP_TLS_IgnoreHost(FTP_TLS):
    def makepasv(self):
        _, port = super().makepasv()
        return self.host, port

    """Explicit FTPS, with shared TLS session"""
    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                      server_hostname=self.host,
                      session=self.sock.session)   
            conn.__class__ = ReusedSslSocket 
        return conn, size


class Connection:
    """Unified connection handler for SFTP and FTP."""

    def __init__(self, host: str, port: int, username: str, password: str, use_sftp: bool):
        self.host = host
        self.port = port
        self.username = username.replace('\n', '').replace('\r', '')  
        self.password = password.replace('\n', '').replace('\r', '')
        self.use_sftp = use_sftp
        self._connection = None

    def __enter__(self):
        try:
            if self.use_sftp:
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                self._connection = pysftp.Connection(host=self.host, username=self.username, password=self.password, port=self.port, cnopts=cnopts)
                logging.info("Conexión SFTP establecida")
            else:
                self._connection = FTP_TLS_IgnoreHost(self.host, timeout=5)  
                self._connection.ssl_version = ssl.PROTOCOL_TLS
                self._connection.auth()
                self._connection.login(self.username, self.password)
                self._connection.prot_p()
                self._connection.set_pasv(True)  
                logging.info("Conexión FTPS establecida")
            return self._connection
        except Exception as e:
            self._close()
            raise RuntimeError(f"Error al establecer la conexión: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

    def _close(self):
        if self._connection:
            try:
                if self.use_sftp:
                    self._connection.close()
                else:
                    self._connection.quit()
            except:
                if self.use_sftp:
                    pass 
                else:
                    self._connection.close()
            finally:
                self._connection = None


def download_and_upload_file(host, username, password, port, remote_path, connection_string, container_name, file_path, use_sftp=True):


    try:
        with Connection(host, port, username, password, use_sftp) as conn:
            if use_sftp:
                # SFTP
                    try:
                        remote_files = conn.listdir(remote_path)
                        for remote_file in remote_files:
                            with tempfile.TemporaryDirectory() as tmpdir:  
                                local_filepath = os.path.join(tmpdir, remote_file)  
                                remote_filepath = os.path.join(remote_path, remote_file)

                                logging.info(f"Downloading file (SFTP): {remote_filepath}")
                                
                                conn.get(remote_filepath, local_filepath) 

                                _upload_to_azure(connection_string, container_name, local_filepath, remote_file)

                    except Exception as e:
                        logging.exception(f"Error SFTP: {e}")
                        raise

            else:
                # FTP
                try:
                    conn.cwd(remote_path)
                    remote_files = conn.nlst()

                    for remote_file in remote_files:
                        with tempfile.TemporaryDirectory() as tmpdir:  
                            local_filepath = os.path.join(tmpdir, remote_file) 
                            remote_filepath = os.path.join(remote_path, remote_file)

                            logging.info(f"Downloading file (FTP): {remote_filepath}")
                            with open(local_filepath, "wb") as local_file:
                                conn.retrbinary(f"RETR {remote_file}", local_file.write)

                            _upload_to_azure(connection_string, container_name, local_filepath, remote_file)
                        
                except Exception as e:
                    logging.exception(f"An error occurred during FTP: {e}")
                    raise

        logging.info(f"Files processed successfully.")
        return f"Files processed successfully", 200

    except Exception as e:
        logging.exception(f"A general error occurred: {e}")
        return f"An error occurred: {e}", 500


def _upload_to_azure(connection_string, container_name, local_filepath, filename):

    partition_date = datetime.date.today().strftime("%d-%m-%Y")

    folder_path = os.environ.get('storage_path')
    blob_name = os.path.join(folder_path, f"partition_date={partition_date}", filename)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    blobs_to_delete = [blob for blob in container_client.list_blobs(name_starts_with=os.path.join(folder_path, f"partition_date={partition_date}"))]
    if blobs_to_delete:
        container_client.delete_blobs(*blobs_to_delete)
        logging.info(f"Deleted existing blobs with partition date {partition_date}")

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(local_filepath, "rb") as data:
        blob_client.upload_blob(data)

    logging.info(f"File uploaded to Azure Blob Storage: {container_name}/{blob_name}")