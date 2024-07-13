import psycopg2
import apache_beam as beam
from google.cloud import secretmanager
import os
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

class GetNames(beam.DoFn):
    def process(self, element):
        client = secretmanager.SecretManagerServiceClient()
        name = "projects/1030074550193/secrets/secret-motors-word-db/versions/1"
        response = client.access_secret_version(name=name)
        secret_payload = response.payload.data.decode("UTF-8")
        secret_dict = json.loads(secret_payload)

        # Definindo as variáveis de ambiente
        os.environ['DB_USER'] = secret_dict['DB_USER']
        os.environ['DB_PASSWORD'] = secret_dict['DB_PASSWORD']
        os.environ['DB_HOST'] = secret_dict['DB_HOST']
        os.environ['DB_PORT'] = secret_dict['DB_PORT']
        os.environ['DB_DATABASE'] = secret_dict['DB_DATABASE']

        USERNAME = os.getenv('DB_USER')
        PASSWORD = os.getenv('DB_PASSWORD')
        HOST = os.getenv('DB_HOST')
        PORT = os.getenv('DB_PORT')
        DATABASE = os.getenv('DB_DATABASE')

        conn_params = {
            "host": HOST,
            "database": DATABASE,
            "user": USERNAME,
            "password": PASSWORD,
            "port": PORT
        }

        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
            cursor.execute(query)
            tables = cursor.fetchall()
            list_names = [table[0] for table in tables]

            logging.info(f'Nomes das tabelas: {list_names}')
            
            cursor.close()
            conn.close()

            yield list_names
            
        except psycopg2.Error as e:
            logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")
            if cursor:
                cursor.close()
            if conn:
                conn.close()

            yield []
