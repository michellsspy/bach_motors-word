import psycopg2
import apache_beam as beam
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import secretmanager
import os
import json

class GetTables(beam.DoFn):
    def process(self, element):
        list_names = []
        for table in element:
            # Recupera a lista de names para passar na próxima PCollection
            list_names.append(table)

            # Acessa o valor do secret
            client = secretmanager.SecretManagerServiceClient()
            name = "projects/1030074550193/secrets/secret-motors-word-db/versions/1"
            response = client.access_secret_version(name=name)
            secret_payload = response.payload.data.decode("UTF-8")
            secret_dict = json.loads(secret_payload)

            # Pega as credenciais via variável de ambiente
            USERNAME = secret_dict['DB_USER']
            PASSWORD = secret_dict['DB_PASSWORD']
            HOST = secret_dict['DB_HOST']
            PORT = secret_dict['DB_PORT']
            DATABASE = secret_dict['DB_DATABASE']

            conn_params = {
                "host": HOST,
                "database": DATABASE,
                "user": USERNAME,
                "password": PASSWORD,
                "port": PORT
            }

            try:
                # Conecta ao banco de dados
                conn = psycopg2.connect(**conn_params)
                cursor = conn.cursor()

                query = f"select * from {table}"
                cursor.execute(query)

                col_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                df_new = pd.DataFrame(rows, columns=col_names)
                
                cursor.close()
                conn.close()    

                df_new = df_new.astype(str)            

                # Nome do bucket e caminho do arquivo
                bucket_name = 'motors-word'
                path = f'landing/{table}.parquet'

                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(path)

                if blob.exists():
                    # Lê o arquivo Parquet existente do GCS para um DataFrame
                    gcs_file_path = f'gs://{bucket_name}/{path}'
                    df_old = pd.read_parquet(gcs_file_path)
                    df_old = df_old.astype(str)

                    # Obtém o nome da primeira coluna
                    merge_column = df_new.columns[0]

                    # Faz o merge dos DataFrames utilizando a primeira coluna
                    df_combined = pd.merge(df_old, df_new, on=merge_column, how='outer', suffixes=('_old', ''))

                    # Preenche os valores NaN nos dados combinados
                    for column in df_new.columns:
                        old_column = f'{column}_old'
                        if old_column in df_combined.columns:
                            df_combined[column] = df_combined[column].fillna(df_combined[old_column])
                            df_combined.drop(columns=[old_column], inplace=True)
                else:
                    df_combined = df_new.astype(str)

                # Salva o DataFrame combinado como Parquet
                df_combined = df_combined.astype(str)
                blob.upload_from_string(df_combined.to_parquet(index=False), content_type='application/octet-stream')

                logging.info(f'{table} {"=" * (80 - len(table))} {df_combined.shape}')

            except psycopg2.Error as e:
                logging.error(f"Erro encontrado durante a conexão: {e}")
        
        yield list_names
