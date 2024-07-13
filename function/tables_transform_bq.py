import apache_beam as beam
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager
import json

class TablesTransformBQ(beam.DoFn):
    def process(self, element):
        for table in element:
            
            # Parâmetros de conexão
            # Cria um cliente
            client = secretmanager.SecretManagerServiceClient()

            # Monta o nome do recurso do secret
            name = "projects/1030074550193/secrets/conect-motors-word/versions/1"

            # Acessa o valor do secret
            response = client.access_secret_version(name=name)

            # Decode o payload
            secret_payload = response.payload.data.decode("UTF-8")

            # Se o payload for um JSON, converta para um dicionário
            secret_dict = json.loads(secret_payload)

            # Cria as credenciais diretamente do dicionário do secret
            credentials = service_account.Credentials.from_service_account_info(secret_dict)

            # Configurações do BigQuery
            project_id = 'motors-word'
            dataset_id = 'motors_word_bronze'
            
            # Carregar dados do GCS
            bucket_name = 'motors-word'
            path = f'landing/{table}.parquet'
            gcs_file_path = f'gs://{bucket_name}/{path}'
            
            # Carregar dados do arquivo Parquet
            try:
                df_parquet = pd.read_parquet(gcs_file_path)
            except Exception as e:
                print(f"Erro ao carregar arquivo {gcs_file_path}: {e}")
                continue

            # Converter todas as colunas para string
            df_parquet = df_parquet.astype(str)

            print("Iniciando conexão com BigQuery")
            # Instancia cliente do BigQuery
            client = bigquery.Client(credentials=credentials, project=project_id)

            # Referência da tabela no BigQuery
            table_ref = f'{project_id}.{dataset_id}.{table}'

            print("Verificando se a tabela existe")
            # Verificar se a tabela já existe
            try:
                client.get_table(table_ref)
                table_exists = True
            except Exception:
                table_exists = False

            # Se a tabela existe, truncar a tabela
            if table_exists:
                print("A tabela já existe, truncando")
                client.query(f'TRUNCATE TABLE {table_ref}').result()

            # Carregar dados no BigQuery
            print("Carregando dados no BigQuery")
            df_parquet.to_gbq(destination_table=f'{dataset_id}.{table}', project_id=project_id, if_exists='replace', credentials=credentials)
