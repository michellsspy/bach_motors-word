import apache_beam as beam
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager
import json
from pandas_gbq import to_gbq  # Importando a função correta

class TablesTransformBQ(beam.DoFn):
    def process(self, element):
        for table in element:
            print(f"Processando a tabela: {table}")

            # Cria um cliente para Secret Manager dentro do método process
            secret_client = secretmanager.SecretManagerServiceClient()
            name = "projects/1030074550193/secrets/conect-motors-word/versions/1"
            
            # Acessa o valor do secret
            response = secret_client.access_secret_version(name=name)
            secret_payload = response.payload.data.decode("UTF-8")
            secret_dict = json.loads(secret_payload)
            credentials = service_account.Credentials.from_service_account_info(secret_dict)
            
            project_id = 'motors-word'
            dataset_id = 'motors_word_bronze'
            bucket_name = 'motors-word'
            path = f'landing/{table}.parquet'
            gcs_file_path = f'gs://{bucket_name}/{path}'

            try:
                df_parquet = pd.read_parquet(gcs_file_path)
            except Exception as e:
                print(f"Erro ao carregar arquivo {gcs_file_path}: {e}")
                continue

            df_parquet = df_parquet.astype(str)

            table_ref = f'{project_id}.{dataset_id}.{table}'

            # Instancia cliente do BigQuery dentro do método process
            bq_client = bigquery.Client(credentials=credentials, project=project_id)

            try:
                bq_client.get_table(table_ref)
                table_exists = True
            except Exception:
                table_exists = False

            if table_exists:
                try:
                    print("A tabela já existe, truncando")
                    bq_client.query(f'TRUNCATE TABLE {table_ref}').result()
                except Exception as e:
                    print(f"Erro ao truncar a tabela {table_ref}: {e}")
                    continue

            try:
                print("Carregando dados no BigQuery")
                to_gbq(df_parquet, destination_table=f'{dataset_id}.{table}', project_id=project_id, if_exists='replace', credentials=credentials)
            except Exception as e:
                print(f"Erro ao carregar dados no BigQuery para a tabela {table}: {e}")
                continue
