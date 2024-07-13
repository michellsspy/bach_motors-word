from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import os
import pandas as pd
import pyarrow
from google.cloud import storage
import logging
import sys
from datetime import datetime
from google.cloud import secretmanager
import json
import tempfile

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

# Parâmetros de conexão
# Cria um cliente
client = secretmanager.SecretManagerServiceClient()

# Monta o nome do recurso do secret
name = "projects/1030074550193/secrets/secret-service-account/versions/1"

# Acessa o valor do secret
response = client.access_secret_version(name=name)

# Decode o payload
secret_payload = response.payload.data.decode("UTF-8")

# Se o payload for um JSON, converta para um dicionário
secret_dict = json.loads(secret_payload)

# Criar um arquivo temporário para armazenar as credenciais
with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode='w') as temp_file:
    json.dump(secret_dict, temp_file)
    temp_file_path = temp_file.name

# Definir a variável de ambiente para o caminho do arquivo temporário
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path

now = datetime.now()
formatted_datetime = now.strftime("%Y-%m-%d--%H-%M")

def main(argv=None):
    options = PipelineOptions(
        flags=argv,
        project='motors-word',
        runner='DataflowRunner',
        streaming=False,
        job_name=f'ingest-motors-word-{formatted_datetime}',
        temp_location='gs://motors-word/temp',
        staging_location='gs://motors-word/staging',
        #template_location=f'gs://motors-word/templates/template-novadrive-ingest-{formatted_datetime}',
        autoscaling_algorithm='THROUGHPUT_BASED',
        worker_machine_type='n1-standard-4',
        service_account_key_file='./keys',
        num_workers=1,
        max_num_workers=3,
        number_of_worker_harness_threads=2,
        disk_size_gb=50,
        region='southamerica-east1',
        worker_zone='southamerica-east1-c',
        project_id='motors-word',
        staging_bucket='motors-word',
        save_main_session=False,
        #experiments='use_runner_v2',
        prebuild_sdk_container_engine='cloud_build',
        #docker_registry_push_url='southamerica-east1-docker.pkg.dev/motors-word/ingest-motors-word-v1/motors-word-dev:latest',
        sdk_container_image='southamerica-east1-docker.pkg.dev/motors-word/ingest-motors-word-v1/motors-word-dev:latest',
        sdk_location='container',
        requirements_file='./requirements.txt',
        metabase_file='./metadata.json',
        setup_file='./setup.py',
        service_account_email='sa-motors-word@motors-word.iam.gserviceaccount.com',
        #subnetwork='https://www.googleapis.com/compute/v1/projects/motors-word/regions/southamerica-east1/subnetworks/vpc-novadrive'
    )


    from function.get_names import GetNames
    from function.get_tables import GetTables
    from function.tables_transform_bq import TablesTransformBQ


    with beam.Pipeline(options=options) as p1:
        get_names = (
            p1
            | f'Create get names' >> beam.Create([None])
            | f'Execute Get Names' >> beam.ParDo(GetNames())
        )

        get_tables = (
            get_names
            | f'Execute Get Tables' >> beam.ParDo(GetTables())
        )
        
        table_tranform_bq = (
            get_tables
            | f'Execute Tables Transform BQ' >> beam.ParDo(TablesTransformBQ())
        )


if __name__ == '__main__':
    main()