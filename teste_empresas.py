from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType, StringType, DateType
from datetime import datetime as dt, timedelta
from pyspark.sql import functions as F
from bs4 import BeautifulSoup
from app_s3 import S3 # LIB AWS
from datetime import datetime as dt
from dotenv import load_dotenv # QUANDO NOS TIVERMOS O COFRE DE SEMHAS TROCAREMOS ESSA LIB PELA LIB DE CONSUMO DE CHAVES DA AWS
import pandas as pd
import requests
import boto3
import io
import os

import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv

class S3:
    def __init__(self):
        # Carregar as credenciais do arquivo .env
        dotenv_path = os.path.join('.env')
        load_dotenv(dotenv_path)

        # Credenciais AWS
        self.pbi_access_key_id = os.getenv("pbi_access_key_id")
        self.pbi_secret_access_key = os.getenv("pbi_secret_access_key")

    # Método para ler arquivo CSV do S3
    def ler_csv_bucket_bruto(self, nm_arq):
        bucket = "bi-storage-labutare"
        key = f"PRD/Bronze/{nm_arq}/{nm_arq}.csv"

        # Criação do cliente S3 com boto3
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.pbi_access_key_id,
            aws_secret_access_key=self.pbi_secret_access_key,
            region_name='us-east-1'
        )

        # Baixando o arquivo do S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_string = obj['Body'].read().decode('utf-8')

        # Usando pandas para ler o CSV a partir da string
        df = pd.read_csv(io.StringIO(csv_string), sep=',')
        return df


# Exemplo de uso da classe S3
s3 = S3()
df_enterprises = s3.ler_csv_bucket_bruto("enterprises")

# Realizando as transformações no Pandas
df_enterprises['id'] = df_enterprises['id'].astype(int)
df_enterprises['nome'] = df_enterprises['name'].astype(str)
df_enterprises['nome_comercial'] = df_enterprises['commercialName'].astype(str)
df_enterprises['cnpj'] = df_enterprises['cnpj'].astype(str)
df_enterprises['tipo'] = df_enterprises['type'].astype(int)
df_enterprises['endereco'] = df_enterprises['adress'].astype(str)
df_enterprises['data_criacao'] = pd.to_datetime(df_enterprises['creationDate'])
df_enterprises['data_modificacao'] = pd.to_datetime(df_enterprises['modificationDate'])
df_enterprises['criado_por'] = df_enterprises['createdBy'].astype(str)
df_enterprises['modificado_por'] = df_enterprises['modifiedBy'].astype(str)
df_enterprises['empresa_id'] = df_enterprises['companyId'].astype(int)
df_enterprises['empresa_nome'] = df_enterprises['companyName'].astype(str)
df_enterprises['banco_custos_id'] = df_enterprises['costDatabaseId'].astype(str)
df_enterprises['banco_custos_descricao'] = df_enterprises['costDatabaseDescription'].astype(str)
df_enterprises['tipo_construcao_id'] = df_enterprises['buildingTypeId'].astype(int)
df_enterprises['tipo_construcao_descricao'] = df_enterprises['buildingTypeDescription'].astype(str)
df_enterprises['observacao_empr'] = df_enterprises['enterpriseObservation'].astype(str)

# Criar a coluna cnpj_numerico sem caracteres especiais
df_enterprises['cnpj_numerico'] = df_enterprises['cnpj'].str.replace(r'\D', '', regex=True)

# Exibir o conteúdo do DataFrame
#print(df_enterprises.head())

def overwrite_dataframes_bucket_Silver(self, df_enterprises, nm_arq):
    # CSV
    csv_buffer = io.StringIO()
    df_enterprises.to_csv(csv_buffer, index=False)

    bucket = "bi-storage-labutare"
    key = f"PRD/Silver/{nm_arq}/{nm_arq}.csv"

    s3 = boto3.client(
        's3', 
        aws_access_key_id=self.aws_access_key_id, 
        aws_secret_access_key=self.aws_secret_access_key
    )

    # Envia o arquivo para o S3
    response = s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    
    # Imprime a resposta do S3 para depuração
    print(response)
                        
