from datetime import datetime as dt
from dotenv import load_dotenv
import pandas as pd
import datetime
import requests
import boto3
import os
import io

# CLASS S3
class S3:
    def __init__(self):
        # Carregar as credenciais do arquivo .env
        dotenv_path = os.path.join('.env')
        load_dotenv(dotenv_path)

        # Credenciais AWS
        self.aws_access_key_id = os.getenv("aws_access_key_id")
        self.aws_secret_access_key = os.getenv("aws_secret_access_key")
        self.silver_access_key_id = os.getenv("silver_access_key_id")
        self.silver_secret_access_key = os.getenv("silver_secret_access_key")

    # Método para ler arquivo CSV do S3
    def ler_csv_bucket_bruto(self, nm_arq):
        bucket = "bi-storage-labutare"
        key = f"PRD/Bronze/{nm_arq}/{nm_arq}.csv"

        # Criação do cliente S3 com boto3
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name='us-east-1'
        )

        # Baixando o arquivo do S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_string = obj['Body'].read().decode('utf-8')

        # Usando pandas para ler o CSV a partir da string
        df = pd.read_csv(io.StringIO(csv_string), sep=',')
        return df

    # Método para sobrescrever dados no bucket Bronze (CSV e Parquet)
    def overwrite_dataframes_bucket_bruto(self, df, nm_arq):
        # CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        bucket = "bi-storage-labutare"
        key = f"PRD/Bronze/{nm_arq}/{nm_arq}.csv"

        s3 = boto3.client(
            's3', 
            aws_access_key_id=self.aws_access_key_id, 
            aws_secret_access_key=self.aws_secret_access_key
        )
        
        response = s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

        # PARQUET
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        key_parquet = f"PRD/Bronze/{nm_arq}/{nm_arq}.parquet"
        response = s3.put_object(Bucket=bucket, Key=key_parquet, Body=parquet_buffer.getvalue())

    # Método para salvar os dados na Silver (CSV e Parquet)
    def salvar_na_silver(self, df, nm_arq):
        bucket = "bi-storage-labutare"
        key_csv = f"PRD/Silver/{nm_arq}/{nm_arq}.csv"
        key_parquet = f"PRD/Silver/{nm_arq}/{nm_arq}.parquet"

        s3 = boto3.client(
            's3',
            aws_access_key_id=self.silver_access_key_id,
            aws_secret_access_key=self.silver_secret_access_key,
            region_name='us-east-1'
        )

        # Salvar CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key_csv, Body=csv_buffer.getvalue())

        # Salvar Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key_parquet, Body=parquet_buffer.getvalue())
