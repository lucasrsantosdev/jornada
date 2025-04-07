import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType, StringType, DateType, DecimalType

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

# Iniciando a sessão Spark
spark = SparkSession.builder.appName("BI_Transformations").getOrCreate()

# Exemplo de uso da classe S3
s3 = S3()
df_enterprises = s3.ler_csv_bucket_bruto("enterprises")

# Convertendo o dataframe pandas para Spark
df_spark = spark.createDataFrame(df_enterprises)

# Renomear colunas e ajustar tipos no Spark
df_spark = df_spark.select(
    col("id").cast(IntegerType()).alias("id"),
    col("name").cast(StringType()).alias("nome"),
    col("commercialName").cast(StringType()).alias("nome_comercial"),
    col("cnpj").cast(StringType()).alias("cnpj"),
    col("type").cast(IntegerType()).alias("tipo"),
    col("adress").cast(StringType()).alias("endereco"),
    col("creationDate").cast(DateType()).alias("data_criacao"),
    col("modificationDate").cast(DateType()).alias("data_modificacao"),
    col("createdBy").cast(StringType()).alias("criado_por"),
    col("modifiedBy").cast(StringType()).alias("modificado_por"),
    col("companyId").cast(IntegerType()).alias("empresa_id"),
    col("companyName").cast(StringType()).alias("empresa_nome"),
    col("costDatabaseId").cast(StringType()).alias("banco_custos_id"),
    col("costDatabaseDescription").cast(StringType()).alias("banco_custos_descricao"),
    col("buildingTypeId").cast(IntegerType()).alias("tipo_construcao_id"),
    col("buildingTypeDescription").cast(StringType()).alias("tipo_construcao_descricao"),
    col("enterpriseObservation").cast(StringType()).alias("observacao_empr")
)

# Criar a coluna cnpj_numerico sem caracteres especiais
df_spark = df_spark.withColumn("cnpj_numerico", regexp_replace(col("cnpj"), "[^0-9]", ""))

# Caminho no S3 para Silver
bucket_path_silver = "s3://bi-storage-labutare/PRD/Silver/enterprises"

# Salvar em formato Parquet
df_spark.write.mode("overwrite").parquet(f"{bucket_path_silver}/enterprises.parquet")

# Salvar em formato CSV (caso precise)
# df_spark.write.mode("overwrite").option("header", "true").csv(f"{bucket_path_silver}/enterprises.csv")

# Exibir amostra dos dados processados
df_spark.show()
