from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType, StringType

# 1. Ler os dados bronze
df = spark.sql("SELECT * FROM LabutareLakehouse.`companies-tb`")

# 2. Ajustar os tipos das colunas
df = df.select(
    col("id").cast(IntegerType()),
    col("name").cast(StringType()).alias("nome"),
    col("cnpj").cast(StringType()),
    col("tradeName").cast(StringType()).alias("nome_fantasia")
) 

# 3. cnpj sem os caracterees especiais se precisar felipao
df = df.withColumn("cnpj_numero", regexp_replace(col("cnpj"), "[^0-9]", ""))

# 4. Definir o caminho no S3 nao sei se ta certo esqueci a senha de novo

#bucket_path = "s3://bi-storage-labutare/PRD/Silver/companies-tb"

# Salvar em formato Parquet

#df.write.mode("overwrite").parquet(f"{bucket_path}/companies_tb.parquet")

# Salvar em formato CSV se quizer to deixando so para caso precise alterar um dia
#df.write.mode("overwrite").option("header", "true").csv(f"{bucket_path}/companies_tb.csv")

# Exibir amostra dos dados processados
display(df)
