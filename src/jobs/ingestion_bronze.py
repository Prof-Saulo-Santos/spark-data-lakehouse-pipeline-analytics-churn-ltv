# -------------------------
# Imports do PySpark
# -------------------------

# SparkSession é o ponto de entrada para aplicações Spark
from pyspark.sql import SparkSession

# Tipos de dados e definição explícita de schema
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

# Biblioteca padrão para manipulação de arquivos e caminhos
import os

# Utilitário para criação da SparkSession com configurações padronizadas
from src.lib.spark_utils import get_spark_session

# Logger estruturado para padronizar logs (nível, mensagem, contexto)
from src.lib.logger import StructuredLogger

# Classe responsável por validações de qualidade de dados (Data Contracts)
from src.lib.quality import DataQuality


# Instancia um logger específico para a camada Bronze
logger = StructuredLogger("IngestionBronze")


def ingest_table(
    spark: SparkSession,
    table_name: str,
    source_path: str,
    schema: StructType,
    required_columns: list
):
    """
    Função genérica de ingestão para a camada Bronze.

    Responsabilidades:
    1. Ler dados brutos (Parquet) com schema explícito
    2. Validar contrato mínimo de qualidade
    3. Persistir os dados no formato Delta Lake (camada Bronze)

    Parâmetros:
    - spark: SparkSession ativa
    - table_name: nome lógico da tabela
    - source_path: caminho do arquivo de origem
    - schema: schema esperado dos dados
    - required_columns: lista de colunas obrigatórias (contrato mínimo)
    """
    
    # Log de início da ingestão
    logger.info(
        f"Starting ingestion for {table_name}",
        source=source_path
    )
    
    # -------------------------
    # Validação de existência do arquivo de origem
    # -------------------------
    if not os.path.exists(source_path):
        # Se o arquivo não existir, registra warning e interrompe a ingestão
        logger.warn(
            f"Source file not found: {source_path}. Skipping."
        )
        return

    # -------------------------
    # 1. Leitura dos dados brutos
    # -------------------------
    # Leitura explícita com schema definido para evitar inferência incorreta
    df_raw = (
        spark.read
        .format("parquet")
        .schema(schema)
        .load(source_path)
    )
    
    # -------------------------
    # 2. Validação de Qualidade (Data Contract)
    # -------------------------
    # Instancia o validador de qualidade para a tabela
    validator = DataQuality(table_name)
    
    # Verifica se as colunas obrigatórias estão presentes e válidas
    if not validator.validate_contract(
        df_raw,
        required_columns=required_columns
    ):
        # Interrompe o pipeline se o contrato mínimo falhar
        raise ValueError(
            f"Data Quality Contract failed for {table_name}"
        )

    # -------------------------
    # 3. Escrita na camada Bronze (Delta Lake)
    # -------------------------
    # Define o caminho de destino da tabela Bronze
    dest_path = f"data/bronze/{table_name}"
    
    # Observação:
    # - A camada Bronze normalmente é append-only ou snapshot
    # - Aqui utilizamos overwrite para simplificação do pipeline local (batch didático)
    # - Em produção: Bronze=Append, Silver=Merge, Gold=Overwrite/Merge
    (
        df_raw.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(dest_path)
    )
        
    # Log de sucesso da ingestão
    logger.info(
        f"Successfully ingested {table_name} to {dest_path}"
    )


def main():
    """
    Orquestra a ingestão dos datasets brutos para a camada Bronze.
    
    Tabelas processadas:
    - customers
    - transactions
    """
    
    # Cria a SparkSession
    spark = get_spark_session("IngestionBronze")
    
    # -------------------------
    # Ingestão da tabela Customers
    # -------------------------
    
    # Definição explícita do schema da tabela customers
    cust_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        # Na camada Bronze geralmente mantemos tipos crus (string)
        StructField("signup_date", StringType(), True),
        StructField("region", StringType(), True)
    ])
    
    # Chamada da função genérica de ingestão
    ingest_table(
        spark,
        "customers",
        "data/bronze/customers.parquet",
        cust_schema,
        required_columns=["customer_id"]
    )
    
    # -------------------------
    # Ingestão da tabela Transactions
    # -------------------------
    
    # Definição explícita do schema da tabela transactions
    trans_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        # Mantido como string na Bronze para conversão posterior
        StructField("date", StringType(), True)
    ])
    
    # Chamada da função genérica de ingestão
    ingest_table(
        spark,
        "transactions",
        "data/bronze/transactions.parquet",
        trans_schema,
        required_columns=["transaction_id", "customer_id", "amount"]
    )
    
    # Log de finalização do pipeline Bronze
    logger.info("Bronze Ingestion Complete")
    
    # Encerra a SparkSession
    spark.stop()


# Ponto de entrada do script
if __name__ == "__main__":
    main()