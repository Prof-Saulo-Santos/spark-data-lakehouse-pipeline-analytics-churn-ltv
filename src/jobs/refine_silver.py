# Importa a classe principal do Spark para criação da SparkSession
from pyspark.sql import SparkSession

# Importa funções auxiliares do Spark SQL para manipulação de colunas e datas
from pyspark.sql.functions import col, to_date

# Função utilitária que cria uma SparkSession já configurada (Delta Lake, modo local, etc.)
from src.lib.spark_utils import get_spark_session

# Logger estruturado em JSON para padronizar logs do projeto
from src.lib.logger import StructuredLogger


# Instancia um logger específico para esta etapa do pipeline (Silver)
logger = StructuredLogger("RefineSilver")


def refine_customers(spark: SparkSession):
    """
    Refina a tabela de clientes da camada Bronze para a camada Silver.
    
    Responsabilidades:
    - Ler dados brutos (Delta Bronze)
    - Remover duplicidades
    - Converter tipos de dados
    - Persistir dados limpos como Delta Lake (Silver)
    """
    
    # Caminho de origem (Bronze) e destino (Silver)
    source_path = "data/bronze/customers"
    dest_path = "data/silver/customers"
    
    # Log de início do processamento
    logger.info("Refining customers", source=source_path)
    
    # Leitura da tabela Delta da camada Bronze
    df = spark.read.format("delta").load(source_path)
    
    # -------------------------
    # Deduplicação
    # -------------------------
    # Conta registros antes da deduplicação (ação Spark)
    # ⚠️ Performance Warning: count() é custoso em Big Data.
    # Em produção real, prefira métricas aproximadas ou Delta History.
    initial_count = df.count()
    
    # Remove registros duplicados com base na chave de negócio customer_id
    df_dedup = df.dropDuplicates(["customer_id"])
    
    # Conta registros após a deduplicação
    final_count = df_dedup.count()
    
    # Caso existam duplicatas, registra no log a quantidade removida
    if initial_count != final_count:
        logger.info(f"Removed {initial_count - final_count} duplicates from customers")
        
    # -------------------------
    # Transformações (Bronze -> Silver)
    # -------------------------
    # Converte a coluna signup_date de string para DateType
    # Assume-se formato padrão yyyy-MM-dd
    # Caso o formato fosse inválido, o problema deveria ser tratado na validação
    df_silver = df_dedup.withColumn(
        "signup_date",
        to_date(col("signup_date"))
    )
    
    # -------------------------
    # Escrita na camada Silver
    # -------------------------
    # Salva os dados refinados como tabela Delta
    # overwrite: reprocessamento idempotente
    # overwriteSchema: permite evolução de schema
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(dest_path)
        
    # Log de sucesso
    logger.info(f"Saved customers Silver at {dest_path}")


def refine_transactions(spark: SparkSession):
    """
    Refina a tabela de transações da camada Bronze para a camada Silver.
    
    Responsabilidades:
    - Ler dados brutos
    - Remover duplicidades
    - Renomear e tipar colunas
    - Persistir dados confiáveis para análises
    """
    
    # Caminho de origem (Bronze) e destino (Silver)
    source_path = "data/bronze/transactions"
    dest_path = "data/silver/transactions"
    
    # Log de início do processamento
    logger.info("Refining transactions", source=source_path)
    
    # Leitura da tabela Delta da camada Bronze
    df = spark.read.format("delta").load(source_path)
    
    # -------------------------
    # Deduplicação
    # -------------------------
    # Remove duplicidades com base no identificador único da transação
    df_dedup = df.dropDuplicates(["transaction_id"])
    
    # -------------------------
    # Transformações
    # -------------------------
    # Renomeia a coluna 'date' para 'transaction_date'
    # Evita conflito com palavra reservada SQL e melhora semântica
    # Converte de string/timestamp ISO para DateType
    #
    # Exemplo de valor original:
    # 2024-01-01T10:00:00
    # Após to_date -> 2024-01-01
    df_silver = df_dedup \
        .withColumnRenamed("date", "transaction_date") \
        .withColumn(
            "transaction_date",
            to_date(col("transaction_date"))
        )
    
    # -------------------------
    # Escrita na camada Silver
    # -------------------------
    # Salva como Delta Lake, garantindo idempotência e schema evolutivo
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(dest_path)
        
    # Log de sucesso
    logger.info(f"Saved transactions Silver at {dest_path}")


def main():
    """
    Função principal do job Spark.
    Orquestra o refinamento das tabelas de clientes e transações.
    """
    
    # Cria a SparkSession configurada para o projeto
    spark = get_spark_session("RefineSilver")
    
    # Executa o refinamento de cada entidade
    refine_customers(spark)
    refine_transactions(spark)
    
    # Log de conclusão do job
    logger.info("Silver Refinement Complete")
    
    # Encerra a SparkSession de forma limpa
    spark.stop()


# Ponto de entrada do script
if __name__ == "__main__":
    main()