# Importa a classe SparkSession (usada apenas para tipagem/documentação)
from pyspark.sql import SparkSession

# Importa funções do Spark SQL com alias padrão de mercado
from pyspark.sql import functions as F

# Função utilitária para criar SparkSession configurada com Delta Lake
from src.lib.spark_utils import get_spark_session

# Logger estruturado em JSON
from src.lib.logger import StructuredLogger


# Logger específico para a camada Gold
logger = StructuredLogger("AggGold")


def main():
    """
    Job responsável pela criação da camada Gold (Feature Store).

    Objetivos:
    - Consolidar dados transacionais em nível de cliente
    - Gerar features RFM
    - Produzir dados prontos para Analytics e Machine Learning
    """

    # Inicializa Spark
    spark = get_spark_session("AggGold")

    # -------------------------
    # Definição dos caminhos
    # -------------------------
    customers_path = "data/silver/customers"
    transactions_path = "data/silver/transactions"
    gold_path = "data/gold/customer_features"

    logger.info("Starting Gold Aggregation (RFM Features)")

    # -------------------------
    # 1. Leitura dos dados Silver
    # -------------------------
    df_cust = spark.read.format("delta").load(customers_path)
    df_trans = spark.read.format("delta").load(transactions_path)

    # -------------------------
    # 2. Data de referência
    # -------------------------
    # Em produção poderia ser current_date()
    # Aqui usamos a maior data disponível no dataset
    max_date = (
        df_trans
        .select(F.max("transaction_date"))
        .collect()[0][0]
    )

    logger.info(f"Reference Date for Recency: {max_date}")

    # -------------------------
    # 3. Agregação RFM
    # -------------------------
    df_rfm = df_trans.groupBy("customer_id").agg(
        F.max("transaction_date").alias("last_purchase_date"),
        F.count("transaction_id").alias("frequency"),
        F.avg("amount").alias("monetary_value")
    )

    # -------------------------
    # 4. Cálculo da Recency
    # -------------------------
    # Recency = dias desde a última compra
    df_rfm = df_rfm.withColumn(
        "recency",
        F.datediff(
            F.lit(max_date),
            F.col("last_purchase_date")
        )
    )

    # -------------------------
    # 5. Join com perfil do cliente
    # -------------------------
    # LEFT JOIN para manter clientes sem transações
    df_final = df_cust.join(
        df_rfm,
        on="customer_id",
        how="left"
    )

    # -------------------------
    # 6. Tratamento correto de clientes sem compras (OPÇÃO A)
    # -------------------------
    # Para clientes sem compras:
    # - frequency = 0
    # - monetary_value = 0
    # - recency = dias desde o signup até a data de referência
    #
    # Isso evita valores sentinela artificiais (ex: 999)
    # e mantém coerência temporal e estatística
    df_final = (
        df_final
        .withColumn(
            "recency",
            F.when(
                F.col("recency").isNull(),
                F.datediff(
                    F.lit(max_date),
                    F.col("signup_date")
                )
            ).otherwise(F.col("recency"))
        )
        .fillna({
            "frequency": 0,
            "monetary_value": 0
        })
    )

    # -------------------------
    # 7. Escrita da camada Gold
    # -------------------------
    logger.info(f"Saving features to {gold_path}")

    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold_path)

    logger.info("Gold Aggregation Complete")

    # Finaliza Spark
    spark.stop()


# Ponto de entrada
if __name__ == "__main__":
    main()