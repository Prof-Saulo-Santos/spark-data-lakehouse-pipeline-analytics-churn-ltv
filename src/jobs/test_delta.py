# -------------------------
# Imports de bibliotecas
# -------------------------

# sys é usado para encerrar o processo com código de erro em caso de falha
import sys

# SparkSession é o ponto de entrada para aplicações Spark
from pyspark.sql import SparkSession


def main():
    """
    Script de teste para validar se o ambiente Spark está corretamente
    configurado com suporte ao Delta Lake.

    Objetivo:
    - Inicializar uma SparkSession com Delta Lake habilitado
    - Criar um DataFrame simples
    - Escrever e ler uma tabela Delta
    - Confirmar que o ambiente está operacional
    """
    
    # Mensagem inicial de teste
    print("Testing Spark + Delta Lake...")
    
    try:
        # -------------------------
        # Criação da SparkSession com suporte ao Delta Lake
        # -------------------------
        spark = (
            SparkSession.builder
            # Nome da aplicação Spark
            .appName("DeltaTest")
            
            # Pacote do Delta Lake compatível com Scala 2.12
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.1"
            )
            
            # Extensão necessária para habilitar comandos SQL do Delta
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            
            # Catálogo padrão do Spark apontando para o Delta
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            
            # Cria ou reutiliza a SparkSession
            .getOrCreate()
        )
        
        # -------------------------
        # Criação de um DataFrame simples para teste
        # -------------------------
        data = [
            ("Alice", 1),
            ("Bob", 2)
        ]
        
        # Cria o DataFrame com schema implícito
        df = spark.createDataFrame(
            data,
            ["name", "id"]
        )
        
        # -------------------------
        # Escrita do DataFrame em formato Delta
        # -------------------------
        print("Writing Delta Table...")
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save("/tmp/delta-test-table")
        
        # -------------------------
        # Leitura da tabela Delta
        # -------------------------
        print("Reading Delta Table...")
        
        df_read = (
            spark.read
            .format("delta")
            .load("/tmp/delta-test-table")
        )
        
        # Exibe o conteúdo lido
        df_read.show()
        
        # Se tudo ocorreu corretamente
        print("SUCCESS: Spark + Delta operational!")
        
    except Exception as e:
        # Em caso de erro, exibe a mensagem e encerra com código != 0
        print(f"FAILURE: {e}")
        sys.exit(1)


# -------------------------
# Ponto de entrada do script
# -------------------------
if __name__ == "__main__":
    main()