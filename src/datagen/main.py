# -------------------------
# Imports de bibliotecas
# -------------------------

# Click é utilizado para criar interfaces de linha de comando (CLI)
import click

# Biblioteca padrão para manipulação de arquivos e diretórios
import os

# Classe responsável pela geração de dados sintéticos
from src.datagen.generator import DataGenerator

# Logger estruturado para padronização dos logs
from src.lib.logger import StructuredLogger


# Instancia um logger específico para o CLI de geração de dados
logger = StructuredLogger("DataGenCLI")


# -------------------------
# Definição do comando CLI
# -------------------------
# Este decorador transforma a função main em um comando executável via terminal
@click.command()

# Opção para definir a quantidade de registros de transações a serem gerados
@click.option(
    '--rows',
    default=1000,
    help='Number of rows to generate.'
)

# Opção para definir o diretório de saída dos arquivos gerados
@click.option(
    '--output',
    default='data/bronze',
    help='Output directory.'
)
def main(rows, output):
    """
    CLI responsável pela geração de dados sintéticos de clientes e transações.

    Uso típico:
    python datagen.py --rows 5000 --output data/bronze

    Parâmetros:
    - rows: número de registros de transações
    - output: diretório onde os arquivos Parquet serão salvos
    """
    
    # Log de início da geração de dados com parâmetros informados
    logger.info(
        "Starting Data Generation",
        rows=rows,
        output=output
    )
    
    # Garante que o diretório de saída exista
    os.makedirs(output, exist_ok=True)
    
    # Instancia o gerador de dados sintéticos
    gen = DataGenerator()
    
    # -------------------------
    # Geração de dados de Customers
    # -------------------------
    
    # Define a quantidade de clientes como 10% do total de transações
    # Garante pelo menos 1 cliente
    n_customers = max(1, rows // 10)
    
    # Gera o DataFrame de clientes
    df_customers = gen.generate_customers(
        count=n_customers
    )
    
    # Caminho de saída do arquivo de clientes
    cust_path = os.path.join(
        output,
        "customers.parquet"
    )
    
    # Salva os dados no formato Parquet
    # Em um cenário real, poderia ser JSON ou CSV como dado "raw"
    # Aqui usamos Parquet por eficiência e simplicidade
    df_customers.to_parquet(
        cust_path,
        index=False
    )
    
    # Log de sucesso da geração de clientes
    logger.info(
        f"Saved {len(df_customers)} customers to {cust_path}"
    )
    
    # -------------------------
    # Geração de dados de Transactions
    # -------------------------
    
    # Extrai a lista de IDs de clientes gerados
    customer_ids = df_customers['customer_id'].tolist()
    
    # Gera o DataFrame de transações associadas aos clientes
    df_transactions = gen.generate_transactions(
        customer_ids,
        count=rows
    )
    
    # Caminho de saída do arquivo de transações
    trans_path = os.path.join(
        output,
        "transactions.parquet"
    )
    
    # Salva os dados de transações em Parquet
    df_transactions.to_parquet(
        trans_path,
        index=False
    )
    
    # Log de sucesso da geração de transações
    logger.info(
        f"Saved {len(df_transactions)} transactions to {trans_path}"
    )


# -------------------------
# Ponto de entrada do script
# -------------------------
if __name__ == '__main__':
    main()