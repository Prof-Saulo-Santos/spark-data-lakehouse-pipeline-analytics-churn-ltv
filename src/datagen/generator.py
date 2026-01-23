# -------------------------
# Imports de bibliotecas
# -------------------------

# Pandas para criação e manipulação de DataFrames
import pandas as pd

# NumPy para geração de números aleatórios e distribuições estatísticas
import numpy as np

# Faker para geração de dados sintéticos realistas (nomes, e-mails, datas, etc.)
from faker import Faker

# Logger estruturado para padronização dos logs
from src.lib.logger import StructuredLogger


# Instancia um logger específico para o gerador de dados
logger = StructuredLogger("DataGenerator")

# Instancia o Faker para geração de dados falsos
fake = Faker()


class DataGenerator:
    """
    Classe responsável por gerar dados sintéticos de clientes e transações.

    Objetivo:
    - Simular dados realistas para testes, aulas e pipelines de dados
    - Garantir reprodutibilidade através de seeds fixas
    """

    def __init__(self, seed=42):
        """
        Inicializa o gerador com seed fixa para reprodutibilidade.

        Parâmetros:
        - seed: valor utilizado para inicializar o gerador aleatório
        """
        # Define seed do Faker
        Faker.seed(seed)

        # Define seed do NumPy
        np.random.seed(seed)

    def generate_customers(self, count=1000):
        """
        Gera um conjunto de clientes sintéticos.

        Campos gerados:
        - customer_id : identificador único do cliente
        - name        : nome completo
        - email       : e-mail
        - signup_date : data de cadastro (até 2 anos atrás)
        - region      : região geográfica
        - segment     : segmento do cliente (com distribuição controlada)

        Parâmetros:
        - count: número de clientes a serem gerados

        Retorno:
        - DataFrame Pandas com os clientes gerados
        """
        
        # Log do início da geração
        logger.info(f"Generating {count} customers")
        
        data = []

        # Gera registros individuais de clientes
        for _ in range(count):
            data.append({
                # UUID único para cada cliente
                "customer_id": fake.uuid4(),

                # Nome completo aleatório
                "name": fake.name(),

                # E-mail aleatório
                "email": fake.email(),

                # Data de cadastro entre 2 anos atrás e hoje
                "signup_date": fake.date_between(
                    start_date='-2y',
                    end_date='today'
                ).isoformat(),

                # Região com escolha uniforme
                "region": np.random.choice(
                    ["North", "South", "East", "West"]
                ),

                # Segmento com probabilidades definidas
                # Premium 10%, Standard 60%, Basic 30%
                "segment": np.random.choice(
                    ["Premium", "Standard", "Basic"],
                    p=[0.1, 0.6, 0.3]
                )
            })

        # Converte a lista de dicionários em DataFrame
        return pd.DataFrame(data)

    def generate_transactions(self, customer_ids, count=10000):
        """
        Gera transações sintéticas associadas a clientes existentes.

        Campos gerados:
        - transaction_id : identificador único da transação
        - customer_id    : referência a um cliente existente
        - date           : data/hora da transação
        - amount         : valor monetário da transação
        - category       : categoria do produto

        Parâmetros:
        - customer_ids: lista de IDs de clientes válidos
        - count: número de transações a serem geradas

        Retorno:
        - DataFrame Pandas com as transações geradas
        """
        
        # Log do início da geração
        logger.info(
            f"Generating {count} transactions for {len(customer_ids)} customers"
        )
        
        data = []

        # Converte lista de IDs para array NumPy (melhor performance)
        cust_array = np.array(customer_ids)

        # Gera registros individuais de transações
        for _ in range(count):
            data.append({
                # UUID único para cada transação
                "transaction_id": fake.uuid4(),

                # Associa a transação a um cliente existente
                "customer_id": np.random.choice(cust_array),

                # Data/hora da transação (até 2 anos atrás)
                "date": fake.date_time_between(
                    start_date='-2y',
                    end_date='now'
                ).isoformat(),

                # Valor da transação com distribuição log-normal
                # Simula comportamento realista de gastos
                "amount": round(
                    np.random.lognormal(mean=3, sigma=1),
                    2
                ),

                # Categoria fictícia do produto (renomeado para manter consistência)
                "product_id": fake.word()
            })

        # Converte a lista de dicionários em DataFrame
        return pd.DataFrame(data)