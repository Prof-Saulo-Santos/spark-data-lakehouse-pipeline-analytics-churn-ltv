# Manual Completo de Implementação: Data Lakehouse & ML Analytics
**Projeto: Customer Intelligence 360**

Este manual reúne todas as fases de implementação do projeto, desde a configuração da infraestrutura até o treinamento dos modelos de Machine Learning. Ele foi desenhado para ser autossuficiente: contém tanto as explicações teóricas quanto o código-fonte integral, permitindo que qualquer engenheiro reproduza o ambiente do zero.

---

# Capítulo 1: Configuração, Infraestrutura e Planejamento

## 1.1 Objetivo
Estabelecer uma fundação sólida para o projeto. Em vez de scripts soltos, foi criada uma estrutura profissional que suporta processamento em larga escala (Spark), armazenamento confiável (Delta Lake) e observabilidade (Logs estruturados).

## 1.2 Plano de Execução & Setup Inicial
O fluxo segue a metodologia de "Fail-Fast" e desenvolvimento iterativo. Antes da codificação, é preciso ter a "casa arrumada".

### Pré-requisitos
*   **Python 3.10+**: `python3 --version`
*   **Git**: `git --version`
*   **Poetry**: `pipx install poetry` (Gerenciador de dependências moderno)

### Passo 1: Inicialização do Git
```bash
# Na raiz do projeto
git init
git branch -M main
```

### Passo 2: Estrutura de Diretórios
Antes da configuração, deve-se criar a estrutura de pastas e os arquivos `__init__.py` para que o Python reconheça os pacotes.

```bash
mkdir -p src/datagen src/jobs src/lib src/ml
touch src/__init__.py src/datagen/__init__.py src/jobs/__init__.py src/lib/__init__.py src/ml/__init__.py
```

### Passo 3: Arquivo `.gitignore`
Essencial para não commitar lixo (logs, dados temporários, ambientes virtuais).

```gitignore
__pycache__/
*.py[cod]
.venv/
poetry.lock
.env
.pytest_cache/
.coverage
htmlcov/
dist/
.DS_Store

# Data Lake Local (Não versionar dados massivos!)
data/bronze/*
data/silver/*
data/gold/*
!data/**/.gitkeep

# Spark/Metastore
metastore_db/
derby.log
spark-warehouse/
```

### Passo 4: Gerenciamento de Dependências (`pyproject.toml`)
Este arquivo define o projeto e suas bibliotecas. Note que as versões abaixo são as testadas para este projeto.

```toml
[tool.poetry]
name = "customer-intelligence-360"
version = "0.1.0"
description = "Predição de Churn e LTV em escala com Spark, Delta Lake e MLOps"
authors = ["Engenheiro de Dados <seu@email.com>"]
readme = "README.md"
packages = [{include = "src"}]

[tool.poetry.dependencies]
python = ">=3.10,<3.13"
# Core
pyspark = "3.5.3"
delta-spark = "3.2.1"
# Local Data
pandas = "2.1.4"
numpy = "1.26.4"
# Generation & Quality
Faker = "30.8.2"
great-expectations = "1.3.14"
# Models
mlflow = "2.19.0"
scikit-learn = "1.5.2"
xgboost = "2.1.3"
lifetimes = "0.11.3"
# Utils
click = "8.1.8"
python-dotenv = "1.0.1"
setuptools = "^80.9.0"

[tool.poetry.group.dev.dependencies]
pytest = "8.3.4"
bump2version = "1.0.1"
pre-commit = "4.0.1"
black = "24.10.0"
ruff = "0.8.3"

[tool.poetry.scripts]
ci360-datagen = "src.datagen.main:main"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

### Passo 5: Configuração de Versionamento (`.bumpversion.cfg`)
Automatiza o incremento de versão (ex: `v0.1.0` -> `v0.2.0`) em todos os arquivos necessários.

```ini
[bumpversion]
current_version = 0.1.0
commit = True
tag = True
tag_name = v{new_version}

[bumpversion:file:pyproject.toml]
search = version = "{current_version}"
replace = version = "{new_version}"

[bumpversion:file:src/lib/__init__.py]
search = __version__ = "{current_version}"
replace = __version__ = "{new_version}"
```

### Passo 6: Automação (`Makefile`)
Crie um arquivo `Makefile` para criar atalhos de comando.

```makefile
.PHONY: setup dataset etl train clean bump-patch bump-minor bump-major

setup:
	pip install poetry
	poetry install

dataset:
	poetry run python src/datagen/main.py --rows 1000000

etl:
	poetry run python src/jobs/ingestion_bronze.py
	poetry run python src/jobs/refine_silver.py
	poetry run python src/jobs/agg_gold.py

train:
	poetry run python src/ml/train_churn.py

clean:
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -rf data/gold/*
	rm -rf spark-warehouse
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} +

bump-patch:
	poetry run bump2version patch --verbose

bump-minor:
	poetry run bump2version minor --verbose

bump-major:
	poetry run bump2version major --verbose
```


### Passo 7: Documentação (`README.md`)
O Poetry exige um arquivo `README.md`. Crie-o na raiz com o seguinte conteúdo:

```markdown
# Customer Intelligence 360

```

### 🚀 Passo 8: Instalação!

Com todos os arquivos de configuração criados (`pyproject.toml`, `Makefile`, `README.md`, etc.), a fundação está pronta. Agora, com a configuração finalizada, ver-se-á a mágica acontecer.

**Abrir o terminal na raiz do projeto e executar:**

```bash
make setup
```

Este comando vai acionar o Poetry, criar o ambiente virtual isolado e baixar todas as bibliotecas pesadas (Spark, Delta Lake, XGBoost).
Se forem vistas várias barras de progresso terminando com sucesso... Parabéns! 🎉
O ambiente de Engenharia de Dados está pronto.

Agora, **segue-se para a codificação** dos scripts:

1. src/obs/test_delta.py
2. src/lib/spark_utils.py
3. src/lib/logger.py
---
### 🧪 Passo 9: Teste de Fumaça (Smoke Test)

Antes de gerar terabytes de dados, garanta que o Spark conseguirá escrever no disco.
Crie o arquivo `src/jobs/test_delta.py` com o conteúdo abaixo:

```python
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
```

**Executar o teste:**
```bash
poetry run python src/jobs/test_delta.py
```

Se aparecer `SUCCESS: Spark + Delta operational!`, você está aprovado para a Fase 2.

## 1.3 Estrutura de Diretórios

A organização das pastas segue o padrão de projetos de Engenharia de Dados modernos, separando código, dados e configuração.

```text
raiz/
├── data/               # Armazenamento do Lakehouse (Local)
│   ├── bronze/         # Dados brutos (Delta Lake)
│   ├── silver/         # Dados limpos e deduplicados
│   └── gold/           # Dados agregados para Analytics/ML
├── docs/               # Documentação do projeto
├── src/                # Código-fonte
│   ├── datagen/        # Scripts de geração de dados
│   ├── jobs/           # Jobs Spark (ETL)
│   ├── lib/            # Utilitários compartilhados (Logger, Spark Session)
│   └── ml/             # Scripts de treinamento de modelos
├── tests/              # Testes unitários e de integração
├── mlruns/             # Rastreamento de experimentos (MLflow)
├── poetry.lock         # Versões exatas das dependências
├── pyproject.toml      # Definição do projeto e dependências
└── Makefile            # Atalhos para comandos comuns
```

## 1.4 Infraestrutura de Código Base

Estes arquivos "lib" são os alicerces do projeto. Eles abstraem a complexidade de configuração do Spark e padronizam o logging.

### `src/lib/spark_utils.py`
**O que faz:** Cria e devolve uma `SparkSession` pronta para uso.
**Por que é necessário:** Configurar o Delta Lake envolve várias flags (`spark.sql.extensions`, `spark.jars.packages`). Centralizar isso evita bugs de configuração e repetição de código.
**Destaques:**
*   **Delta Integration**: As configs `io.delta` habilitam o Delta Lake (ACID transactions).
*   **Local Mode**: `master("local[*]")` instrui o Spark a usar todos os núcleos de CPU disponíveis na sua máquina.

```python
from pyspark.sql import SparkSession
import os

def get_spark_session(app_name: str, local: bool = True) -> SparkSession:
    """
    Creates or gets a Spark Session configured for Delta Lake.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")

    if local:
        # Optimization for local mode
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g")
            
    spark = builder.getOrCreate()
    
    # Adjust log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
```

### `src/lib/logger.py`
**O que faz:** Padroniza a saída de logs em formato JSON.
**Por que é necessário:** Em produção, logs de texto puro são difíceis de parsear. Logs JSON são ingeridos facilmente por ferramentas como Datadog, Splunk ou CloudWatch (AWS).

```python
# -------------------------
# Imports de bibliotecas padrão
# -------------------------

# Módulo padrão de logging do Python
import logging

# JSON para serialização estruturada dos logs
import json

# Socket para obter informações da máquina (hostname)
import socket

# Datetime para registro preciso do horário do evento
from datetime import datetime


class StructuredLogger:
    """
    Logger estruturado que gera logs em formato JSON.

    Objetivos:
    - Facilitar ingestão por sistemas de observabilidade (ELK, OpenSearch, Datadog)
    - Padronizar logs em pipelines de dados e MLOps
    - Permitir inclusão de metadados contextuais (ex: table_name, rows, source)
    """

    def __init__(self, name="CI360"):
        """
        Inicializa o logger estruturado.

        Parâmetros:
        - name: nome lógico do logger (normalmente o nome do serviço ou pipeline)
        """
        
        # Cria ou recupera um logger com o nome especificado
        self.logger = logging.getLogger(name)
        
        # Define o nível mínimo de log (INFO)
        self.logger.setLevel(logging.INFO)
        
        # Handler para saída em console (stdout)
        handler = logging.StreamHandler()
        
        # Formatter simples: a mensagem já estará em JSON
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        
        # Evita adicionar múltiplos handlers ao mesmo logger
        # (comum em execuções repetidas ou testes)
        if not self.logger.handlers:
            self.logger.addHandler(handler)
        
        # Captura o hostname da máquina para rastreabilidade
        self.hostname = socket.gethostname()

    def _format(self, level, message, **kwargs):
        """
        Formata a mensagem de log em um objeto JSON.

        Campos padrão:
        - timestamp: data/hora em UTC
        - level: nível do log (INFO, ERROR, WARNING)
        - host: hostname da máquina
        - message: mensagem principal
        - kwargs: metadados adicionais fornecidos na chamada
        """
        
        log_entry = {
            # Timestamp em UTC para padronização em ambientes distribuídos
            "timestamp": datetime.utcnow().isoformat(),
            
            # Nível do log
            "level": level,
            
            # Host onde o processo está rodando
            "host": self.hostname,
            
            # Mensagem principal
            "message": message,
            
            # Metadados adicionais (ex: tabela, linhas, caminho)
            **kwargs
        }
        
        # Converte o dicionário para string JSON
        return json.dumps(log_entry)

    def info(self, message, **kwargs):
        """
        Registra uma mensagem de log no nível INFO.
        """
        self.logger.info(
            self._format("INFO", message, **kwargs)
        )

    def error(self, message, **kwargs):
        """
        Registra uma mensagem de log no nível ERROR.
        """
        self.logger.error(
            self._format("ERROR", message, **kwargs)
        )
    
    def warning(self, message, **kwargs):
        """
        Registra uma mensagem de log no nível WARNING.
        """
        self.logger.warning(
            self._format("WARNING", message, **kwargs)
        )

```


### 🛑 Checkpoint: Infraestrutura Base (v0.1.0)

Com a infraestrutura configurada e testada, será salva a primeira versão do projeto. Esta versão (`v0.1.0`) representa o ambiente base estável.

1.  **Commitar Infraestrutura:**
    ```bash
    git add .
    git commit -m "build: setup project infrastructure"
    ```

2.  **Tag Inicial:**
    Como a versão `0.1.0` já foi definida no `pyproject.toml`, ela será oficializada no Git:
    ```bash
    git tag v0.1.0
    ```

3.  **Sincronizar com Repositório Remoto:**
    Troque o link abaixo pelo link do seu repositório:
    ```bash
    git remote add origin https://github.com/Prof-Saulo-Santos/spark-data-lakehouse-pipeline-analytics-churn-ltv
    git push -u origin main
    git push --tags
    ```

---

# Capítulo 2: Geração de Dados (Fase 2)

## 2.1 Visão Geral
Como não há acesso a dados reais de clientes (LGPD/GDPR), foi criado um gerador de dados sintéticos.
**Objetivo:** Gerar arquivos `.parquet` contendo histórico de navegação e compras.
**Ferramenta:** `Faker` (biblioteca Python para gerar nomes, emails e datas falsas mas realistas).

## 2.2 Scripts do Gerador

### `src/datagen/generator.py`
**O que faz:** Encapsula a lógica de "inventar" dados.
**Regras de Negócio Simuladas:**
*   `generate_customers`: Cria perfis com data de cadastro (signup_date) nos últimos 2 anos.
*   `generate_transactions`: Cria compras associadas a esses clientes. O valor (amount) segue uma distribuição log-normal para simular compras reais (muitas compras pequenas, poucas compras grandes).

```python
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

```

### `src/datagen/main.py`
**O que faz:** É a interface de linha de comando (CLI). Orquestra a chamada das funções acima e salva os resultados em disco.
**Bibliotecas:** `click` facilita criar comandos como `--rows 1000`.

```python
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

```

---

### Preparação para Versionamento
Para que o `bumpversion` funcione, ele precisa encontrar a string de versão no arquivo inicial.
Edite o arquivo `src/lib/__init__.py` e adicione a seguinte linha:

```python
__version__ = "0.1.0"
```

### 🛑 Checkpoint: Execução & Versionamento (v0.2.0)

Agora que foi criado o Gerador de Dados, este deve ser executado, o progresso salvo e a versão 0.2.0 lançada.


1.  **Executar o Gerador:**
    ```bash
    make dataset
    # ou: poetry run python src/datagen/main.py --rows 1000000
    ```
**Os arquivos serão gerados em:**
- `data/bronze/customers.parquet`
- `data/bronze/transactions.parquet` 

2.  **Commitar Mudanças:**
    ```bash
    git add .
    git commit -m "feat: implement data generator"
    ```

3.  **Lançar Versão v0.2.0:**
    ```bash
    make bump-minor
    # Isso atualizará automaticamente a versão de 0.1.0 para 0.2.0
    ```

4.  **Sincronizar com Repositório Remoto:**
    ```bash
    git push origin main
    git push --tags
    ```
---

# Capítulo 3: Ingestão Bronze (Fase 3)

## 3.1 Visão Geral
Os arquivos `parquet` (que poderiam ser CSV, JSON) são transformados em formato **Delta Lake**.
**Por que Delta?** Parquet comum não suporta atualizações atômicas (ACID). Se um job falhar no meio, dados podem ficar corrompidos. O Delta resolve isso.
**Data Contracts:** Também é introduzida aqui uma validação básica. Se o arquivo não tiver a coluna `customer_id`, a ingestão é rejeitada.

## 3.2 Validação e Jobs

### `src/lib/quality.py`
**O que faz:** Verifica se o DataFrame atende aos requisitos mínimos antes de salvar.

```python
# -------------------------
# Imports do PySpark
# -------------------------

# DataFrame é a estrutura básica de dados no Spark
from pyspark.sql import DataFrame

# Funções auxiliares para operações em colunas (não usadas aqui, mas comuns em validações)
from pyspark.sql.functions import col

# Logger estruturado para padronização dos logs
from src.lib.logger import StructuredLogger


# Instancia um logger específico para validações de qualidade
logger = StructuredLogger("DataQuality")


class DataQuality:
    """
    Classe responsável por validar contratos mínimos de qualidade de dados
    antes que os dados avancem no pipeline (ex: Bronze -> Silver).
    
    Esta implementação é propositalmente simples e didática.
    
    > **Nota para Produção:**
    > Esta classe não valida ranges, tipos complexos ou nulidade avançada.
    > Em ambientes reais, ela deve ser substituída por frameworks robustos como:
    > - Great Expectations
    > - AWS Deequ
    > - Soda
    """

    def __init__(self, context_name: str):
        """
        Inicializa o contexto da validação.

        Parâmetros:
        - context_name: nome lógico da tabela ou etapa do pipeline
        """
        self.context = context_name

    def validate_contract(self, df: DataFrame, required_columns: list) -> bool:
        """
        Validação básica de contrato de dados.

        Regras implementadas:
        1. Verificar se todas as colunas obrigatórias existem
        2. Verificar se o DataFrame não está vazio

        Observação:
        - Esta validação NÃO verifica tipos, ranges ou regras estatísticas
        - Serve como uma proteção mínima contra dados quebrados

        Parâmetros:
        - df: DataFrame Spark a ser validado
        - required_columns: lista de colunas obrigatórias

        Retorno:
        - True  -> contrato atendido
        - False -> contrato violado
        """
        
        # Log de início da validação com contexto e colunas exigidas
        logger.info(
            f"Validating contract for {self.context}",
            required_columns=required_columns
        )
        
        # -------------------------
        # 1. Validação de Schema (existência das colunas)
        # -------------------------
        
        # Conjunto de colunas existentes no DataFrame
        df_cols = set(df.columns)
        
        # Identifica colunas obrigatórias ausentes
        missing_cols = [
            c for c in required_columns
            if c not in df_cols
        ]
        
        # Se houver colunas ausentes, o contrato falha
        if missing_cols:
            logger.error(
                "Contract Failed: Missing columns",
                missing=missing_cols
            )
            return False
            
        # -------------------------
        # 2. Validação de DataFrame vazio
        # -------------------------
        # Verifica se o DataFrame não possui nenhuma linha
        # (checagem simples e opcional, mas recomendada)
        if df.rdd.isEmpty():
            logger.error(
                "Contract Failed: DataFrame is empty"
            )
            return False
            
        # Se todas as validações passaram
        logger.info("Contract Validation Passed")
        return True

```

### `src/jobs/ingestion_bronze.py`
**O que faz:**
1. Lê os arquivos brutos (Raw).
2. Valida qualidade.
3. Escreve na camada Bronze como tabela Delta.
**Nota:** Usa-se `.option("overwriteSchema", "true")` para permitir que o esquema evolua caso sejam adicionadas colunas no futuro.

```python
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

```

### 🛑 Checkpoint: Execução & Versionamento (v0.3.0)

Os dados brutos serão ingeridos e o contrato validado.

1.  **Executar Ingestão Bronze:**

    ```bash
    poetry run python src/jobs/ingestion_bronze.py
    ```
    *Verifique se a pasta `data/bronze/customers` (Delta) foi criada.*

- Os avisos (WARN) que aparecerão durante a execução, são apenas o Spark reclamando que está rodando localmente (loopback IP) e sem bibliotecas nativas do Hadoop otimizadas para o seu SO, o que é completamente normal e esperado para um ambiente de desenvolvimento local. 
- OBS: Quando as pessoas dizem "Hadoop está obsoleto", elas geralmente se referem a duas coisas:

    - HDFS (Sistema de Arquivos): Foi amplamente substituído pelo armazenamento em nuvem (S3, Azure Blob, GCS).
    - MapReduce (Motor de Processamento): Foi totalmente esmagado pelo Spark. O Spark é 100x mais rápido porque processa em memória (RAM), enquanto o   - MapReduce gravava em disco a cada etapa.

- Então por que o Spark ainda pede "bibliotecas do Hadoop"? Porque, ironicamente, o Spark nasceu dentro do ecossistema Hadoop. Mesmo que você use S3 e Spark, o Spark ainda usa internamente as APIs de cliente do Hadoop (as hadoop-client-libs) para saber como "falar" com sistemas de arquivos distribuídos. Ele usa o código legado do Hadoop apenas como um "driver" ou "conector" para ler/gravar arquivos, não para processar os dados.

Resumindo: O Hadoop como plataforma central (clusters gigantes de HDFS + YARN) está de fato em declínio/legado, mas o código do cliente Hadoop ainda vive escondido dentro do Spark como uma dependência de baixo nível para I/O.



2.  **Commitar Mudanças:**
    ```bash
    git add .
    git commit -m "feat: implement bronze ingestion"
    ```

3.  **Lançar Versão v0.3.0:**
    ```bash
    make bump-minor
    # Isso atualizará automaticamente a versão de 0.2.0 para 0.3.0
    ```

4.  **Sincronizar com Repositório Remoto:**
    ```bash
    git push origin main
    git push --tags
    ```
---

# Capítulo 4: Refinamento Silver (Fase 4)

## 4.1 Visão Geral
A camada **Silver** é o coração da qualidade do Data Lakehouse. Enquanto a Bronze é uma cópia fiel (e muitas vezes "suja") da origem, a Silver contém dados limpos, tipados e confiáveis, prontos para análise exploratória.

Nesta etapa, é essencial aplicar regras rigorosas de transformação:

1.  **Deduplicação (De-duplication):**
    Garante a unicidade dos registros (idempotência). Se o sistema de origem enviou a mesma transação duas vezes por erro de rede ou reprocessamento, o Spark detecta e mantém apenas uma versão, eliminando redundâncias.

2.  **Imposição de Schema e Tipagem (Schema Enforcement):**
    Converte dados genéricos (onde tudo chega como `string`) para tipos específicos (`Integer`, `Double`, `Date`, `Timestamp`). Isso habilita operações aritméticas e filtros de data performáticos, além de evitar erros de conversão no futuro.
    *   Exemplo: `"2023-01-01"` (String) → `2023-01-01` (DateType).

3.  **Padronização e Renomeação (Standardization):**
    Ajusta nomes de colunas para seguir convenções de engenharia (snake_case) e evita conflitos com palavras reservadas de SQL/Spark.
    *   Exemplo: Alterar a coluna `date` para `transaction_date`, pois `DATE` é um tipo de dado reservado em SQL.

### `src/jobs/refine_silver.py`

```python
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

```

### 🛑 Checkpoint: Execução & Versionamento (v0.4.0)

Momento de limpar os dados e criar a camada confiável.

1.  **Executar Refinamento Silver:**
    ```bash
    poetry run python src/jobs/refine_silver.py
    ```
    *Verifique se a pasta `data/silver/` foi populada.*

2.  **Commitar Mudanças:**
    ```bash
    git add .
    git commit -m "feat: implement silver refinement"
    ```

3.  **Lançar Versão v0.4.0:**
    ```bash
    make bump-minor
    # Isso atualizará automaticamente a versão de 0.3.0 para 0.4.0
    ```

4.  **Sincronizar com Repositório Remoto:**
    ```bash
    git push origin main
    git push --tags
    ```

---

# Capítulo 5: Agregação Gold (Fase 5)

## 5.1 Visão Geral: Feature Store
A camada **Gold** é onde os dados se transformam em **Inteligência**. Aqui, deixamos de olhar para "transações" (eventos isolados) e passamos a olhar para "entidades" (o Cliente).

Machine Learning não aprende bem com dados brutos como "João comprou pão na terça" e "João comprou leite na quarta". Os modelos precisam de **Features** (Atributos) consolidados que descrevam o perfil de consumo.

Será utilizada a metodologia **RFM**, um clássico do Marketing Analítico:

1.  **Recência (Recency):** "Há quanto tempo o cliente não compra?"
    *   *Insight:* Clientes que compraram recentemente são mais propensos a comprar de novo do que aqueles que sumiram há 6 meses.

2.  **Frequência (Frequency):** "Quantas vezes ele comprou no total?"
    *   *Insight:* Clientes fiéis têm alta frequência. Clientes esporádicos têm baixa.

3.  **Monetário (Monetary):** "Quanto ele gasta em média (Ticket Médio)?"
    *   *Insight:* Separa os clientes "Baleia" (alto valor) dos clientes "Sardinha" (baixo valor).

O resultado final será uma **Tabela Analítica (ABT - Analytical Base Table)** com uma linha única por cliente, pronta para alimentar modelos de Churn e LTV.

### `src/jobs/agg_gold.py`
**Destaque Técnico (Left Join Strategy):**
A intuição inicial seria fazer um `inner join` entre Clientes e Transações. Porém, isso seria um **erro grave** para modelagem.
*   **O Problema:** Um `inner join` descartaria todos os clientes que se cadastraram mas nunca compraram.
*   **A Solução:** Use `left join` (Cliente -> Transações).
*   **Por que isso importa?** O modelo de Machine Learning precisa aprender o padrão de **quem compra** E TAMBÉM o padrão de **quem NÃO compra**. Clientes com `frequency = 0` são exemplos valiosos de "inativos" ou "churners imediatos". Se você os remove, introduz um **Viés de Sobrevivência (Survivorship Bias)** nos dados.

```python
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


```

### 🛑 Checkpoint: Execução & Versionamento (v0.5.0)

Criação da Feature Store para o Machine Learning.

1.  **Executar Agregação Gold:**
    ```bash
    poetry run python src/jobs/agg_gold.py
    ```
    *Verifique se a pasta `data/gold/customer_features` foi criada.*

2.  **Commitar Mudanças:**
    ```bash
    git add .
    git commit -m "feat: implement gold features aggregation"
    ```

3.  **Lançar Versão v0.5.0:**
    ```bash
    make bump-minor
    # Isso atualizará automaticamente a versão de 0.4.0 para 0.5.0
    ```

4.  **Sincronizar com Repositório Remoto:**
    ```bash
    git push origin main
    git push --tags
    ```

---

# Capítulo 6: Machine Learning (Fase 6)

## 6.1 Visão Geral: De Dados para Decisões
Chegamos ao ápice do projeto. Com dados limpos (Silver) e agregados por comportamento (Gold), podemos finalmente responder perguntas estratégicas de negócio usando IA.

Este projeto aplica uma abordagem híbrida de modelagem:

1.  **Churn Prediction (O "Quem Sai"):**
    *   **Tipo:** Classificação Binária (Supervisionado).
    *   **Pergunta:** "Qual a probabilidade deste cliente abandonar a empresa nos próximos 90 dias?"
    *   **Ação de Negócio:** Enviar cupom de retenção *apenas* para quem tem alta chance de sair, economizando marketing.
    *   **Algoritmo:** XGBoost (Gradient Boosting), escolhido por sua robustez em dados tabulares desbalanceados.

2.  **LTV Prediction (O "Quem Vale a Pena"):**
    *   **Tipo:** Regressão Probabilística (Generativo).
    *   **Pergunta:** "Quanto dinheiro este cliente ainda vai gastar conosco até o fim da vida?"
    *   **Ação de Negócio:** Identificar clientes VIP (Baleias/Whales) para dar atendimento premium.
    *   **Algoritmo:** Lifetimes (BG/NBD), que modela estatisticamente a "morte" e a "frequência" do cliente.

## 6.2 Scripts de Treinamento

### `src/ml/train_churn.py` (XGBoost)
**Estratégia de Modelagem:**

1.  **Definição do Target (O que é Churn?):**
    *   Em contratos mensais (ex: Netflix), Churn é quando o usuário cancela. No varejo, não há cancelamento explícito.
    *   *Definição adotada:* Se o cliente não comprar nada por **90 dias** (Recência > 90), ele é considerado Churn.
    *   *Nota:* Este limiar (threshold) deve ser ajustado conforme o negócio (ex: venda de imóveis vs venda de pão).

2.  **Algoritmo: XGBoost (eXtreme Gradient Boosting):**
    *   Foi escolhido por ser o "estado da arte" para dados tabulares estruturados.
    *   *Vantagens:* Lida bem com valores nulos, previne overfitting e captura relações não-lineares complexas (ex: clientes que gastam muito pouco ou muito, ambos podem sair por motivos diferentes).

3.  **Experiment Tracking (MLflow):**
    *   Em vez de apenas printar a acurácia no terminal, o MLflow atua como uma "caixa preta" do avião. Ele grava:
        *   **Parâmetros:** Hiperparâmetros usados (learning rate, depth).
        *   **Métricas:** AUC, Acurácia, F1-Score.
        *   **Artefatos:** O próprio arquivo do modelo (`model.pkl`) para deploy futuro.

```python
import mlflow
import mlflow.xgboost
import xgboost as xgb
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score

from src.lib.spark_utils import get_spark_session
from src.lib.logger import StructuredLogger


# Logger específico para o treinamento de churn
logger = StructuredLogger("TrainChurn")


def main():
    """
    Job de treinamento do modelo de Churn utilizando XGBoost.

    Objetivos:
    - Ler features da camada Gold
    - Criar variável alvo (churn)
    - Tratar desbalanceamento de classes via scale_pos_weight
    - Treinar e avaliar modelo
    - Registrar experimento no MLflow
    """

    # -------------------------
    # 1. Leitura dos dados (Gold)
    # -------------------------
    spark = get_spark_session("TrainChurn")

    logger.info("Loading Gold data...")
    df_spark = spark.read.format("delta").load(
        "data/gold/customer_features"
    )

    # Converte para Pandas (assume dados cabem em memória)
    df = df_spark.toPandas()
    logger.info(f"Loaded {len(df)} records")

    # -------------------------
    # 2. Feature Engineering
    # -------------------------
    # Definição do alvo:
    # Cliente é considerado churn se não compra há mais de 90 dias
    df["churn"] = (df["recency"] > 90).astype(int)

    features = ["frequency", "monetary_value"]
    target = "churn"

    X = df[features]
    y = df[target]

    # -------------------------
    # 3. Split Train / Test
    # -------------------------
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.3,
        random_state=42,
        stratify=y  # mantém proporção das classes
    )

    # -------------------------
    # 4. Cálculo do scale_pos_weight
    # -------------------------
    # Fórmula recomendada pelo XGBoost:
    # scale_pos_weight = (# negativos) / (# positivos)
    # Conversão explícita para int/float nativos do Python
    # (JSON não serializa tipos do NumPy como int64/float32)
    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())

    scale_pos_weight = neg / pos

    logger.info(
        "Class balance (train set)",
        negative=neg,
        positive=pos,
        scale_pos_weight=scale_pos_weight
    )

    # -------------------------
    # 5. Treinamento do modelo
    # -------------------------
    mlflow.set_experiment("churn_prediction")

    with mlflow.start_run():

        logger.info("Training XGBoost Classifier...")

        model = xgb.XGBClassifier(
            objective="binary:logistic",
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            eval_metric="logloss",
            scale_pos_weight=scale_pos_weight,
            use_label_encoder=False,
            random_state=42
        )

        model.fit(X_train, y_train)

        # -------------------------
        # 6. Avaliação
        # -------------------------
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        acc = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_prob)

        logger.info(f"Accuracy: {acc:.4f}")
        logger.info(f"ROC-AUC: {auc:.4f}")

        # -------------------------
        # 7. Registro no MLflow
        # -------------------------
        mlflow.log_params({
            "n_estimators": 100,
            "learning_rate": 0.1,
            "max_depth": 5,
            "scale_pos_weight": scale_pos_weight
        })

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("roc_auc", auc)

        mlflow.xgboost.log_model(
            model,
            artifact_path="model"
        )

        logger.info("Model trained and logged to MLflow successfully")

    # Encerra Spark
    spark.stop()


# Ponto de entrada
if __name__ == "__main__":
    main()


```

### `src/ml/train_ltv.py` (Lifetimes)
**Estratégia de Modelagem:**

1.  **Framework Teórico (BG/NBD):**
    *   Utilizamos o modelo **Beta-Geometric / Negative Binomial Distribution**, popularizado por Peter Fader (Wharton School).
    *   *Como funciona:* Ele assume que o processo de compra do cliente segue uma distribuição de probabilidade (moeda viciada). Enquanto a moeda der "cara", ele continua "vivo" e comprando. Se der "coroa", ele "morre" (Churn). O modelo estima os parâmetros latentes dessas moedas para cada cliente.

2.  **Preparação dos Dados (Lifetimes format):**
    *   O algoritmo exige um formato específico:
        *   `frequency`: Quantas compras repetidas ele fez?
        *   `recency`: Qual a "idade" do cliente no momento da última compra?
        *   `T`: Qual a idade total do cliente hoje?
    *   *Validação Crítica:* Em dados reais (e sintéticos), às vezes ocorrem anomalias onde a data da compra é anterior ao cadastro. O script aplica um filtro rigoroso (`mask_valid`) para garantir que `T >= recency`, caso contrário o modelo matemático quebra.

3.  **Regularização (Penalizer):**
    *   Como muitos clientes têm poucas compras (esparsidade), o modelo pode "alucinar" e prever que um cliente que comprou 1 vez vai comprar 1000 vezes amanhã.
    *   *Solução:* Aplicamos `penalizer_coef=0.1` para "acalmar" o modelo e forçar previsões mais conservadoras e realistas.

```python
# -------------------------
# Imports de MLOps e Modelagem Estatística
# -------------------------

# MLflow para rastreamento de experimentos (parâmetros, métricas e execuções)
import mlflow

# Pandas para manipulação de dados em memória
import pandas as pd

# Modelo probabilístico BG/NBD para previsão de frequência de compras (LTV)
from lifetimes import BetaGeoFitter

# Utilitário para criação da SparkSession configurada (Delta Lake, modo local)
from src.lib.spark_utils import get_spark_session

# Logger estruturado em JSON para padronização dos logs
from src.lib.logger import StructuredLogger


# Instancia um logger específico para o treinamento do modelo de LTV
logger = StructuredLogger("TrainLTV")


def main():
    """
    Script responsável pelo treinamento do modelo de LTV (Lifetime Value),
    utilizando o modelo estatístico BG/NBD (Beta-Geometric / Negative Binomial Distribution).
    
    Pipeline de alto nível:
    1. Carregar dados da camada Gold (Feature Store)
    2. Preparar os dados no formato exigido pelo Lifetimes
    3. Treinar o modelo BG/NBD
    4. Estimar o número esperado de compras futuras
    5. Registrar experimento no MLflow
    """
    
    # Cria a SparkSession para leitura dos dados Delta
    spark = get_spark_session("TrainLTV")
    
    # -------------------------
    # 1. Leitura dos dados (Camada Gold)
    # -------------------------
    logger.info("Loading Gold data for LTV...")
    
    # Lê a tabela de features consolidadas por cliente
    df_spark = spark.read.format("delta").load(
        "data/gold/customer_features"
    )
    
    # Converte o DataFrame Spark para Pandas
    # OBS: Assume-se que o volume de dados cabe em memória
    # Em cenários de grande escala, alternativas incluem:
    # - Spark MLlib
    # - Feature Store externa (Feast, Hopsworks)
    # - Amostragem controlada para treino local
    df = df_spark.toPandas()
    
    # -------------------------
    # 2. Preparação dos dados para o Lifetimes
    # -------------------------
    # O modelo BG/NBD exige três variáveis principais:
    #
    # - frequency: número de compras repetidas
    # - recency: "idade" do cliente no momento da última compra
    # - T: idade total do cliente no período de observação
    #
    # ATENÇÃO:
    # - A coluna 'recency' da camada Gold NÃO é compatível com o Lifetimes
    # - Precisamos recalcular recency e T a partir das datas
    
    # Converte colunas de data para datetime
    df['signup_date'] = pd.to_datetime(df['signup_date'])
    df['last_purchase_date'] = pd.to_datetime(df['last_purchase_date'])
    
    # Define a data de referência do modelo
    # Em dados reais, poderia ser a data atual (today)
    # Aqui usamos a maior data encontrada no dataset
    current_date = df['last_purchase_date'].max()
    
    logger.info(f"Reference Date: {current_date}")
    
    # -------------------------
    # 3. Cálculo das variáveis do Lifetimes
    # -------------------------
    
    # T (Age): idade total do cliente
    # T = data de referência - data de cadastro
    df['T'] = (current_date - df['signup_date']).dt.days
    
    # lifetimes_recency: idade do cliente na última compra
    # lifetimes_recency = data da última compra - data de cadastro
    df['lifetimes_recency'] = (
        df['last_purchase_date'] - df['signup_date']
    ).dt.days
    
    # Para clientes que nunca compraram:
    # - frequency = 0
    # - lifetimes_recency deve ser 0 (exigência do modelo)
    df.loc[df['frequency'] == 0, 'lifetimes_recency'] = 0
    
    # -------------------------
    # 4. Validação dos dados
    # -------------------------
    # Remove registros inválidos:
    # - recency negativa (compra antes do cadastro)
    # - T negativo (datas inconsistentes)
    mask_valid = (df['lifetimes_recency'] >= 0) & (df['T'] >= 0)
    
    logger.info(
        f"Filtering {len(df) - mask_valid.sum()} invalid records "
        "(negative recency/age)"
    )
    
    df = df[mask_valid]
    
    # DataFrame final usado no treinamento
    data = df[['frequency', 'lifetimes_recency', 'T']].copy()
    
    # -------------------------
    # 5. Treinamento do modelo BG/NBD
    # -------------------------
    # Define o experimento no MLflow
    mlflow.set_experiment("ltv_prediction")
    
    # Inicia um novo run no MLflow
    with mlflow.start_run():
        
        logger.info("Training BetaGeoFitter...")
        
        # Instancia o modelo BG/NBD
        # penalizer_coef atua como regularização para evitar overfitting
        bgf = BetaGeoFitter(penalizer_coef=0.1)
        
        # Ajusta o modelo aos dados históricos
        bgf.fit(
            data['frequency'],
            data['lifetimes_recency'],
            data['T']
        )
        
        # Log do resumo estatístico do modelo
        logger.info(str(bgf))
        
        # -------------------------
        # 6. Previsão de compras futuras
        # -------------------------
        # Define horizonte de previsão (ex: próximos 90 dias)
        t = 90
        
        # Estima o número esperado de compras futuras por cliente
        df['predicted_purchases'] = bgf.conditional_expected_number_of_purchases_up_to_time(
            t,
            data['frequency'],
            data['lifetimes_recency'],
            data['T']
        )
        
        # -------------------------
        # 7. Análise de resultados
        # -------------------------
        # Exibe os 5 clientes com maior expectativa de compras futuras
        logger.info("Top 5 Customers by Predicted LTV:")
        
        top_5 = (
            df
            .sort_values('predicted_purchases', ascending=False)
            [['customer_id', 'predicted_purchases']]
            .head(5)
        )
        
        logger.info(
            "Top 5 Customers",
            data=top_5.to_dict(orient='records')
        )
        
        # -------------------------
        # 8. Registro no MLflow
        # -------------------------
        # Observação:
        # - O Lifetimes não é baseado em scikit-learn
        # - O log direto do modelo exige um custom flavor ou pickle
        # - Para simplicidade, registramos apenas parâmetros
        
        mlflow.log_param("penalizer_coef", 0.0)
        
        logger.info("LTV Model Trained successfully")


# Ponto de entrada do script
if __name__ == "__main__":
    main()

```

### 🛑 Checkpoint Final: Execução & Versionamento (v0.6.0)

Última etapa! Os dois modelos serão treinados e a entrega oficializada.

1.  **Executar Treinamento ML:**
    ```bash
    make train
    # ou: poetry run python src/ml/train_churn.py
    # (O LTV pode ser rodado com: poetry run python src/ml/train_ltv.py)
    ```
    *Verifique os logs de acurácia e o output do MLflow.*

    **Resultados Esperados (Exemplo):**
    ```text
    Resultados do Modelo:
    Acurácia: 71.90% (0.7190)
    AUC: 53.82% (0.5382)
    ```
    > **Nota:** O AUC está baixo (próximo de 0.5, que é aleatório), o que é esperado para dados sintéticos gerados aleatoriamente. Em dados reais, esperaríamos > 0.7.

    **Sobre os Avisos (Warnings):**
    *   **XGBoost:** O aviso sobre `use_label_encoder` é porque essa funcionalidade foi depreciada nas versões novas. O aviso sobre UBJSON é apenas informativo.
    *   **MLflow:** O aviso sobre "Model logged without a signature" é apenas uma sugestão para incluir schemas nos metadados, mas não afeta o funcionamento.

2.  **Commitar Mudanças:**
    ```bash
    git add .
    git commit -m "feat: implement ml models"
    ```
- Ao rodar make train, o MLflow criou a pasta mlruns/, e como foi usado git add ., todos esses arquivos de log (métricas, parâmetros, metadados) foram adicionados ao commit.

- É normal commitar a pasta mlruns?

- Projetos Pessoais/Estudo: Sim, é útil para manter o histórico dos seus experimentos junto com o código.
- Projetos em Equipe/Produção: Geralmente não. O MLflow seria configurado para salvar esses dados em um servidor remoto (banco de dados + S3) e adicionaríamos mlruns/ ao .gitignore.
- Como este é um projeto standalone de portfólio, não tem problema! 


3.  **Lançar Versão Final (v0.6.0):**
    ```bash
    make bump-minor
    ```

4.  **Sincronizar com Repositório Remoto:**
    ```bash
    git push origin main
    git push --tags
    ```

---

# Capítulo 7: CI/CD & Testes Automatizados (Fase 7)

Para elevar o nível de profissionalismo do projeto ("Roadmap Enterprise"), foi implementado um pipeline de **Integração Contínua (CI)**. Isso garante que nenhum código quebrado entre na branch `main`.

## 7.0 Configuração Inicial
Antes de criar os testes, precisamos preparar a estrutura de diretórios para o ambiente de testes e GitHub Actions.

**Ação:** Prepare os diretórios no terminal:
```bash
mkdir -p tests .github/workflows
touch tests/__init__.py
```

## 7.1 Smoke Testing (Teste de Fumaça)
Antes de rodar pipelines complexos, precisamos garantir que o Spark consegue iniciar e que as dependências estão corretas.

**Ação:** Crie o arquivo `tests/test_smoke.py` com o conteúdo abaixo:
**Objetivo:** Validar se o ambiente Spark/Delta está funcional em menos de 10 segundos.

```python
import pytest
from src.lib.spark_utils import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for testing."""
    # Cria uma sessão local para testes
    spark = get_spark_session("TestSession", local=True)
    yield spark
    spark.stop()

def test_spark_session_is_active(spark):
    """Smoke test: Validation of Spark Session."""
    data = [("ok", 1)]
    df = spark.createDataFrame(data, ["status", "id"])
    
    # Verifica escrita/leitura em memória
    assert df.count() == 1
    assert df.collect()[0]["status"] == "ok"
```

## 7.2 Teste de Integração (Lógica de Deduplicação)
Além de verificar se o Spark liga, precisamos verificar se a **lógica de negócio** (ETL) está correta.

**Ação:** Crie o arquivo `tests/test_silver.py` com o conteúdo abaixo:
**Objetivo:** Garantir que a deduplicação da camada Silver realmente funciona.

```python
import pytest
from pyspark.sql import Row
from src.lib.spark_utils import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for testing."""
    spark = get_spark_session("TestSilver", local=True)
    yield spark
    spark.stop()

def test_refine_customers_deduplication(spark, tmp_path):
    """
    Validation: Check if duplicate customers are correctly removed.
    Criteria: 
    - Input: 2 records with same customer_id.
    - Output: 1 unique record.
    """
    # 1. Setup Data with Duplicates
    data = [
        Row(customer_id="123", name="John Doe", signup_date="2023-01-01"),
        Row(customer_id="123", name="John Doe", signup_date="2023-01-01"), # Duplicate
        Row(customer_id="456", name="Jane Doe", signup_date="2023-02-01")
    ]
    df_raw = spark.createDataFrame(data)
    
    # 2. Execute Logic (Simulated)
    # Testamos a lógica isolada de dropDuplicates usada no job Silver
    df_dedup = df_raw.dropDuplicates(["customer_id"])
    
    # 3. Assertions
    assert df_dedup.count() == 2
    ids = [r.customer_id for r in df_dedup.collect()]
    assert "123" in ids
    assert "456" in ids
```

## 7.3 GitHub Actions Pipeline
Foi criado um workflow que executa automaticamente a cada `git push`.

**Ação:** Crie o arquivo `ci.yaml` dentro dele:
**Arquivo:** `.github/workflows/ci.yaml`

```yaml
name: CI Pipeline

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Set up Python 3.10
      uses: actions/setup-python@v4
      with:
        python-version: "3.10"

    - name: Install Java (for Spark)
      uses: actions/setup-java@v3
      with:
        distribution: 'temurin'
        java-version: '17'

    - name: Install Poetry
      run: |
        curl -sSL https://install.python-poetry.org | python3 -

    - name: Install Dependencies
      run: |
        poetry install

    - name: Run Smoke Tests
      run: |
        poetry run pytest tests/test_smoke.py
```

### 🛑 Checkpoint: Execução & Versionamento (v0.7.0)


1.  **Garanta a estrutura:**
    Verifique se as pastas `tests/` e `.github/workflows` foram criadas corretamente na etapa 7.0.

2.  **Validar Tests Localmente:**
    Antes de subir, teste na sua máquina.
    ```bash
    poetry run pytest tests/test_smoke.py
    ```
    *Resultado esperado: 1 passed in X.Xs*

3.  **Commitar Mudanças:**
    ```bash
    git add .
    git commit -m "feat: implement ci/cd pipeline"
    ```

4.  **Lançar Versão v0.7.0:**
    ```bash
    make bump-minor
    ```

5.  **Sincronizar com Repositório Remoto e Disparar CI:**
    Ao fazer o push, vá até a aba "Actions" no seu GitHub para ver o pipeline rodando.
    ```bash
    git push origin main
    git push --tags
    ```

---

# Capítulo 8: Próximos Passos (Roadmap)

Features planejadas para versões futuras:
1.  **Containerização (Docker):** Criar imagem otimizada para execução em Kubernetes.
2.  **Externalização (Pydantic):** Migrar configurações hardcoded para `config.yaml` validado.
