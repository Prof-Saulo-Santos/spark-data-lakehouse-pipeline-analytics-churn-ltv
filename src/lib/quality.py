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