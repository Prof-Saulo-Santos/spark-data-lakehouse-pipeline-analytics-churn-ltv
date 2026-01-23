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