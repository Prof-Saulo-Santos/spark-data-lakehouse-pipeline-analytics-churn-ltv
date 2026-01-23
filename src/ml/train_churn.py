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
    # Formula recomendada pelo XGBoost:
    # scale_pos_weight = (# negativos) / (# positivos)
    # Conversão explícita para int nativo (evita bug de JSON serialization)
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