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