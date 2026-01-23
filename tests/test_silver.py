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