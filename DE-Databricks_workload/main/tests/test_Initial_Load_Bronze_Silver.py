import pytest
from pyspark.sql import SparkSession
from ecom_data_load import create_spark_session, create_delta_table, run_data_load

@pytest.fixture(scope="session")
def spark_session():
    spark = create_spark_session()
    yield spark
    spark.stop()

@pytest.fixture
def bronze_path(tmp_path):
    return tmp_path / "bronze"

@pytest.fixture
def silver_path(tmp_path):
    return tmp_path / "silver"

@pytest.fixture
def silver_checkpoint_path(tmp_path):
    return tmp_path / "checkpoint"

def test_create_spark_session(spark_session):
    assert spark_session is not None, "SparkSession creation failed."

def test_create_delta_table(spark_session, silver_path):
    test_data = [
        # Test data rows
    ]
    schema = # Define the schema for the test data
    
    # Create a DataFrame with the test data
    test_df = spark_session.createDataFrame(test_data, schema=schema)
    
    # Call create_delta_table function
    table_name = "test_table"
    primary_key = "id"  # Replace with the primary key of the test table
    create_delta_table(test_df, table_name, primary_key)
    
    # Check if the Delta table is created and its content
    delta_table = spark_session.read.format("delta").load(f"{silver_path}/{table_name}")
    assert delta_table.count() == len(test_data), "Delta table creation failed."
    # Perform additional assertions if needed

def test_run_data_load(spark_session, bronze_path, silver_path, silver_checkpoint_path):
    # Assuming you have already prepared test data for each table in Bronze layer
    
    # Call run_data_load function
    SilverPath = str(silver_path)
    SilverCheckpointPath = str(silver_checkpoint_path)
    BronzePath = str(bronze_path)
    run_data_load()
    
    # Check if the Delta tables are created and their content
    # Use spark_session.read.format("delta").load(path) to check Delta tables
    # You can also perform additional assertions if needed


