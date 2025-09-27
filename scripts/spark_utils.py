from pyspark.sql import SparkSession

def create_spark_session(name: str = 'DataEnrichmentProcess') -> SparkSession:
    """Initializes and returns a SparkSession."""
    return (
        SparkSession.builder
        .appName(name)
        .getOrCreate()
    )