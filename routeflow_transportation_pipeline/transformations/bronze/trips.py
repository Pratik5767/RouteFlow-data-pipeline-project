from pyspark import pipelines as dp
import pyspark.sql.functions as F

# configuration 
SOURCE_PATH = "/Volumes/transportation/routeflow_schema/data_store/trips"
SCHEMA_PATH = "/Volumes/transportation/routeflow_schema/data_store/_schema"

@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming ingestion of raw orders data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def trips_bronze():
    df = (
        spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv") 
                .option("cloudFiles.inferColumnTypes", "true") 
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .option("cloudFiles.maxFilesPerTrigger", 100)
                .option("cloudFiles.schemaLocation", SCHEMA_PATH)
                .load(SOURCE_PATH)
    )

    df = df.withColumnRenamed("distance_travelled(km)", "distance_travelled_km")

    df = df.withColumn("file_name", F.col("_metadata.file_path")) \
            .withColumn("ingest_datetime", F.current_timestamp())

    return df