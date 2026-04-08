from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, md5, concat_ws, sha2

# configuration 
SOURCE_PATH = "/Volumes/transportation/routeflow_schema/data_store/city"

@dp.materialized_view(
    name="transportation.bronze.city",
    comment="City Raw Data Processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = (
        spark.read.format("csv")
                .option("header", True) 
                .option("inferschema", True) 
                .option("mode", "PERMISSIVE") 
                .option("mergeSchema", True) 
                .option("columnNameOfCorruptRecord", "_corrupt_record") 
                .load(SOURCE_PATH)
    )

    df = df.withColumn("file_name", col("_metadata.file_path")) \
                .withColumn("ingest_datetime", current_timestamp())

    return df