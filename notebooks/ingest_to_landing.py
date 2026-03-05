dbutils.widgets.text("source_name", "ext_criliti_sc")
dbutils.widgets.text("subfolder", "iw/ext_criliti_sc")
dbutils.widgets.text("file_format", "csv")

source_name = dbutils.widgets.get("source_name")
subfolder = dbutils.widgets.get("subfolder")
file_format = dbutils.widgets.get("file_format")

VOLUME_ROOT = "/Volumes/edbi_teamg01"
SOURCE_PATH = f"{VOLUME_ROOT}/landing/{subfolder}/"

from pyspark.sql.functions import col, current_timestamp, regexp_extract

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", file_format)
    .option("cloudFiles.schemaLocation", f"{SOURCE_PATH}_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(SOURCE_PATH)
)

df = df.withColumn("_source_file", col("_metadata.file_path")).withColumn("_ingested_at", current_timestamp()).withColumn("_file_date", regexp_extract(col("_metadata.file_path"), r"(\d{8})\.csv", 1))

(
    df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{SOURCE_PATH}_checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"edbi_teamg01.landing.{source_name}")
)
