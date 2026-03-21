from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_timestamp

VOLUME_ROOT = "/Volumes/edbi_teamg01/landing"
CATALOG = "edbi_teamg01"


metadata = spark.table(f"{CATALOG}.config.metadata_tables").filter(col("active")).collect()

for row in metadata:
    source_name = row["source_name"]
    subfolder = row["subfolder"]
    file_format = row["file_format"]
    date_pattern = row["date_pattern"]
    source_path = f"{VOLUME_ROOT}/{subfolder}"
    target_table = f"{CATALOG}.landing.{source_name}"

    print(f"Ingesting {source_name} from {subfolder} into {target_table}")

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{source_path}_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    )

    df = (
        df.withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn(
            "_file_date",
            to_timestamp(regexp_extract(col("_metadata.file_path"), date_pattern, 1), "yyyyMMddHHmmss"),
        )
    )

    df.writeStream.format("delta").outputMode("append").option("checkpointLocation", f"{source_path}_checkpoint").option("mergeSchema", "true").trigger(availableNow=True).toTable(target_table)

    print(f"{source_name} done")
