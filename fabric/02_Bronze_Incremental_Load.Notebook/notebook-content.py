# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a9b60b3b-0d1b-46a4-bd8c-799674a7c644",
# META       "default_lakehouse_name": "TorontoFraudLakehouse",
# META       "default_lakehouse_workspace_id": "4a0e3a62-1a2f-4fce-a940-d27895ac93a9",
# META       "known_lakehouses": [
# META         {
# META           "id": "a9b60b3b-0d1b-46a4-bd8c-799674a7c644"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import input_file_name, current_timestamp, lit, col, to_timestamp
from pyspark.sql.types import StringType, DoubleType, IntegerType

landing_folder = "Files/api_ingestion"
archive_folder = "Files/archive/api_ingestion"
bronze_table = "bronze_transactions"

mssparkutils.fs.mkdirs(archive_folder)

items = mssparkutils.fs.ls(landing_folder)

batch_items = [
    item for item in items
    if item.name.startswith("transactions_batch")
]

if len(batch_items) == 0:
    print("No new files found for bronze load.")

else:
    for item in batch_items:

        print(f"Processing: {item.path}")

        df = (
            spark.read
            .option("header", "true")
            .csv(item.path)
        )

        df = (
            df
            .withColumn("transaction_id", col("transaction_id").cast(StringType()))
            .withColumn("batch_id", col("batch_id").cast(StringType()))
            .withColumn("customer_id", col("customer_id").cast(StringType()))
            .withColumn("card_id", col("card_id").cast(StringType()))
            .withColumn("merchant_id", col("merchant_id").cast(StringType()))
            .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
            .withColumn("transaction_amount", col("transaction_amount").cast(DoubleType()))
            .withColumn("transaction_city", col("transaction_city").cast(StringType()))
            .withColumn("transaction_latitude", col("transaction_latitude").cast(DoubleType()))
            .withColumn("transaction_longitude", col("transaction_longitude").cast(DoubleType()))
            .withColumn("transaction_status", col("transaction_status").cast(StringType()))
            .withColumn("authorization_reason", col("authorization_reason").cast(StringType()))
            .withColumn("risk_signal", col("risk_signal").cast(StringType()))
            .withColumn("fraud_scenario_flag", col("fraud_scenario_flag").cast(IntegerType()))
            .withColumn("fraud_scenario_type", lit(None).cast(StringType()))
            .withColumn("generated_at", to_timestamp(col("generated_at")))
        )

        df = (
            df
            .withColumn("source_file", input_file_name())
            .withColumn("bronze_loaded_at", current_timestamp())
            .withColumn("batch_folder", lit(item.name))
        )

        df.write \
          .mode("append") \
          .format("delta") \
          .option("mergeSchema", "true") \
          .saveAsTable(bronze_table)

        archive_path = f"{archive_folder}/{item.name}"

        mssparkutils.fs.mv(item.path, archive_path)

        print(f"Archived: {item.name}")

    print(f"Bronze incremental load completed. Processed {len(batch_items)} batch folder(s).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("bronze_transactions"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.table("bronze_transactions")

print(f"Bronze Row Count: {bronze_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql("""
        SELECT
            batch_folder,
            COUNT(*) AS transaction_count
        FROM bronze_transactions
        GROUP BY batch_folder
        ORDER BY batch_folder DESC
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
