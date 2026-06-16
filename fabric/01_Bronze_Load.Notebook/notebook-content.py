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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file in mssparkutils.fs.ls("Files"):
    print(file.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BRONZE LAYER
# ###### Purpose:
# ###### Load raw customer data from the Files directory and store it as a Delta table in the Bronze layer. No transformations are applied at this stage.

# CELL ********************

customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/customers.csv")
)

customers_df.write.mode("overwrite").format("delta").saveAsTable("bronze_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Load raw merchant reference data and persist it as a Bronze Delta table.

# CELL ********************

merchants_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/merchants.csv")
)

merchants_df.write.mode("overwrite").format("delta").saveAsTable("bronze_merchants")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Load customer card information and store it as a Bronze Delta table for downstream processing.

# CELL ********************

cards_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/cards.csv")
)

cards_df.write.mode("overwrite").format("delta").saveAsTable("bronze_cards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Load raw transaction records generated from the fraud analytics API simulation. Data is stored exactly as received.

# CELL ********************

transactions_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/transactions_jan_mar_2026.csv")
)

transactions_df.write.mode("overwrite").format("delta").saveAsTable("bronze_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Verify that all Bronze tables were created, successfully and review record counts.

# CELL ********************

print("Customers:", spark.table("bronze_customers").count())
print("Merchants:", spark.table("bronze_merchants").count())
print("Cards:", spark.table("bronze_cards").count())
print("Transactions:", spark.table("bronze_transactions").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bronze Layer Processing
# 
# This notebook ingests raw fraud analytics data files from the Lakehouse Files area and stores them as Delta tables in the Bronze layer.
# 
# Source Files:
# - customers.csv
# - merchants.csv
# - cards.csv
# - transactions_jan_mar_2026.csv
# 
# Output Tables:
# - bronze_customers
# - bronze_merchants
# - bronze_cards
# - bronze_transactions
# 
# No business rules or transformations are applied at this stage. The objective is to preserve source-system fidelity and establish a reliable foundation for downstream Silver and Gold transformations.

# CELL ********************

spark.table("bronze_transactions").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
