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

# MARKDOWN ********************

# # Silver Layer Processing
# 
# This notebook cleans and standardizes the Bronze fraud analytics tables.
# 
# Silver layer objectives:
# - Remove duplicate records
# - Standardize text fields
# - Convert dates and timestamps
# - Validate numeric fields
# - Add audit columns
# - Prepare clean tables for Gold dimensional modeling

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Purpose:
# Read raw Bronze Delta tables into Spark DataFrames for cleaning and standardization.

# CELL ********************

customers_bronze = spark.table("bronze_customers")
merchants_bronze = spark.table("bronze_merchants")
cards_bronze = spark.table("bronze_cards")
transactions_bronze = spark.table("bronze_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_bronze.printSchema()
merchants_bronze.printSchema()
cards_bronze.printSchema()
transactions_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# SILVER CUSTOMERS


silver_customers = (
    customers_bronze
    .dropDuplicates(["customer_id"])
    .withColumn("customer_name", initcap(trim(col("customer_name"))))
    .withColumn("home_city", initcap(trim(col("home_city"))))
    .withColumn("spending_profile", initcap(trim(col("spending_profile"))))
    .withColumn("customer_risk_level", initcap(trim(col("customer_risk_level"))))
    .withColumn("avg_transaction_amount", round(col("avg_transaction_amount"), 2))
    .withColumn("ingestion_timestamp", current_timestamp())
)

silver_customers.write.mode("overwrite").format("delta").saveAsTable("silver_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SILVER MERCHANTS


silver_merchants = (
    merchants_bronze
    .dropDuplicates(["merchant_id"])
    .withColumn("merchant_name", initcap(trim(col("merchant_name"))))
    .withColumn("merchant_category", initcap(trim(col("merchant_category"))))
    .withColumn("merchant_city", initcap(trim(col("merchant_city"))))
    .withColumn("merchant_risk_level", initcap(trim(col("merchant_risk_level"))))
    .withColumn("ingestion_timestamp", current_timestamp())
)

silver_merchants.write.mode("overwrite").format("delta").saveAsTable("silver_merchants")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SILVER CARDS


silver_cards = (
    cards_bronze
    .dropDuplicates(["card_id"])
    .withColumn("card_type", initcap(trim(col("card_type"))))
    .withColumn("card_network", initcap(trim(col("card_network"))))
    .withColumn("card_status", initcap(trim(col("card_status"))))
    .withColumn("ingestion_timestamp", current_timestamp())
)

silver_cards.write.mode("overwrite").format("delta").saveAsTable("silver_cards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SILVER TRANSACTIONS


silver_transactions = (
    transactions_bronze
    .drop("source_file", "bronze_loaded_at", "batch_folder")
    .dropDuplicates(["transaction_id"])
    .filter(col("transaction_amount") > 0)
    .withColumn("transaction_city", initcap(trim(col("transaction_city"))))
    .withColumn("transaction_status", initcap(trim(col("transaction_status"))))
    .withColumn("authorization_reason", initcap(trim(col("authorization_reason"))))
    .withColumn("risk_signal", initcap(trim(col("risk_signal"))))
    .withColumn("fraud_scenario_type", initcap(trim(col("fraud_scenario_type"))))
    .withColumn("transaction_date", to_date(col("transaction_timestamp")))
    .withColumn("transaction_year", year(col("transaction_timestamp")))
    .withColumn("transaction_month", month(col("transaction_timestamp")))
    .withColumn("transaction_day", dayofmonth(col("transaction_timestamp")))
    .withColumn("transaction_hour", hour(col("transaction_timestamp")))
    .withColumn("transaction_amount", round(col("transaction_amount"), 2))
    .withColumn("ingestion_timestamp", current_timestamp())
)

silver_transactions.write.mode("overwrite").format("delta").saveAsTable("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SILVER VALIDATION


print("Silver Customers:", spark.table("silver_customers").count())
print("Silver Merchants:", spark.table("silver_merchants").count())
print("Silver Cards:", spark.table("silver_cards").count())
print("Silver Transactions:", spark.table("silver_transactions").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #####
#  Silver Layer Processing
# 
# This notebook transforms raw Bronze tables into cleansed and standardized Silver tables for downstream analytics and dimensional modeling.
# 
# Transformation activities include:
# 
# • Removal of duplicate records using business keys
# • Standardization of text fields through trimming and capitalization
# • Validation of transaction amounts to exclude invalid records
# • Creation of transaction date attributes for analytical reporting
# • Addition of ingestion timestamps for auditability and data lineage
# • Preservation of data quality and consistency across customer, merchant, card, and transaction datasets
# 
# Source Tables:
# - bronze_customers
# - bronze_merchants
# - bronze_cards
# - bronze_transactions
# 
# Output Tables:
# - silver_customers
# - silver_merchants
# - silver_cards
# - silver_transactions
# 
# The Silver layer serves as the trusted, business-ready dataset that supports Gold layer dimensional models, fraud analytics reporting, and Power BI dashboards.
