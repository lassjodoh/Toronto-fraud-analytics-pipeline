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

# # Gold Layer Dimensional Model
# 
# This notebook creates analytics ready Gold tables from the Silver layer.
# 
# Gold layer objectives:
# - Build reusable dimension tables
# - Build a transaction fact table
# - Support fraud analytics reporting in Power BI
# - Create a simple star schema for business intelligence
# 


# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# LOAD SILVER TABLES

silver_customers = spark.table("silver_customers")
silver_merchants = spark.table("silver_merchants")
silver_cards = spark.table("silver_cards")
silver_transactions = spark.table("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD DIM LOCATION
# Purpose:
# Create a conformed location dimension from customer, merchant, and transaction location data.


customer_locations = (
    silver_customers
    .select(
        col("home_city").alias("city"),
        col("home_latitude").alias("latitude"),
        col("home_longitude").alias("longitude")
    )
)

merchant_locations = (
    silver_merchants
    .select(
        col("merchant_city").alias("city"),
        col("merchant_latitude").alias("latitude"),
        col("merchant_longitude").alias("longitude")
    )
)

transaction_locations = (
    silver_transactions
    .select(
        col("transaction_city").alias("city"),
        col("transaction_latitude").alias("latitude"),
        col("transaction_longitude").alias("longitude")
    )
)

gold_dim_location = (
    customer_locations
    .unionByName(merchant_locations)
    .unionByName(transaction_locations)
    .dropDuplicates(["city", "latitude", "longitude"])
    .withColumn(
        "location_id",
        row_number().over(Window.orderBy("city", "latitude", "longitude"))
    )
    .select(
        "location_id",
        "city",
        "latitude",
        "longitude"
    )
)

gold_dim_location.write.mode("overwrite").format("delta").saveAsTable("gold_dim_location")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD DIM CUSTOMER
# Purpose:
# Create customer dimension and reference customer home location through location_id.


gold_dim_customer = (
    silver_customers.alias("c")
    .join(
        gold_dim_location.alias("l"),
        (col("c.home_city") == col("l.city")) &
        (col("c.home_latitude") == col("l.latitude")) &
        (col("c.home_longitude") == col("l.longitude")),
        "left"
    )
    .select(
        col("c.customer_id"),
        col("c.customer_name"),
        col("c.customer_since_date"),
        col("c.spending_profile"),
        col("c.avg_transaction_amount"),
        col("c.daily_limit"),
        col("c.customer_risk_level"),
        col("l.location_id").alias("customer_location_id")
    )
    .dropDuplicates(["customer_id"])
)

gold_dim_customer.write.mode("overwrite").format("delta").saveAsTable("gold_dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# GOLD DIM MERCHANT
# Purpose:
# Create merchant dimension and reference merchant location through location_id.


gold_dim_merchant = (
    silver_merchants.alias("m")
    .join(
        gold_dim_location.alias("l"),
        (col("m.merchant_city") == col("l.city")) &
        (col("m.merchant_latitude") == col("l.latitude")) &
        (col("m.merchant_longitude") == col("l.longitude")),
        "left"
    )
    .select(
        col("m.merchant_id"),
        col("m.merchant_name"),
        col("m.merchant_category"),
        col("m.merchant_risk_level"),
        col("l.location_id").alias("merchant_location_id")
    )
    .dropDuplicates(["merchant_id"])
)

gold_dim_merchant.write.mode("overwrite").format("delta").saveAsTable("gold_dim_merchant")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD DIM CARD

gold_dim_card = (
    silver_cards
    .select(
        "card_id",
        "customer_id",
        "card_type",
        "card_network",
        "issue_date",
        "expiry_date",
        "card_status",
        "card_limit"
    )
    .dropDuplicates(["card_id"])
)

gold_dim_card.write.mode("overwrite").format("delta").saveAsTable("gold_dim_card")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD DIM DATE
# Purpose:
# This is a standalone calendar dimension independent of transactional data.

start_date = date(2026, 1, 1)
end_date = date(2026, 12, 31)

date_list = [
    (start_date + timedelta(days=i),)
    for i in range((end_date - start_date).days + 1)
]

gold_dim_date = spark.createDataFrame(
    date_list,
    ["date"]
)

gold_dim_date = (
    gold_dim_date
    .withColumn(
        "date_key",
        date_format(col("date"), "yyyyMMdd").cast("int")
    )
    .withColumn("year", year(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn("month_number", month(col("date")))
    .withColumn("month_name", date_format(col("date"), "MMMM"))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("day_name", date_format(col("date"), "EEEE"))
    .withColumn(
        "is_weekend",
        when(dayofweek(col("date")).isin(1, 7), 1).otherwise(0)
    )
)

gold_dim_date.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_format(col("transaction_date"), "yyyyMMdd").cast("int").alias("date_key")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD FACT TRANSACTIONS
# Purpose:
# Create the transaction fact table with foreign keys
# to date, customer, card, merchant, and location dimensions.

gold_fact_transactions = (
    silver_transactions.alias("t")
    .join(
        gold_dim_location.alias("l"),
        (col("t.transaction_city") == col("l.city")) &
        (col("t.transaction_latitude") == col("l.latitude")) &
        (col("t.transaction_longitude") == col("l.longitude")),
        "left"
    )
    .select(
        col("t.transaction_id"),
        date_format(col("t.transaction_date"), "yyyyMMdd").cast("int").alias("date_key"),
        col("t.customer_id"),
        col("t.card_id"),
        col("t.merchant_id"),
        col("l.location_id").alias("transaction_location_id"),
        col("t.batch_id"),
        col("t.transaction_timestamp"),
        col("t.transaction_amount"),
        col("t.transaction_status"),
        col("t.authorization_reason"),
        col("t.risk_signal"),
        col("t.fraud_scenario_flag"),
        col("t.fraud_scenario_type"),
        col("t.transaction_hour"),
        col("t.generated_at")
    )
)

gold_fact_transactions.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fact_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Gold Fact Transactions:", spark.table("gold_fact_transactions").count())

display(spark.table("gold_fact_transactions").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD LAYER RECORD COUNTS


print("Gold Dim Location:", spark.table("gold_dim_location").count())
print("Gold Dim Customer:", spark.table("gold_dim_customer").count())
print("Gold Dim Merchant:", spark.table("gold_dim_merchant").count())
print("Gold Dim Card:", spark.table("gold_dim_card").count())
print("Gold Dim Date:", spark.table("gold_dim_date").count())
print("Gold Fact Transactions:", spark.table("gold_fact_transactions").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT COUNT(*) AS missing_customer_location
FROM gold_dim_customer
WHERE customer_location_id IS NULL
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT
    COUNT(*) AS invalid_date_keys
FROM gold_fact_transactions f
LEFT JOIN gold_dim_date d
ON f.date_key = d.date_key
WHERE d.date_key IS NULL
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT
    fraud_scenario_flag,
    COUNT(*) AS transactions
FROM gold_fact_transactions
GROUP BY fraud_scenario_flag
ORDER BY fraud_scenario_flag
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT
    MIN(transaction_amount) AS min_amount,
    MAX(transaction_amount) AS max_amount,
    AVG(transaction_amount) AS avg_amount
FROM gold_fact_transactions
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Layer Dimensional Model
# 
# This notebook transforms cleansed Silver layer data into an analytics-ready dimensional model optimized for reporting, fraud monitoring, and business intelligence.
# 
# Dimension Tables:
# - gold_dim_customer
# - gold_dim_merchant
# - gold_dim_card
# - gold_dim_location
# - gold_dim_date
# 
# Fact Tables:
# - gold_fact_transactions
# 
# Key Design Decisions:
# • Implemented a conformed location dimension to eliminate duplicate geographic attributes across customers, merchants, and transactions
# • Generated an independent calendar dimension to support complete time intelligence and future reporting periods
# • Established surrogate keys and foreign key relationships to support a scalable star schema
# • Preserved transaction level granularity within the fact table for detailed fraud analysis
# 
# Business Value:
# The Gold layer provides a trusted analytical data model that enables fraud trend analysis, risk monitoring, transaction performance reporting, customer behavior analysis, geographic insights, and executive dashboarding within Power BI.

