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

import requests
from datetime import datetime

API_URL = "https://cryptic-limes-rake.ngrok-free.dev/transactions?count=100"

print("Calling API...")

response = requests.get(API_URL, timeout=30)
response.raise_for_status()

transactions = response.json()

print(f"Received {len(transactions)} transactions")

batch_time = datetime.now().strftime("%Y%m%d_%H%M%S")

df = spark.createDataFrame(transactions)

output_path = f"Files/api_ingestion/transactions_batch_{batch_time}"

df.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Saved batch to {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
