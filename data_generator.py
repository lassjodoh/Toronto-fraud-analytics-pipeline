import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_CA")

random.seed(42)
Faker.seed(42)

GTA_LOCATIONS = [
    ("Toronto", 43.6532, -79.3832),
    ("Mississauga", 43.5890, -79.6441),
    ("Brampton", 43.7315, -79.7624),
    ("Vaughan", 43.8361, -79.4985),
    ("Markham", 43.8561, -79.3370),
    ("Richmond Hill", 43.8828, -79.4403),
    ("Pickering", 43.8384, -79.0868),
    ("Ajax", 43.8509, -79.0204),
    ("Oakville", 43.4675, -79.6877),
    ("Burlington", 43.3255, -79.7990)
]

def generate_customers(num_customers=1000):
    customers = []

    for i in range(1, num_customers + 1):
        city, latitude, longitude = random.choice(GTA_LOCATIONS)

        spending_profile = random.choices(
            ["Low", "Medium", "High", "Premium"],
            weights=[40, 35, 20, 5],
            k=1
        )[0]

        if spending_profile == "Low":
            avg_transaction_amount = round(random.uniform(20, 100), 2)
            daily_limit = random.randint(1000, 3000)

        elif spending_profile == "Medium":
            avg_transaction_amount = round(random.uniform(100, 500), 2)
            daily_limit = random.randint(3000, 7000)

        elif spending_profile == "High":
            avg_transaction_amount = round(random.uniform(500, 1500), 2)
            daily_limit = random.randint(7000, 15000)

        else:
            avg_transaction_amount = round(random.uniform(1500, 5000), 2)
            daily_limit = random.randint(15000, 50000)

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "customer_name": fake.name(),
            "home_city": city,
            "home_latitude": latitude,
            "home_longitude": longitude,
            "customer_since_date": fake.date_between(start_date="-10y", end_date="today"),
            "spending_profile": spending_profile,
            "avg_transaction_amount": avg_transaction_amount,
            "daily_limit": daily_limit,
            "customer_risk_level": random.choice(["Low", "Medium", "High"])
        })

    return pd.DataFrame(customers)


customers_df = generate_customers(1000)

customers_df.to_csv("datasets\customers.csv", index=False)

print("customers.csv created successfully")
print(customers_df.head())


## Merchants

MERCHANTS = [
    ("Walmart", "Grocery"),
    ("Costco", "Grocery"),
    ("Loblaws", "Grocery"),
    ("No Frills", "Grocery"),
    ("Metro", "Grocery"),
    ("FreshCo", "Grocery"),

    ("Shoppers Drug Mart", "Pharmacy"),
    ("Rexall", "Pharmacy"),

    ("Best Buy", "Electronics"),
    ("Apple Store", "Electronics"),
    ("Staples", "Electronics"),

    ("Canadian Tire", "Retail"),
    ("Winners", "Retail"),
    ("Home Depot", "Retail"),
    ("IKEA", "Retail"),

    ("Amazon", "Online Retail"),
    ("eBay", "Online Retail"),

    ("Uber", "Transportation"),
    ("Uber Eats", "Food Delivery"),

    ("Tim Hortons", "Restaurant"),
    ("Starbucks", "Restaurant"),
    ("McDonald's", "Restaurant"),
    ("Subway", "Restaurant"),
    ("KFC", "Restaurant"),
    ("Pizza Pizza", "Restaurant"),

    ("Shell", "Gas Station"),
    ("Petro Canada", "Gas Station"),
    ("Esso", "Gas Station"),

    ("Air Canada", "Travel"),
    ("WestJet", "Travel"),

    ("Cineplex", "Entertainment"),
    ("Ticketmaster", "Entertainment"),

    ("Marriott", "Hotel"),
    ("Hilton", "Hotel")
]


def generate_merchants():
    merchants = []

    for i, (merchant_name, merchant_category) in enumerate(MERCHANTS, start=1):

        city, latitude, longitude = random.choice(GTA_LOCATIONS)

        merchants.append({
            "merchant_id": f"MERCH{i:04d}",
            "merchant_name": merchant_name,
            "merchant_category": merchant_category,
            "merchant_city": city,
            "merchant_latitude": latitude,
            "merchant_longitude": longitude,
            "merchant_risk_level": random.choices(
                ["Low", "Medium", "High"],
                weights=[75, 20, 5],
                k=1
            )[0]
        })

    return pd.DataFrame(merchants)


merchants_df = generate_merchants()

merchants_df.to_csv("datasets/merchants.csv", index=False)

print("Merchants saved successfully")
print(merchants_df.head())


### cards

from datetime import timedelta

def generate_card_number():
    return "".join([str(random.randint(0, 9)) for _ in range(16)])


def generate_cards(customers_df):
    cards = []
    card_counter = 1

    for _, customer in customers_df.iterrows():
        customer_id = customer["customer_id"]
        spending_profile = customer["spending_profile"]

        # Every customer gets one debit card
        card_types = ["Debit"]

        # Some customers also get one credit card
        if random.random() < 0.70:
            card_types.append("Credit")

        for card_type in card_types:
            issue_date = fake.date_between(
                start_date=customer["customer_since_date"],
                end_date="today"
            )

            expiry_date = issue_date + timedelta(days=365 * 5)

            if card_type == "Debit":
                card_limit = customer["daily_limit"]
            else:
                if spending_profile == "Low":
                    card_limit = random.choice([1000, 1500, 2000, 3000])
                elif spending_profile == "Medium":
                    card_limit = random.choice([4000, 5000, 7000, 10000])
                elif spending_profile == "High":
                    card_limit = random.choice([12000, 15000, 20000])
                else:
                    card_limit = random.choice([25000, 35000, 50000])

            cards.append({
                "card_id": f"CARD{card_counter:06d}",
                "customer_id": customer_id,
                "card_type": card_type,
                "card_number": generate_card_number(),
                "card_network": random.choice(["Visa", "Mastercard"]),
                "issue_date": issue_date,
                "expiry_date": expiry_date,
                "card_status": random.choices(
                    ["Active", "Blocked", "Expired"],
                    weights=[90, 7, 3],
                    k=1
                )[0],
                "card_limit": card_limit
            })

            card_counter += 1

    return pd.DataFrame(cards)

cards_df = generate_cards(customers_df)

cards_df.to_csv("datasets/cards.csv", index=False)

print("Cards saved successfully")
print(cards_df.head())


print(len(customers_df))
print(len(cards_df))
print(len(merchants_df))

print(cards_df["customer_id"].nunique())


### Transactions for batch streaming

START_DATE = "2026-01-01"
END_DATE = "2026-03-31"
TOTAL_TRANSACTIONS = 150000

FRAUD_TARGET_RATE = 0.10
FRAUD_HOTSPOT_CITY = "Brampton"

customers_df = pd.read_csv("datasets/customers.csv")
cards_df = pd.read_csv("datasets/cards.csv")
merchants_df = pd.read_csv("datasets/merchants.csv")

random.seed(42)


def random_timestamp():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    total_seconds = int((end - start).total_seconds())
    random_seconds = random.randint(0, total_seconds)

    return start + timedelta(seconds=random_seconds)


def generate_transaction_amount(avg_transaction_amount, fraud_flag):
    if fraud_flag:
        multiplier = random.choice([8, 10, 12, 15, 20])
    else:
        multiplier = random.choices(
            [0.5, 1, 1.5, 2, 3],
            weights=[25, 40, 20, 10, 5],
            k=1
        )[0]

    return round(max(avg_transaction_amount * multiplier, 1), 2)


def choose_fraud_scenario():
    return random.choices(
        [
            "Abnormal Spend Amount",
            "High Risk Merchant",
            "Restricted Merchant",
            "Unusual Transaction Time",
            "Fraud Hotspot Area"
        ],
        weights=[40, 20, 15, 10, 15],
        k=1
    )[0]


def authorize_transaction(card, merchant, customer, amount):
    if card["card_status"] == "Blocked":
        return "Declined", "Blocked Card", "High"

    if card["card_status"] == "Stolen":
        return "Declined", "Stolen Card", "High"

    if card["card_status"] == "Expired":
        return "Declined", "Expired Card", "High"

    if merchant["merchant_risk_level"] == "Blocked":
        return "Declined", "Blocked Merchant", "High"

    if merchant["merchant_risk_level"] == "Restricted":
        return "Declined", "Restricted Merchant", "High"

    if amount > customer["daily_limit"]:
        return "Declined", "Daily Limit Exceeded", "Medium"

    return "Approved", "Normal Transaction Pattern", "Low"


def generate_transactions():
    transactions = []

    for i in range(1, TOTAL_TRANSACTIONS + 1):
        fraud_flag = random.random() < FRAUD_TARGET_RATE

        customer = customers_df.sample(1).iloc[0]

        customer_cards = cards_df[
            cards_df["customer_id"] == customer["customer_id"]
        ]

        card = customer_cards.sample(1).iloc[0]

        if fraud_flag and random.random() < 0.70:
            hotspot_merchants = merchants_df[
                merchants_df["merchant_city"] == FRAUD_HOTSPOT_CITY
            ]

            merchant = hotspot_merchants.sample(1).iloc[0]
        else:
            merchant = merchants_df.sample(1).iloc[0]

        fraud_scenario_type = (
            choose_fraud_scenario()
            if fraud_flag
            else "Normal Transaction"
        )

        timestamp = random_timestamp()

        if fraud_scenario_type == "Unusual Transaction Time":
            timestamp = timestamp.replace(
                hour=random.choice([0, 1, 2, 3, 4]),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

        amount = generate_transaction_amount(
            customer["avg_transaction_amount"],
            fraud_flag
        )

        status, reason, risk_signal = authorize_transaction(
            card,
            merchant,
            customer,
            amount
        )

        if fraud_scenario_type == "Abnormal Spend Amount":
            reason = "Amount Above Customer Average"
            risk_signal = "High"

        elif fraud_scenario_type == "High Risk Merchant":
            reason = "High Risk Merchant"
            risk_signal = "High"

        elif fraud_scenario_type == "Restricted Merchant":
            status = "Declined"
            reason = "Restricted Merchant"
            risk_signal = "High"

        elif fraud_scenario_type == "Fraud Hotspot Area":
            reason = "Fraud Hotspot Area"
            risk_signal = "High"

        transactions.append({
            "transaction_id": f"TXN{i:07d}",
            "batch_id": "BATCH_Q1_2026",
            "customer_id": customer["customer_id"],
            "card_id": card["card_id"],
            "merchant_id": merchant["merchant_id"],
            "transaction_timestamp": timestamp,
            "transaction_amount": amount,
            "transaction_city": merchant["merchant_city"],
            "transaction_latitude": merchant["merchant_latitude"],
            "transaction_longitude": merchant["merchant_longitude"],
            "transaction_status": status,
            "authorization_reason": reason,
            "risk_signal": risk_signal,
            "fraud_scenario_flag": 1 if fraud_flag else 0,
            "fraud_scenario_type": fraud_scenario_type,
            "generated_at": datetime.now()
        })

    return pd.DataFrame(transactions)


transactions_df = generate_transactions()

transactions_df.to_csv(
    "datasets/transactions_jan_mar_2026.csv",
    index=False
)

print("transactions_jan_mar_2026.csv created successfully")
print(f"Rows: {len(transactions_df)}")
print(transactions_df["fraud_scenario_flag"].value_counts())
print(transactions_df["transaction_status"].value_counts())
print(transactions_df["authorization_reason"].value_counts())