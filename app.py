from flask import Flask, jsonify, request
import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

app = Flask(__name__)

customers_df = pd.read_csv("datasets/customers.csv")
cards_df = pd.read_csv("datasets/cards.csv")
merchants_df = pd.read_csv("datasets/merchants.csv")

START_DATE = "2026-04-01"
END_DATE = "2026-04-30"
FRAUD_TARGET_RATE = 0.10
FRAUD_HOTSPOT_CITY = "Brampton"


def random_timestamp():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def generate_amount(avg_amount, fraud_flag):
    multiplier = random.choice([8, 10, 12, 15, 20]) if fraud_flag else random.choice([0.5, 1, 1.5, 2, 3])
    return round(max(avg_amount * multiplier, 1), 2)


def authorize(card, merchant, customer, amount):
    if card["card_status"] in ["Blocked", "Stolen", "Expired"]:
        return "Declined", f"{card['card_status']} Card", "High"

    if merchant["merchant_risk_level"] in ["Blocked", "Restricted"]:
        return "Declined", f"{merchant['merchant_risk_level']} Merchant", "High"

    if amount > customer["daily_limit"]:
        return "Declined", "Daily Limit Exceeded", "Medium"

    return "Approved", "Normal Transaction Pattern", "Low"


def generate_transaction():
    fraud_flag = random.random() < FRAUD_TARGET_RATE

    customer = customers_df.sample(1).iloc[0]
    customer_cards = cards_df[cards_df["customer_id"] == customer["customer_id"]]
    card = customer_cards.sample(1).iloc[0]

    if fraud_flag and random.random() < 0.70:
        merchant = merchants_df[merchants_df["merchant_city"] == FRAUD_HOTSPOT_CITY].sample(1).iloc[0]
    else:
        merchant = merchants_df.sample(1).iloc[0]

    amount = generate_amount(customer["avg_transaction_amount"], fraud_flag)
    status, reason, risk_signal = authorize(card, merchant, customer, amount)

    return {
        "transaction_id": f"TXN-{uuid.uuid4()}",
        "batch_id": f"API_BATCH_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "customer_id": customer["customer_id"],
        "card_id": card["card_id"],
        "merchant_id": merchant["merchant_id"],
        "transaction_timestamp": random_timestamp().isoformat(),
        "transaction_amount": amount,
        "transaction_city": merchant["merchant_city"],
        "transaction_latitude": merchant["merchant_latitude"],
        "transaction_longitude": merchant["merchant_longitude"],
        "transaction_status": status,
        "authorization_reason": reason,
        "risk_signal": risk_signal,
        "fraud_scenario_flag": 1 if fraud_flag else 0,
        "generated_at": datetime.now().isoformat()
    }


@app.route("/")
def home():
    return jsonify({
        "message": "Toronto Credit Card Fraud Transaction API is running",
        "endpoints": [
            "/customers",
            "/cards",
            "/merchants",
            "/transactions?count=100"
        ]
    })


@app.route("/customers")
def customers():
    return jsonify(customers_df.to_dict(orient="records"))


@app.route("/cards")
def cards():
    return jsonify(cards_df.to_dict(orient="records"))


@app.route("/merchants")
def merchants():
    return jsonify(merchants_df.to_dict(orient="records"))


@app.route("/transactions")
def transactions():
    count = request.args.get("count", default=100, type=int)

    if count > 5000:
        return jsonify({"error": "Maximum count per request is 5000"}), 400

    return jsonify([generate_transaction() for _ in range(count)])


if __name__ == "__main__":
    app.run(debug=True, port=5000)