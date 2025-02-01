import time
import random
import csv

import random
import csv
import time

def generate_transaction(flag_fraudulent_activity):
    transaction_type = random.choice(["purchase", "withdrawal", "deposit"])
    amount = round(random.uniform(10.0, 1000.0), 2)
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

    # Merchant Information
    merchant_id = random.randint(1000, 9999)
    merchant_name = f"Merchant_{merchant_id}"
    merchant_location = {
        "latitude": round(random.uniform(-90, 90), 6),
        "longitude": round(random.uniform(-180, 180), 6),
    }
    merchant_category = random.choice(["restaurant", "electronics_store", "clothing_store"])

    # Customer Information
    customer_id = random.randint(10000, 99999)
    customer_age = random.randint(18, 80)
    customer_location = {
        "latitude": round(random.uniform(-90, 90), 6),
        "longitude": round(random.uniform(-180, 180), 6),
    }

    # Transaction Method
    payment_method = random.choice(["credit_card", "debit_card", "cash"])
    currency = random.choice(["USD", "EUR", "GBP"])

    # Time-related Features
    day_of_week = random.choice(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    time_of_day = random.choice(["morning", "afternoon", "evening"])

    # Transaction Status
    transaction_status = random.choice(["completed", "pending", "declined"])

    # Fraud Detection Features
    if flag_fraudulent_activity:
        anomaly_score = round(random.uniform(0.5, 1.0), 2)  # Adjusted for higher anomaly in fraudulent cases
        flag_fraudulent_activity = random.choice([True, True, False])  # Increase chance of fraud
    else:
        anomaly_score = round(random.uniform(0.0, 0.5), 2)

    # Discounts and Rewards
    applied_discounts = round(random.uniform(0.0, 50.0), 2)
    reward_points = random.randint(0, 100)

    # Transaction Channel
    transaction_channel = random.choice(["online", "in-person"])
    app_vs_website = random.choice(["mobile_app", "website"])

    # Device Information
    device_used = random.choice(["smartphone", "desktop"])

    # Additional Financial Metrics
    balance_before = round(random.uniform(100.0, 10000.0), 2)
    balance_after = balance_before - amount
    transaction_fee = round(random.uniform(0.0, 10.0), 2)

    return {
        "timestamp": timestamp,
        "type": transaction_type,
        "amount": amount,
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "merchant_location": merchant_location,
        "merchant_category": merchant_category,
        "customer_id": customer_id,
        "customer_age": customer_age,
        "customer_location": customer_location,
        "payment_method": payment_method,
        "currency": currency,
        "day_of_week": day_of_week,
        "time_of_day": time_of_day,
        "transaction_status": transaction_status,
        "flag_fraudulent_activity": flag_fraudulent_activity,
        "anomaly_score": anomaly_score,
        "applied_discounts": applied_discounts,
        "reward_points": reward_points,
        "transaction_channel": transaction_channel,
        "app_vs_website": app_vs_website,
        "device_used": device_used,
        "balance_before": balance_before,
        "balance_after": balance_after,
        "transaction_fee": transaction_fee,
    }

def generate_location():
    latitude = round(random.uniform(-90, 90), 6)
    longitude = round(random.uniform(-180, 180), 6)
    return {"latitude": latitude, "longitude": longitude}

def generate_realtime_data_and_save(num_transactions, interval=1, csv_filename="transactions.csv"):
    # Open a CSV file for writing
    with open(csv_filename, mode='w', newline='') as csv_file:
        fieldnames = [
            "timestamp", "type", "amount", "merchant_id", "merchant_name",
            "merchant_latitude", "merchant_longitude", "merchant_category",
            "customer_id", "customer_age", "customer_latitude", "customer_longitude",
            "payment_method", "currency", "day_of_week",
            "time_of_day", "transaction_status", "flag_fraudulent_activity",
            "anomaly_score", "applied_discounts", "reward_points",
            "transaction_channel", "app_vs_website", "device_used",
            "balance_before", "balance_after", "transaction_fee"
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header row
        writer.writeheader()

        # Generate a stream of data with additional features
        for _ in range(num_transactions):
            transaction = generate_transaction('False')
            merchant_location = generate_location()
            customer_location = generate_location()

            # Remove 'customer_location' and 'merchant_location' fields
            transaction.pop("customer_location", None)
            transaction.pop("merchant_location", None)

            # Add 'latitude' and 'longitude' fields
            transaction.update({
                "merchant_latitude": merchant_location["latitude"],
                "merchant_longitude": merchant_location["longitude"],
                "customer_latitude": customer_location["latitude"],
                "customer_longitude": customer_location["longitude"]
            })

            writer.writerow(transaction)
            print(f"Transaction: {transaction}")
            time.sleep(interval)

if __name__ == "__main__":
    # Set the number of transactions, interval, and CSV filename
    num_transactions = 500
    interval_seconds = 1
    csv_filename = "transactions_n1.csv"

    # Generate real-time data with additional features and save to CSV
    generate_realtime_data_and_save(num_transactions, interval_seconds, csv_filename)