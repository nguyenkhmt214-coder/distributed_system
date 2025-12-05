# kafka_producer_app.py
# =========================
# App producer Kafka with debug logging
# =========================

from confluent_kafka import Producer
import json
import time
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.generator import generate_bundle


# =========================
# Kafka Producer config
# =========================
producer_conf = {
    "bootstrap.servers": "localhost:29092,localhost:29093",
    "client.id": "fraud-simulator",
}
producer = Producer(producer_conf)


def send_to_kafka(topic, value, key=None):
    """
    Send message to Kafka with debug print.
    """
    payload = json.dumps(value).encode("utf-8")

    print("\n----------------------------------")
    print(f"[PRODUCER] Sending to topic: {topic}")
    print(f"[PRODUCER] Key: {key}")
    print("[PRODUCER] Value JSON:")
    print(json.dumps(value, indent=2))
    print("----------------------------------\n")

    producer.produce(
        topic=topic,
        key=(key.encode("utf-8") if key else None),
        value=payload
    )
    producer.flush(0)


# =========================
# Main loop — push data
# =========================
if __name__ == "__main__":
    while True:
        bundle = generate_bundle()

        # LOG bundle tổng thể
        print("\n========== GENERATED BUNDLE ==========")
        print(json.dumps(bundle, indent=2))
        print("======================================\n")

        send_to_kafka("user_profile",
                      bundle["user_profile"],
                      key=bundle["user_profile"]["party_id"])

        # send_to_kafka("card_account",
        #               bundle["card_account"],
        #               key=bundle["card_account"]["card_ref"])

        # send_to_kafka("merchant_profile",
        #               bundle["merchant_profile"],
        #               key=bundle["merchant_profile"]["merchant_id"])

        # send_to_kafka("card_txn_auth",
        #               bundle["card_txn_auth"],
        #               key=bundle["card_txn_auth"]["card_ref"])

        print(">>> Sent 4 events → Kafka\n")
        time.sleep(5)
