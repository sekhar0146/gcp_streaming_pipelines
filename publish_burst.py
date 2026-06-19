import time
import json
from google.cloud import pubsub_v1

project_id = "gcp-learnings-498010"
topic_id = "transactions-input"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Capture one single current timestamp for all 3 messages
frozen_timestamp = time.time()

messages = [
    {"txn_id": "BURST-1", "country": "IN", "amount": 100, "timestamp": frozen_timestamp},
    {"txn_id": "BURST-2", "country": "IN", "amount": 150, "timestamp": frozen_timestamp},
    {"txn_id": "BURST-3", "country": "IN", "amount": 400, "timestamp": frozen_timestamp}
]

print("💥 Firing synchronized burst of 3 messages...")

futures = []
for msg in messages:
    payload = json.dumps(msg).encode('utf-8')
    # This initiates the send in the background
    future = publisher.publish(topic_path, payload)
    futures.append(future)

# FIX: Force the main thread to wait until every background thread finishes uploading!
for i, future in enumerate(futures):
    msg_id = future.result()  # This blocks and waits for GCP's confirmation
    print(f"📡 Message {i+1} successfully sent! ID: {msg_id}")
    
print("\n🤫 Now stop typing and wait 15 seconds for the Session to close...")