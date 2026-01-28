from google.cloud import pubsub_v1
import glob
import json
import os

# Search the current directory for the JSON file
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "temporal-works-485600-g0"
topic_name = "labels"
subscription_id = "labels-sub"

# Create a subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}..\n")

# Callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    message_data = json.loads(message.data.decode('utf-8'))
    
    print(f"Consumed record: {message_data}")
    
    # Acknowledge the message
    message.ack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()