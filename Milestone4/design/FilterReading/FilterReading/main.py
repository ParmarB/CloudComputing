import json
import os
from google.cloud import pubsub_v1

# Configuration
project_id = os.getenv("GCP_PROJECT")
topic_name = os.getenv("TOPIC_NAME", "meter-data")
subscription_id = os.getenv("SUBSCRIPTION_ID", "filter-sub")

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        print(f"Received reading: {data}")

        # Logic: Eliminate records if ANY measurement is None (Pressure, Temp, or Humidity)
        required_fields = ["pressure", "temperature", "humidity"]
        
        if all(data.get(field) is not None for field in required_fields):
            print("Data valid. Forwarding to conversion...")
            
            # Publish to same topic, but change function tag to 'convert'
            publisher.publish(
                topic_path, 
                message.data, 
                function="convert"
            )
        else:
            print(f"Data invalid (missing one of {required_fields}). Dropping message.")
        
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

# Subscribe and listen
print(f"FilterReading listening on {subscription_path}...")
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()