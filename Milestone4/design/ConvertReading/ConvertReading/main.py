import json
import os
from google.cloud import pubsub_v1

# Configuration
project_id = os.getenv("GCP_PROJECT")
topic_name = os.getenv("TOPIC_NAME", "meter-data")
subscription_id = os.getenv("SUBSCRIPTION_ID", "convert-sub")

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        
        # Unit Conversion Logic
        # P(psi) = P(kPa) / 6.895
        # T(F) = T(C) * 1.8 + 32
        data['pressure'] = round(data['pressure'] / 6.895, 2)
        data['temperature'] = round((data['temperature'] * 1.8) + 32, 2)
        data['unit_p'] = 'psi'
        data['unit_t'] = 'F'

        print(f"Converted Data: {data}")

        # Publish result for BigQuery storage
        output_data = json.dumps(data).encode("utf-8")
        publisher.publish(
            topic_path, 
            output_data, 
            function="store"
        )
        
        message.ack()
    except Exception as e:
        print(f"Error in conversion: {e}")
        message.nack()

# Subscribe and listen
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"ConvertReading listening on {subscription_path}...")
with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()