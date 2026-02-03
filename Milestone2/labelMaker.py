from google.cloud import pubsub_v1
import glob
import json
import os
import pandas as pd
import time

# Search the current directory for the JSON file
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "temporal-works-485600-g0"
topic_name = "labels"

# Create a publisher and get the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.\n")

# Read the CSV file
df = pd.read_csv('Labels.csv')

# Iterate through each row in the CSV
for index, row in df.iterrows():
    # Build message (ID, profile_name, temperature, humidity, pressure, time)
    msg = {
        "ID": int(index) + 1,
        "profile_name": row['profileName'] if 'profileName' in row else row.get('profile_name'),
        "temperature": None if pd.isna(row['temperature']) else float(row['temperature']),
        "humidity": None if pd.isna(row['humidity']) else float(row['humidity']),
        "pressure": None if pd.isna(row['pressure']) else float(row['pressure']),
        "time": int(float(row['time'])) if not pd.isna(row['time']) else None
    }

    record_value = json.dumps(msg).encode('utf-8')
    
    try:
        future = publisher.publish(topic_path, record_value)
        future.result()
        print(f"Published record {index + 1}: {msg}")
    except Exception as e:
        print(f"Failed to publish record {index + 1}: {e}")
    
    time.sleep(0.5)  # wait 0.5 seconds between records

print("\nAll records published successfully!")