import pandas as pd
import json
import time
from azure.eventhub import EventHubProducerClient, EventData

connection_str = "{ConnectionString}"
eventhub_name = "{custom}"

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str,
    eventhub_name=eventhub_name
)

df = pd.read_csv("Amazon Customer Behavior Survey.csv")

for index, row in df.iterrows():

    event_data = {
        "purchase_frequency": row["Purchase_Frequency"],
        "browsing_frequency": row["Browsing_Frequency"],
        "cart_completion": row["Cart_Completion_Frequency"],
        "abandonment_reason": row["Cart_Abandonment_Factors"],
        "satisfaction": row["Shopping_Satisfaction"]
    }

    batch = producer.create_batch()
    batch.add(EventData(json.dumps(event_data)))

    producer.send_batch(batch)

    print("Sent:", event_data)

    time.sleep(2)   # simulate streaming

producer.close()

