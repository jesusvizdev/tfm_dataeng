import os, json, time
from iot_generator.sensor_producer import generate_synthetic_data
from azure.eventhub import EventHubProducerClient, EventData

CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENT_HUB_NAME  = os.getenv("EVENTHUB_NAME", "sensor-data")
INTERVAL_SEC    = float(os.getenv("SEND_INTERVAL_SEC", "1"))

if not CONNECTION_STR:
    raise RuntimeError("EVENTHUB_CONNECTION_STRING no está definido")

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

if __name__ == "__main__":
    try:
        while True:
            df = generate_synthetic_data(n_samples=1)
            record = df.to_dict(orient="records")[0]
            print(f"Enviando: {record}", flush=True)
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(record)))
            producer.send_batch(batch)
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print("⛔ Generación detenida por el usuario.")
