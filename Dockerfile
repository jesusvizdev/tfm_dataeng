FROM python:3.11-slim

WORKDIR /app

COPY main.py /app/
COPY iot_generator/sensor_producer.py /app/iot_generator/

RUN pip install --no-cache-dir pandas numpy azure-eventhub

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
