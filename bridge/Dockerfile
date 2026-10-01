FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bridge_binance_kafka.py .
CMD ["python", "-u", "bridge_binance_kafka.py"]