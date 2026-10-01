FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer_crypto.py .
CMD ["python", "-u", "consumer_crypto.py"]