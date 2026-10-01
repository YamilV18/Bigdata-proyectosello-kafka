import json
import os
import time
import websocket
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:49092")
TOPIC = os.getenv("KAFKA_TOPIC", "binance-btc-trades")
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Inicialización del Productor de Kafka
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[BRIDGE] Conectado exitosamente a Kafka en {KAFKA_BOOTSTRAP}")
    except Exception as e:
        print(f"[BRIDGE] Esperando a Kafka... ({e})")
        time.sleep(3)

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Mapeo del mensaje de Binance al Contrato del Proyecto Sello
        price = float(data.get("p", 0.0))
        qty = float(data.get("q", 0.0))
        
        evento = {
            "tipoEvento": "crypto.trade",
            "tradeId": data.get("t"),
            "symbol": data.get("s"),
            "price": price,
            "quantity": qty,
            "quoteQuantity": round(price * qty, 8),
            "isBuyerMaker": data.get("m"),
            "origen": "binance-websocket",
            "timestamp": data.get("T")
        }
        
        # Clave para particionado consistente por par comercial
        key = evento["symbol"]
        
        producer.send(TOPIC, key=key, value=evento)
        print(f"[BRIDGE] Evento enviado a Kafka | TradeID: {evento['tradeId']} | Precio: {evento['price']}")
    except Exception as e:
        print(f"[BRIDGE] Error procesando mensaje WS: {e}")

def on_error(ws, error):
    print(f"[BRIDGE] Error en WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("[BRIDGE] Conexión WebSocket cerrada. Reconectando...")

def on_open(ws):
    print(f"[BRIDGE] Conexión WebSocket establecida con Binance: {WS_URL}")

if __name__ == "__main__":
    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
        time.sleep(2)