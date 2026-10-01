import json
import os
import time
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:49092")
TOPIC = os.getenv("KAFKA_TOPIC", "binance-btc-trades")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "criptoanalytics-consumer-group")

# Límites plausibles de mercado para la validación de rango financiero
MIN_PRICE_USDT = 10000.0
MAX_PRICE_USDT = 300000.0
MIN_QTY_BTC = 0.000001
MAX_QTY_BTC = 500.0

def validar_esquema(payload):
    """1. Validación de Esquema"""
    if not isinstance(payload, dict):
        return False
    
    campos_requeridos = ["tipoEvento", "tradeId", "symbol", "price", "quantity", "quoteQuantity", "timestamp"]
    for campo in campos_requeridos:
        if campo not in payload or payload[campo] is None:
            return False
            
    if payload.get("tipoEvento") != "crypto.trade":
        return False
        
    return True

def validar_rango_plausible(payload):
    """2. Validación de Rango Físico/Financiero Plausible"""
    precio = payload.get("price", 0.0)
    cantidad = payload.get("quantity", 0.0)
    
    if not (MIN_PRICE_USDT <= precio <= MAX_PRICE_USDT):
        return False, f"Precio fuera de rango financiero plausible: {precio} USDT"
        
    if not (MIN_QTY_BTC <= cantidad <= MAX_QTY_BTC):
        return False, f"Volumen operado fuera de rango plausible: {cantidad} BTC"
        
    return True, "OK"

def deserializar(m):
    try:
        return json.loads(m.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

# Conexión al Consumidor Kafka
consumer = None
while not consumer:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=deserializar,
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
        print(f"[CONSUMER] Conectado a Kafka. Escuchando topic: {TOPIC}")
    except Exception as e:
        print(f"[CONSUMER] Esperando a Kafka... ({e})")
        time.sleep(3)

for msg in consumer:
    raw_payload = msg.value
    partition = msg.partition
    offset = msg.offset
    key = msg.key
    
    # Paso 1: Validación de Esquema
    if not validar_esquema(raw_payload):
        print(f"[CONSUMER][INVALID] Partición: {partition} | Offset: {offset} | Mensaje malformado o sin esquema correcto.")
        continue
        
    # Paso 2: Validación de Rango Físico/Financiero
    es_valido, motivo = validar_rango_plausible(raw_payload)
    if not es_valido:
        print(f"[CONSUMER][ALERTA - RANGO EXCESIVO] Partición: {partition} | Offset: {offset} | TradeID: {raw_payload['tradeId']} | Motivo: {motivo}")
        continue
        
    # Mensaje Consumido Exitosamente
    print(f"[CONSUMER][CONSUMED] Partición: {partition} | Offset: {offset} | Key: {key} | "
          f"TradeID: {raw_payload['tradeId']} | Price: {raw_payload['price']} USDT | Qty: {raw_payload['quantity']} BTC")