# Contrato del evento `crypto.trade`

**Proyecto:** CriptoAnalytics (Proyecto Sello)
**Topic:** `binance-btc-trades`
**Particiones:** 3 (recomendadas)
**Key:** `symbol` (ej. `BTCUSDT`), para garantizar el orden por par comercial
**Productor:** `bridge_binance_kafka.py` (WebSocket `wss://stream.binance.com:9443/ws/btcusdt@trade` → Kafka)
**Consumidor:** `consumer_crypto.py` (grupo `criptoanalytics-consumer-group`)

## Campos

| Campo | Tipo | Unidad / Formato | Descripción | Campo origen (Binance) |
|---|---|---|---|---|
| tipoEvento | string | Literal | Siempre `"crypto.trade"` | (fijo) |
| tradeId | integer | ID numérico | Identificador único de la transacción | `t` |
| symbol | string | Par cripto/fiat | Par comercial, ej. `"BTCUSDT"` | `s` |
| price | number | USDT | Precio de ejecución por BTC | `p` (string → float) |
| quantity | number | BTC | Cantidad de BTC negociada | `q` (string → float) |
| quoteQuantity | number | USDT | Monto total (price × quantity) | calculado |
| isBuyerMaker | boolean | true/false | `true` si el comprador fue el maker | `m` |
| origen | string | Literal | Siempre `"binance-websocket"` | (fijo) |
| timestamp | integer | Época en ms | Momento de ejecución del trade | `T` |

## Ejemplo

```json
{
  "tipoEvento": "crypto.trade",
  "tradeId": 6723269804,
  "symbol": "BTCUSDT",
  "price": 83845.92,
  "quantity": 0.0005,
  "quoteQuantity": 41.92296,
  "isBuyerMaker": true,
  "origen": "binance-websocket",
  "timestamp": 1790723699762
}
```

## Validaciones en el consumer

| Nivel | Qué verifica | Resultado |
|---|---|---|
| Esquema | JSON válido, objeto, campos requeridos presentes y no nulos, `tipoEvento == "crypto.trade"` | `INVALID` |
| Rango plausible | `10000 ≤ price ≤ 300000` USDT y `0.000001 ≤ quantity ≤ 500` BTC | `ALERTA - RANGO EXCESIVO` |
| Correcto | Pasa ambos niveles | `CONSUMED` |