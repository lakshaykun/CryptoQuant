import requests
import time
from datetime import datetime
from pipelines.streaming.kafka_producer import CryptoProducer
from pipelines.utils.logger import get_logger

logger = get_logger("producer")

SYMBOL = "BTCUSDT"
INTERVAL = "5m"

producer = CryptoProducer()

def fetch_data():
    url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit=1"
    response = requests.get(url).json()

    kline = response[0]

    return {
        "timestamp": kline[0],
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
        "trades": int(kline[8]),
        "taker_buy_base": float(kline[9]),
        "symbol": SYMBOL
    }

if __name__ == "__main__":
    while True:
        data = fetch_data()
        producer.send("crypto_prices", data)
        logger.info(f"Sent: {data}")
        time.sleep(300)  # 5 minutes