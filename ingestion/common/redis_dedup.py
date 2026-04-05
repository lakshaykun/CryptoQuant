import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

BLOOM_KEY = "crypto_sentiment_bloom"


def is_duplicate(item_id: str) -> bool:
    return r.execute_command("BF.EXISTS", BLOOM_KEY, item_id) == 1


def add_to_bloom(item_id: str):
    r.execute_command("BF.ADD", BLOOM_KEY, item_id)