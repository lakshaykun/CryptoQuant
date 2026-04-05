import os

import redis


def ensure_bloom_filter():
	host = os.getenv("REDIS_HOST", "localhost")
	port = int(os.getenv("REDIS_PORT", "6379"))
	key = os.getenv("REDIS_BLOOM_KEY", "crypto_sentiment_bloom")
	error_rate = float(os.getenv("REDIS_BLOOM_ERROR_RATE", "0.001"))
	capacity = int(os.getenv("REDIS_BLOOM_CAPACITY", "1000000"))

	client = redis.Redis(host=host, port=port, decode_responses=True)

	try:
		client.execute_command("BF.RESERVE", key, error_rate, capacity)
		print(f"Created bloom filter '{key}'")
	except redis.ResponseError as exc:
		# RedisBloom returns an error if the filter already exists.
		if "item exists" in str(exc).lower():
			print(f"Bloom filter '{key}' already exists")
			return
		raise


if __name__ == "__main__":
	ensure_bloom_filter()
