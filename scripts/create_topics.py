import subprocess
import yaml
import time

with open("/configs/kafka.yaml") as f:
    config = yaml.safe_load(f)

brokers = config["brokers"]

for topic in config["topics"]:
    for attempt in range(5):
        try:
            cmd = [
                "/opt/kafka/bin/kafka-topics.sh",
                "--create",
                "--if-not-exists",
                "--topic", topic["name"],
                "--bootstrap-server", brokers,
                "--partitions", str(topic["partitions"]),
                "--replication-factor", str(topic["replication_factor"]),
            ]

            subprocess.run(cmd, check=True)
            print(f"Created topic: {topic['name']}")
            break

        except Exception as e:
            print(f"Retrying topic creation for {topic['name']}...")
            time.sleep(5)