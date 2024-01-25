import json
import logging
import random
import time

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.DEBUG,
)

def main():
    json_config = open("./machines-random-producer-config.json", "r")
    configs = json.load(json_config)

    logging.info(configs)
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        key_serializer=lambda v: json.dumps(v).encode('ascii'),
        value_serializer=lambda v: json.dumps(v).encode('ascii')
    )

    while True:
        for config in configs:
            message = {
                "machineId": config.get("machineId"),
                "name": config.get("name"),
                "throughput": random.normalvariate(config.get("meanThroughput"), config.get("stdevThroughput")),
                "temperature": random.normalvariate(config.get("meanTemperature"), config.get("stdevTemperature")),
            }
            producer.send("machines", json.dumps(message))

        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    main()

