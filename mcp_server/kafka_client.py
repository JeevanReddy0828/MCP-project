import os
from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def end_offsets(topic: str) -> dict:
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False)
    parts = consumer.partitions_for_topic(topic)
    if not parts:
        return {"topic": topic, "partitions": {}}
    tps = [TopicPartition(topic, p) for p in parts]
    ends = consumer.end_offsets(tps)
    consumer.close()
    return {"topic": topic, "partitions": {str(tp.partition): int(ends[tp]) for tp in tps}}

def consumer_lag(topic: str, group: str) -> dict:
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=group, enable_auto_commit=False)
    parts = consumer.partitions_for_topic(topic)
    if not parts:
        return {"topic": topic, "group": group, "lag": 0, "partitions": {}}

    tps = [TopicPartition(topic, p) for p in parts]
    consumer.assign(tps)

    ends = consumer.end_offsets(tps)
    committed = {tp: (consumer.committed(tp) or 0) for tp in tps}

    per_part = {}
    total_lag = 0
    for tp in tps:
        lag = int(ends[tp] - committed[tp])
        total_lag += lag
        per_part[str(tp.partition)] = {"end": int(ends[tp]), "committed": int(committed[tp]), "lag": lag}

    consumer.close()
    return {"topic": topic, "group": group, "lag": total_lag, "partitions": per_part}
