from kafka import KafkaProducer

def send_to_kafka(rows):
    producer = KafkaProducer(bootstrap_servers = util.get_broker_metadata())
    for row in rows:
        producer.send('topic',str(row.asDict()))
        producer.flush()

