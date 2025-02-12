from kafka import KafkaProducer, KafkaConsumer
import json

# Produtor
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

evento = {
    'evento': 'mms.order.created',
    'id': 123,
    'valor': 99.90
}

producer.send('mms.order', evento)
producer.close()

# Consumidor
consumer = KafkaConsumer('mms.order', bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for mensagem in consumer:
    print(mensagem.value)
consumer.close()
