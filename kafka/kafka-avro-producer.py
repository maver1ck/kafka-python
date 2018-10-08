from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == '__main__':
    key_schema = avro.load("schemas/key.avsc")
    value_schema = avro.load("schemas/value.avsc")
    key = {"name": "key_123"}
    value = {"name": "value_abc"}

    avroProducer = AvroProducer({
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'http://localhost:8081'
        })

    avroProducer.produce(topic='mbrynski-logevents', value=value, key=key,
                         value_schema=value_schema, key_schema=key_schema, callback=delivery_report)
    avroProducer.flush()
