from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


value_schema_str = """
{
   "namespace": "xcaliber",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

key_schema_str = """
{
   "namespace": "xcaliber",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

if __name__ == '__main__':

    value_schema = avro.loads(value_schema_str)
    key_schema = avro.loads(key_schema_str)
    value = {"name": "value_abc"}
    key = {"name": "key_123"}

    avroProducer = AvroProducer({
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'http://localhost:8081'
        })

    avroProducer.produce(topic='mbrynski-logevents', value=value, key=key, value_schema=value_schema, key_schema=key_schema)
    avroProducer.flush()
