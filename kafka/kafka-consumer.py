from confluent_kafka import Consumer, KafkaError


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'abc1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})



if __name__ == '__main__':
    c.subscribe(['maverick'])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        # if msg.error():
        #     if msg.error().code() == KafkaError._PARTITION_EOF:
        #         continue
        #     else:
        #         print(msg.error())
        #         break

        print('Received message: {}, {}'.format(msg.key(), msg.value()))
        c.commit(msg)

    c.close()
