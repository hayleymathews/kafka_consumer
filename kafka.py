import pickle
from confluent_kafka import Producer, Consumer, KafkaError

class KafkaProducer(object):

    def __init__(self, servers=None):
        servers = servers or ['localhost:9092']
        self.producer = Producer({'bootstrap.servers': ','.join(servers)})

    def produce(self, topic, data_iter, blocking=False, logging=False):
        for data in self.serializer(data_iter):
            if blocking:
                self.producer.poll(0.1)
            self.producer.produce(topic, value=data, callback=self.logger if logging else None)
        self.producer.flush(1)

    def serializer(self, data_iter):
        for data in data_iter:
            yield pickle.dumps(data)
    
    def logger(self, err, msg):
        if err is not None:
            print('Message failed to send with error: {}'.format(err))
        else:
            print('Message {} delivered to {} [{}]'.format(msg.value(), msg.topic(), msg.partition()))

class KafkaConsumer(object):
    def __init__(self, servers=None, group_id='group'):
        servers = servers or ['localhost:9092']
        self.consumer = Consumer({'bootstrap.servers': ','.join(servers),
                                  'group.id': group_id})

    def consume(self, topics, process_func=None):
        process_func = process_func or self.logger
        self.consumer.subscribe(topics)
        while True:
            msg = self.consumer.poll(1)
            if not msg:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            data = pickle.loads(msg.value())
            process_func(msg.topic(), data)
        self.consumer.close()
    
    def logger(self, topic, data):
        print('Message {} recieved from {}'.format(data, topic))


            