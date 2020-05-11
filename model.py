from kafka import KafkaProducer, KafkaConsumer
import random


model_producer = KafkaProducer()
model_consumer = KafkaConsumer(group_id='model')


def make_prediction(topic, rec):
    score = random.randint(0, 100)
    model_producer.produce('model_results', [{'id': rec['id'], 'score': score}])


if __name__ == '__main__':
    model_consumer.consume(['features'], process_func=make_prediction)