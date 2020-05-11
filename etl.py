from kafka import KafkaProducer
import random


etl_producer = KafkaProducer()

def create_features(data):
    for rec in data:
        feat_y = rec['field_y']  + random.randint(0, 3)
        feat_z = random.choice(['D', 'E', 'F'])
        etl_producer.produce('features', [{'id': rec['id'],
                                           'feat_y': feat_y,
                                           'feat_z': feat_z}])

if __name__ == '__main__':
    data = [{'id': random.randint(500, 600), 
             'field_x': random.choice(['A', 'B', 'C']), 
             'field_y': random.choice([1, 2, 3])} for _ in range(10)]

    etl_producer.produce('raw_fields', data)
    create_features(data)

