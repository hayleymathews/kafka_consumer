from kafka import KafkaConsumer
import pymongo


app_consumer = KafkaConsumer(group_id='app')
conn = pymongo.MongoClient("mongodb://localhost")
db = conn.kafka


def save_to_db(topic, rec):
    try:
        db.results.update_one({'id': rec.pop('id')}, {'$set': {topic: rec}}, upsert=True)
    except Exception as e:
        print('bad rec: {}, exception: {}'.format(rec, e))


if __name__ == '__main__':
    app_consumer.consume(['raw_fields', 'features', 'model_results'], process_func=save_to_db)
