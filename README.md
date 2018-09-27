####Producer
```
>>> from kafka import KafkaProducer
>>> producer = KafkaProducer()
>>> data = range(0, 10, 2)
>>> producer.produce('add_10', data) # send data to add_10 topic
>>> producer.produce('sub_10', data) # send data to sub_10 topic
```

####Consumer
```
>>> from kafka import KafkaConsumer
>>> consumer = KafkaConsumer()
>>> def process_func(topic, data):
...     if topic == 'add_10':
...         print(data, data + 10)
...     elif topic == 'sub_10':
...         print(data, data - 10)
...     else:
...         print(data)
>>> consumer.consume(['add_10', 'sub_10'], process_func=process_func)
0 10
2 12
4 14
6 16
8 18
0 -10
2 -8
4 -6
6 -4
8 -2
```