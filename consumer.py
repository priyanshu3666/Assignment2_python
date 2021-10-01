from confluent_kafka import Consumer
from confluent_kafka import KafkaError,KafkaException
import sys
from sqlalchemy import MetaData,Table,VARCHAR,Column,Integer,create_engine, engine
import json

engine = create_engine("mysql+mysqlconnector://root:123456@localhost/sample",echo = True)
connection = engine.connect()


conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "student_data",
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

def msg_process(msg):
    data = {}
    rows = []
    data["{}".format(msg.key().decode('utf-8'))] = json.loads(msg.value().decode('utf-8')) 
    for field in data.values():
        rows.append((
            field['firstName'],
            field['lastName'],
            field['gender'],
            field['age'],
            field['address']['streetAddress'],
            field['address']['city'],
            field['address']['state'],
            field["address"]['postalCode'],
            field['phoneNumbers'][0]['type'],
            field['phoneNumbers'][0]['number']
        ))
    rur = tuple(rows)
    # for i in range(len(rur)):
    print("inside ") 
    insert = "insert into student values{}".format(rur[0])
    connection.execute(insert)
    

    


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(1)
            if msg is None: break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
    basic_consume_loop(consumer, ['student_data'])


