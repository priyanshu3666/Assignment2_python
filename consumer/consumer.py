from confluent_kafka import Consumer
from confluent_kafka import KafkaError,KafkaException
import sys
from sqlalchemy import MetaData,Table,VARCHAR,Column,Integer,create_engine, engine
import json
from sqlalchemy.sql.schema import Constraint, ForeignKey, ForeignKeyConstraint

from sqlalchemy.sql.sqltypes import BIGINT

meta =MetaData()
engine = create_engine("mysql+mysqlconnector://shukla:a123456@0.0.0.0:3306/sample",echo = True)
connection = engine.connect()
Table(
    'student', meta,
    Column('stu_id', Integer, primary_key=True),
    Column('firstName', VARCHAR(50)),
    Column('lastName', VARCHAR(50)),
    Column('gender', VARCHAR(11)),
    Column('age', Integer),
    Column('streetAddress', VARCHAR(30)),
    Column('city', VARCHAR(20)),
    Column('state', VARCHAR(20)),
    Column('postalCode',Integer()),
    
)
Table(
    'student_contact', meta,
    Column('cont_id', Integer, primary_key=True, autoincrement=True),
    Column('stu_id', Integer,ForeignKey("student.stu_id")),
    Column('type', VARCHAR(10)),
    Column('number', BIGINT())
)

meta.create_all(engine)
conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "student_data",
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

def msg_process(msg):
    data = {}
    try:
        last_id_sql = connection.execute("select max(stu_id) from student")
        last_id = last_id_sql.all()[0][0]
    except ConnectionError:
        print("connection not established with database ")
       
    data[msg.key().decode('utf-8')] = msg.value().decode('utf-8')
    stu_detail_dict= {}
    stu_phone_dict = {}
    if last_id is None:
        stu_detail_dict['stu_id'] = 1
        stu_phone_dict['stu_id'] = 1
    else:
        print(stu_detail_dict)
        last_id +=1
        stu_detail_dict['stu_id'] = last_id
        stu_phone_dict['stu_id'] = last_id
    try:
        for k,v in json.loads(data['sample']).items():
            if type(v)==dict:
                for key, value in v.items():
                    stu_detail_dict[key]=value
            elif type(v)==list:
                continue
            else:
                if k=='firstName' and v=="":
                    return "First name cannot be empty"
                stu_detail_dict[k]=v
    except AttributeError:
        print("empty file sent by producer")

    details_columns = ', '.join(""+str(x)+"" for x in stu_detail_dict.keys())
    details_values = ', '.join("'"+str(x)+"'" for x in stu_detail_dict.values())
    insert_sql1 = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('student', details_columns, details_values)
    connection.execute(insert_sql1)

    try:
        for k,v in json.loads(data['sample']).items():
            if type(v)==list:
                for i in range(len(v)):
                    for key, value in v[i].items():
                        stu_phone_dict[key]=value
                    stu_contact_field = ', '.join(""+str(value)+"" for value in stu_phone_dict.keys())
                    contact_values = ', '.join("'"+str(value)+"'" for value in stu_phone_dict.values())
                    insert_statement= "INSERT INTO %s ( %s ) VALUES ( %s );" % ('student_contact', stu_contact_field, contact_values)
                    connection.execute(insert_statement)
    except AttributeError:
        print("empty file sent by producer")

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(1)
            if msg is None: 
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
                
            else:
                msg_process(msg)
    except KafkaException:
        print("Unknown topic or partition")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
    basic_consume_loop(consumer, ['student_data'])


