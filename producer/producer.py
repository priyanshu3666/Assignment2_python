from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

def acknw(error,message):
    if error is not None:
        print("Failed to send messages : {}:{} ".format(message,error))
    else:
        print("Message sent succesfully")
       
with open("data.json",'r') as data:
    content = data.read()
    producer.produce("student_data" ,key = "sample",value = content , callback = acknw(None,"hi"))
    producer.flush()




