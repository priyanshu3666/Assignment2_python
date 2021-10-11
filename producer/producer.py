from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

def acknw(error,message):
    try:
        if error is not None:
            print("Failed to send messages : {}:{} ".format(message,error))
        else:
            print("Message sent succesfully")
    except TypeError:
        print("mes sage should be string")
        
if __name__ == '__main__':
    
            with open('/home/priyanshu/Desktop/Assignment2_python/producer/data.json','r') as file:
                data = file.read()
                producer.produce("student_data" ,key = "sample",value = data , callback = acknw)
                producer.flush()
                        
            
            




