import logging,csv,os,json
from confluent_kafka import Consumer
from confluent_kafka import KafkaError,KafkaException
import random
import string
conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "student_data",
        'auto.offset.reset': 'earliest',
        }
consumer = Consumer(conf)

def msg_process(msg):    
    data = {}    
    data[msg.key().decode('utf-8')] = msg.value().decode('utf-8')
    if data.values()==  "":
        print("empty string")
        return "empty string"
    
    else:
        stu_detail_dict={}
        stu_phone_dict={}
        stu_id = ''.join([random.choice(string.ascii_letters
                + string.digits) for num in range(10)]) 
        stu_detail_dict['stu_id']=stu_id
        stu_phone_dict['stu_id']=stu_id
                 
        for k,v in json.loads(data[msg.key().decode('utf-8')]).items():
            if k=='firstName' and v=="":
                return "First name cannot be empty" 
            elif type(v)==dict:
                for key, value in v.items():
                    stu_detail_dict[key]=value
            elif type(v)==list:
                continue
            else:            
                stu_detail_dict[k]=v   
         
        for k,v in json.loads(data[msg.key().decode('utf-8')]).items():
            if type(v)==list:
                for i in range(len(v)):
                    for key, value in v[i].items():
                        stu_phone_dict[key]=value
   
    student_header = ['stu_id','firstName','lastName','gender','age','streetAddress','city','state','postalCode']
    student_contact_header = ['stu_id','type','number']
    file_path_stu_contact = "/home/priyanshu/Desktop/Assignment2_python/student_contact.csv"
    file_path_stu = "/home/priyanshu/Desktop/Assignment2_python/student.csv"
    
    if os.path.exists(f'{file_path_stu}'):
        with open(f'{file_path_stu}','a') as csv_file:
            csv_writer_ = csv.DictWriter(csv_file,student_header)        
            csv_writer_.writerow({field: stu_detail_dict[field] for field in student_header})
    else:
        with open(f'{file_path_stu}','a') as csv_file:
            csv_writer_ = csv.DictWriter(csv_file,student_header)
            csv_writer_.writeheader()        
            csv_writer_.writerow( stu_detail_dict )
    
            
    if os.path.exists(f"{file_path_stu_contact}"):
        with open(f"{file_path_stu_contact}",'a') as csv_file:
            csv_writer_ = csv.DictWriter(csv_file,student_contact_header)        
            csv_writer_.writerow(stu_phone_dict)
    else:
        with open(f"{file_path_stu_contact}",'a') as csv_file:
            csv_writer_ = csv.DictWriter(csv_file,student_contact_header)
            csv_writer_.writeheader()        
            csv_writer_.writerow( stu_phone_dict )
                        


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(1)
            if msg is None: 
                
                print("hhehehehheh")          
                break

            if msg.error():                
                if msg.error():
                    raise KafkaException(msg.error())
                
            else:
                msg_process(msg)
    except KafkaException:
        print("Unknown topic or partition")
    finally:
        consumer.close()
   
if __name__ == '__main__':
    basic_consume_loop(consumer, ['student_data'])


