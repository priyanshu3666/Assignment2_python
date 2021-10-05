import unittest
from consumer import consumer
import logging

logging.basicConfig(filename="logfileconsumer.log",format='%(asctime)s %(message)s',filemode='a')
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
class TestConsumer(unittest.TestCase):
    
    def test_data_insertion(self):
        self.assertIsNone(consumer.basic_consume_loop(consumer.consumer,['student_data']))
            
    def test_mysql_connection(self):
        self.assertIsNotNone(consumer.connection)
        logging.info("test_mysql_connection successfully tested")
        
    def test_table_creation(self):
        self.assertIsNone(consumer.meta.create_all(consumer.engine))
    
    def test_topic_existence(self):
        self.assertRaises(TypeError,consumer.basic_consume_loop(consumer.consumer, ['student_da']))
        
    def test_final_expec_result(self):
        self.assertEqual(consumer.connection.execute("select firstName,lastName,age,postalcode,number from student,student_contact  where student_contact.number=8004954515 and student.postalcode=210431").fetchone(),('Priyanshu', 'Shukla', 22, 210431,8004954515))
        
   
        
    
                
if __name__ =="__main__":
    unittest.main()