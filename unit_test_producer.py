import unittest
from producer.producer import acknw
import logging

logging.basicConfig(filename="logfile.log",format='%(asctime)s %(message)s',filemode='w')
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
class  TestProducer(unittest.TestCase):
    
    def test_acknw(self):
        self.assertEqual(acknw(None,"hi"),None)
        logging.info("test_acknw successfully tested")
        
    def test_type_compatibilty_acknw(self):
        self.assertRaises(TypeError,acknw(None,123465))
        logging.info("test_type_compatibilty_acknw  successfully tested")
    
if __name__ == '__main__':
    unittest.main()