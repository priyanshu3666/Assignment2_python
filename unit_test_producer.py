import unittest
from producer.producer import acknw

class  TestProducer(unittest.TestCase):
    
    def test_acknw(self):
        self.assertEqual(acknw(None,"hi"),None)
        
    def test_type_compatibilty_acknw(self):
        self.assertRaises(TypeError,acknw(None,123465))
    
if __name__ == '__main__':
    unittest.main()