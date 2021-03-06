import unittest
from input_handler import InputHandler
from helpers.network_helper import unpack_message


class TestNetworkHelper(unittest.TestCase):
    def setUp(self):
        pass

    def test_unpack_message(self):
        msg = 'coordinator,get,1,9'
        unpacked_msg = unpack_message(msg)
        self.assertEqual(unpacked_msg, ('coordinator', 'get', 1, [9]))

if __name__ == '__main__':
    unittest.main()
