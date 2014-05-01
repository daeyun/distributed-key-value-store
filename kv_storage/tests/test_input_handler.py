import unittest
from input_handler import InputHandler
from helpers.distribution_helper import kv_hash
import hashlib

class TestInputHandler(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_coordinator(self):
        input_handler = InputHandler(0)
        process_hashes = [kv_hash(i) for i in range(5)]
        coordinator_id = input_handler.get_coordinator(42)

        print(process_hashes)
        process_hashes[coordinator_id]
        key_hash = kv_hash(42)
        print(process_hashes)
        print(key_hash)
