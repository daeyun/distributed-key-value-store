import unittest
from storage_handler import StorageHandler


class TestInputHandler(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_replica_ids(self):
        storage_handler = StorageHandler(0, 0)
        self.assertEqual(storage_handler.get_replica_ids(0), [1, 2, 3])
        self.assertEqual(storage_handler.get_replica_ids(1), [2, 3, 0])
        self.assertEqual(storage_handler.get_replica_ids(2), [3, 0, 1])
        self.assertEqual(storage_handler.get_replica_ids(3), [0, 1, 2])
