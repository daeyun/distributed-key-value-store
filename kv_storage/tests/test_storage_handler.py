import unittest
from storage_handler import StorageHandler


class TestStorageHandler(unittest.TestCase):
    def setUp(self):
        self.delay_times = [0, 0, 0]

    def test_get_replica_ids(self):
        storage_handler = StorageHandler(0, self.delay_times)
        self.assertEqual(storage_handler.get_replica_ids(0), [1, 2, 3])
        self.assertEqual(storage_handler.get_replica_ids(1), [2, 3, 0])
        self.assertEqual(storage_handler.get_replica_ids(2), [3, 0, 1])
        self.assertEqual(storage_handler.get_replica_ids(3), [0, 1, 2])

    def test_find_inconsistent_replicas_highest_version(self):
        storage_handler = StorageHandler(0, self.delay_times)
        replica_values = [(2, 42, 0), (3, 99, 1), (0, 1, 2)]
        replica_ids, version_num, value = storage_handler.find_inconsistent_replicas(replica_values)

        self.assertEqual(replica_ids, [0, 2])
        self.assertEqual(version_num, 3)
        self.assertEqual(value, 99)

    def test_find_inconsistent_replicas_break_ties(self):
        storage_handler = StorageHandler(0, self.delay_times)
        replica_values = [(1, 42, 0), (1, 42, 1), (1, 1, 2)]
        replica_ids, version_num, value = storage_handler.find_inconsistent_replicas(replica_values)

        self.assertEqual(replica_ids, [2])
        self.assertEqual(version_num, 1)
        self.assertEqual(value, 42)

    def test_find_inconsistent_replicas_handle_none(self):
        storage_handler = StorageHandler(0, self.delay_times)
        replica_values = [(1, 42, 0), (2, 'None', 1), (1, 1, 2)]
        replica_ids, version_num, value = storage_handler.find_inconsistent_replicas(replica_values)

        self.assertEqual(replica_ids, [0, 2])
        self.assertEqual(version_num, 2)
        self.assertEqual(value, 'None')