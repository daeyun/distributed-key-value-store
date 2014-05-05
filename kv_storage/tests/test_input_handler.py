import unittest
from input_handler import InputHandler


class TestInputHandler(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_coordinator_greatest(self):
        input_handler = InputHandler(0)
        #[115, 32, 900, 63, 736] for 0 to 4
        coordinator_id = input_handler.get_coordinator(0, 1906)
        self.assertEqual(coordinator_id, 1)

    def test_get_coordinator_mid(self):
        input_handler = InputHandler(0)
        #[115, 32, 900, 63, 736] for 0 to 4
        coordinator_id = input_handler.get_coordinator(0, 36)
        self.assertEqual(coordinator_id, 3)

    def test_get_coordinator_mid2(self):
        input_handler = InputHandler(0)
        #[115, 32, 900, 63, 736] for 0 to 4
        coordinator_id = input_handler.get_coordinator(0, 800)
        self.assertEqual(coordinator_id, 2)

if __name__ == '__main__':
    unittest.main()