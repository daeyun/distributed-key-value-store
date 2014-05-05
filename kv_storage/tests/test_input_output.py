import unittest
import os
from storage_handler import StorageHandler
from input_handler import InputHandler

class TestInputOutput(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.all_inputs = []
        cls.all_outputs = []

    def setUp(self):
        self.num_processes = 4

        self.config = {
            'hosts': [['localhost', 0, 0] for i in range(self.num_processes)],
            'input': [],
            'output': [],
        }

        self.inputs = []
        self.outputs = []
        for i in range(self.num_processes):
            # set up pipes
            r, w = os.pipe()
            self.config['input'].append(os.fdopen(r, 'r'))
            self.inputs.append(os.fdopen(w, 'w'))

            r, w = os.pipe()
            self.config['output'].append(os.fdopen(w, 'w'))
            self.outputs.append(os.fdopen(r, 'r'))

        self.input_handlers = []
        for i in range(self.num_processes):
            self.input_handlers.append(InputHandler(i, self.config))

        self.storage_handlers = []
        for i in range(self.num_processes):
            self.storage_handlers.append(StorageHandler(i, [1, 1, 1], self.config))

        self.handlers = self.input_handlers + self.storage_handlers
        for handler in self.handlers:
            handler.run()

        self.all_outputs += self.outputs
        self.all_inputs += self.inputs

    def test_insert_and_get(self):
        commands = [
            (0, 'insert 1 42 9'),
            (0, 'get 1 9'),
        ]
        for pid, command in commands:
            self.inputs[pid].write(command + '\n')
            self.inputs[pid].flush()

        responses = [
            (0, 'insert successful'),
            (0, '42'),
        ]
        for pid, response in responses:
            self.assertEqual(self.outputs[pid].readline().rstrip(), '> ' + response)

    def test_get_nonexisting_key(self):
        commands = [
            (0, 'get 2 9'),
        ]
        for pid, command in commands:
            self.inputs[pid].write(command + '\n')
            self.inputs[pid].flush()

        responses = [
            (0, 'None'),
        ]
        for pid, response in responses:
            self.assertEqual(self.outputs[pid].readline().rstrip(), '> ' + response)

    def test_get_insert_update(self):
        commands = [
            (0, 'get 0 9'),
            (0, 'insert 0 42 9'),
            (0, 'get 0 9'),
            (0, 'update 0 10 9'),
            (0, 'get 0 9'),
        ]
        for pid, command in commands:
            self.inputs[pid].write(command + '\n')
            self.inputs[pid].flush()

        responses = [
            (0, 'None'),
            (0, 'insert successful'),
            (0, '42'),
            (0, 'update successful'),
            (0, '10'),
        ]
        for pid, response in responses:
            self.assertEqual(self.outputs[pid].readline().rstrip(), '> ' + response)

    def tearDown(self):
        for i in range(self.num_processes):
            self.inputs[i].write('exit\n')
            self.inputs[i].flush()
            self.inputs[i].close()

        for handler in self.input_handlers + self.storage_handlers:
            handler.join()

    @classmethod
    def tearDownClass(cls):

        for output in cls.all_outputs:
            output.close()

if __name__ == '__main__':
    unittest.main()
