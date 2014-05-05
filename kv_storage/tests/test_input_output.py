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
        num_processes = 4

        self.config = {
            'hosts': [['localhost', 0, 0] for i in range(num_processes)],
            'input': [],
            'output': [],
        }

        self.inputs = []
        self.outputs = []
        for i in range(num_processes):
            # set up pipes
            r, w = os.pipe()
            self.config['input'].append(os.fdopen(r, 'r'))
            self.inputs.append(os.fdopen(w, 'w'))

            r, w = os.pipe()
            self.config['output'].append(os.fdopen(w, 'w'))
            self.outputs.append(os.fdopen(r, 'r'))

        input_handlers = []
        for i in range(num_processes):
            input_handlers.append(InputHandler(i, self.config))

        storage_handlers = []
        for i in range(num_processes):
            storage_handlers.append(StorageHandler(i, [1, 1, 1], self.config))

        self.handlers = input_handlers+storage_handlers
        for handler in self.handlers:
            handler.run()

        self.all_outputs += self.outputs
        self.all_inputs += self.inputs

    def test_insert_and_get(self):
        self.inputs[0].write('insert 1 42 9\n')
        self.inputs[0].write('get 1 9\n')
        self.inputs[0].flush()

        insert_result = self.outputs[0].readline().rstrip()
        get_result = self.outputs[0].readline().rstrip()

        self.assertEqual(insert_result, '> insert successful')
        self.assertEqual(get_result, '> 42')

    def test_get_nonexisting_key(self):
        self.inputs[0].write('get 2 9\n')
        self.inputs[0].flush()

        get_result = self.outputs[0].readline().rstrip()
        self.assertEqual(get_result, '> None')

    def test_get_insert_update(self):
        self.inputs[0].write('get 0 9\n')
        self.inputs[0].write('insert 0 42 9\n')
        self.inputs[0].write('get 0 9\n')
        self.inputs[0].write('update 0 10 9\n')
        self.inputs[0].write('get 0 9\n')
        self.inputs[0].flush()

        self.assertEqual(self.outputs[0].readline().rstrip(), '> None')
        self.assertEqual(self.outputs[0].readline().rstrip(), '> insert successful')
        self.assertEqual(self.outputs[0].readline().rstrip(), '> 42')
        self.assertEqual(self.outputs[0].readline().rstrip(), '> update successful')
        self.assertEqual(self.outputs[0].readline().rstrip(), '> 10')

    @classmethod
    def tearDownClass(cls):
        for input in cls.all_inputs:
            input.write('exit,0,0,0\n')
            input.flush()
            input.close()

        for output in cls.all_outputs:
            output.close()

if __name__ == '__main__':
    unittest.main()
