import unittest
import os
from storage_handler import StorageHandler
from input_handler import InputHandler

class TestInputOutput(unittest.TestCase):
    def test_insert_and_get(self):
        self.run_commands([
            (0, 'insert 1 42 9'),
            (0, 'get 1 9'),
        ])
        self.check_responses([
            (0, 'insert successful'),
            (0, '42'),
        ])

    def test_get_nonexisting_key(self):
        self.run_commands([
            (0, 'get 2 9'),
        ])
        self.check_responses([
            (0, 'None'),
        ])

    def test_get_insert_update(self):
        self.run_commands([
            (0, 'get 0 9'),
            (0, 'insert 0 42 9'),
            (0, 'get 0 9'),
            (0, 'update 0 10 9'),
            (0, 'get 0 9'),
        ])
        self.check_responses([
            (0, 'None'),
            (0, 'insert successful'),
            (0, '42'),
            (0, 'update successful'),
            (0, '10'),
        ])

    def test_update_nonexisting_key(self):
        self.run_commands([
            (0, 'update 0 10 9'),
        ])
        self.check_responses([
            (0, 'update failed - key does not exist'),
        ])

    def test_insert_existing_key(self):
        self.run_commands([
            (0, 'insert 0 999 9'),
            (0, 'insert 0 999 9'),
        ])
        self.check_responses([
            (0, 'insert successful'),
            (0, 'insert failed - key already exists'),
        ])

    def test_get_nonexisting_key_all_processes(self):
        self.run_commands([
            (0, 'get 1 9'),
            (1, 'get 1 9'),
            (2, 'get 1 9'),
            (3, 'get 1 9'),
        ])
        self.check_responses([
            (0, 'None'),
            (1, 'None'),
            (2, 'None'),
            (3, 'None'),
        ])

    def test_get_nonexisting_key_all_processes_one(self):
        self.run_commands([
            (0, 'get 1 1'),
            (1, 'get 2 1'),
            (2, 'get 4 1'),
            (3, 'get 4 1'),
            ])
        self.check_responses([
            (0, 'None'),
            (1, 'None'),
            (2, 'None'),
            (3, 'None'),
            ])

    def test_concurrent_get_requests_delayed_insert(self):
        self.run_commands([
            (1, 'set_delay_times 0 0 0 0'),
            (0, 'set_delay_times 1 1 1 1'),
            (1, 'set_delay_times 2 0 0 0'),
            (1, 'set_delay_times 3 0 0 0'),
            (0, 'insert 2 42 9'),
            (1, 'set_delay_times 1 0 0 0'),
            (1, 'get 2 1'),
            (2, 'get 2 1'),
            (3, 'get 2 1'),
            (1, 'set_delay_times 0 1 1 1'),
            (1, 'set_delay_times 1 1 1 1'),
            (2, 'set_delay_times 2 1 1 1'),
            (3, 'set_delay_times 3 1 1 1'),
            (0, 'get 2 1'),
            (1, 'get 2 1'),
            (2, 'get 2 1'),
            (3, 'get 2 1'),
        ])
        self.check_responses([
            (0, 'insert successful'),
            (1, 'None'),
            (2, 'None'),
            (3, 'None'),
            (0, '42'),
            (1, '42'),
            (2, '42'),
            (3, '42'),
        ])

    def test_inconsistency_repair(self):
        self.force_quit = True
        self.run_commands([
            (0, 'set_delay_times 0 50000 50000 50000'),
            (1, 'set_delay_times 1 0 50000 50000'),  # coord 1 -> rep 2 is fast
            (2, 'set_delay_times 2 50000 50000 50000'),
            (3, 'set_delay_times 3 50000 50000 50000'),
            (0, 'wait 1'),
            (1, 'wait 1'),
            (2, 'wait 1'),
            (3, 'wait 1'),
            (0, 'insert 2 42 1'),  # coord:1. will return after one fast confirmation from rep 2
            (0, 'wait 1'),
            (1, 'wait 1'),
            (2, 'wait 1'),
            (3, 'wait 1'),
            (0, 'set_delay_times 0 0 0 0'),
            (1, 'set_delay_times 1 10 0 0'),  # coord 1 -> rep 2 is slow
            (2, 'set_delay_times 2 0 0 0'),
            (3, 'set_delay_times 3 0 0 0'),
            (0, 'wait 3'),
            (0, 'get 2 1'),  # coord:1. will return None after one fast confirmation from rep 0 or rep 3
            (0, 'wait 20'),
            (1, 'wait 20'),
            (2, 'wait 20'),
            (3, 'wait 20'),  # repair should have completed by now
            (0, 'get 2 1'), # will return 42
        ])
        self.check_responses([
            (0, 'insert successful'),
            (0, 'None'),
            (0, '42'),
        ])
        self.force_quit = False


    ## setup methods
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
        self.input_handlers = []
        self.storage_handlers = []
        for i in range(self.num_processes):
            # set up pipes
            r, w = os.pipe()
            self.config['input'].append(os.fdopen(r, 'r'))
            self.inputs.append(os.fdopen(w, 'w'))

            r, w = os.pipe()
            self.config['output'].append(os.fdopen(w, 'w'))
            self.outputs.append(os.fdopen(r, 'r'))

            storage_handler = StorageHandler(i, [0.1, 0.1, 0.1], self.config)
            input_handler = InputHandler(i, storage_handler, self.config)
            self.input_handlers.append(input_handler)
            self.storage_handlers.append(storage_handler)

        self.handlers = self.input_handlers + self.storage_handlers
        for handler in self.handlers:
            handler.run()

        self.all_outputs += self.outputs
        self.all_inputs += self.inputs
        self.force_quit = False

    def run_commands(self, commands):
        for pid, command in commands:
            self.inputs[pid].write(command + '\n')
            self.inputs[pid].flush()

    def check_responses(self, responses):
        for pid, response in responses:
            line = self.outputs[pid].readline().rstrip()
            self.assertEqual(line, response)

    def tearDown(self):
        if self.force_quit:
            return

        for i in range(self.num_processes):
            self.inputs[i].write('exit\n')
        for i in range(self.num_processes):
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
