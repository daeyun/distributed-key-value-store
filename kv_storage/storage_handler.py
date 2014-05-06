import socket
import threading
import time
import random
from config import config
from helpers.network_helper import pack_message
from helpers.network_helper import unpack_message


class StorageHandler:
    def __init__(self, process_id, delay_times, _config=None):
        """
        Args:
            process_id: an index in the config file
            delay_times: process_id of this node
            _config: custom config values passed in for unit testing
        """
        self.MESSAGE_MAX_SIZE = 1024
        self.NUM_REPLICAS = 3
        self.local_storage = {}
        self.required_num_responses = {}
        self.version_num = {}
        self.replica_response_values = {}
        self.search_replica_values = {}

        if _config == None:
            self.config = config
            # Initialize the UDP socket.
            ip, _, port = self.config['hosts'][process_id]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            self.sock.bind((ip, port))
            self.is_testing = False
        else:
            # Unit testing mode
            from test import support
            self.config = _config
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            port = support.bind_port(self.sock)
            self.config['hosts'][process_id][2] = port
            self.input = self.config['input'][process_id]
            self.output = self.config['output'][process_id]
            self.is_testing = True

        self.process_id = process_id
        self.set_delay_times(delay_times)

    def set_delay_times(self, delay_times):
        replica_ids = self.get_replica_ids()
        self.delay_times = {}

        for idx, id in enumerate(replica_ids):
            self.delay_times[id] = delay_times[idx]

    def run(self):
        self.thread_in = threading.Thread(target=self.incoming_message_handler)
        self.thread_in.daemon = True

        self.thread_in.start()

    def join(self):
        self.thread_in.join()

    def incoming_message_handler(self):
        while True:
            data, _ = self.sock.recvfrom(self.MESSAGE_MAX_SIZE)
            msg_type, command, sender_id, data_array = unpack_message(data)
            # print('receiving', sender_id, '->', self.process_id, ' msg_type:', msg_type, ', command:', command, ', data_array:', data_array)

            if msg_type == 'exit':
                self.print_str('Server is shutting down.')
                self.sock.close()
                return
            elif msg_type == 'coordinator':
                self.process_coordinator_msg(command, sender_id, data_array)
            else:
                self.process_replica_msg(command, sender_id, data_array)

    def print_str(self, string, end='\n'):
        self.output.write(string + end)

    def process_coordinator_msg(self, command, sender_id, data_array):
        if command == 'get':
            key = data_array[0]
            level = data_array[1]
            request_id = data_array[2]

            request_key = (sender_id, request_id, key)
            if level == 1:  # 1: one
                self.required_num_responses[request_key] = 1
            else:  # 9: all
                self.required_num_responses[request_key] = self.NUM_REPLICAS

            self.get_value(key, sender_id, request_id)

        elif command == 'get_response':
            key = data_array[0]
            value = data_array[1]
            client_id = data_array[2]
            request_id = data_array[3]
            key_version_num = data_array[4]

            request_key = (client_id, request_id, key)
            if request_key in self.required_num_responses:
                count = self.required_num_responses[request_key]
                if count == 1:
                    del self.required_num_responses[request_key]
                    msg = "client,get_response,{},{}".format(self.process_id, value)
                    self.send_msg(msg, client_id, is_client=True)
                else:
                    self.required_num_responses[request_key] = count - 1

            if request_key not in self.replica_response_values:
                self.replica_response_values[request_key] = []
            self.replica_response_values[request_key].append((key_version_num, value, sender_id))
            
            if len(self.replica_response_values[request_key]) == self.NUM_REPLICAS:
                # (list of replica id that need to be update, version number, value)
                replica_ids, version_num, value = self.find_inconsistent_replicas(self.replica_response_values[request_key])

                # version_num is -1 if the key never existed
                if version_num > -1:
                    msg = "replica,repair,{},{},{},{}".format(self.process_id, key, value, version_num)
                    self.send_msg_concurrent(msg, replica_ids)

        elif command == 'insert':
            key = data_array[0]
            value = data_array[1]
            level = data_array[2]
            request_id = data_array[3]

            request_key = (sender_id, request_id, key)
            if level == 1:  # 1: one
                self.required_num_responses[request_key] = 1
            else:  # 9: all
                self.required_num_responses[request_key] = self.NUM_REPLICAS

            self.insert_key_value(key, value, sender_id, request_id)
        elif command == 'insert_response':
            key = data_array[0]
            result = data_array[1]
            client_id = data_array[2]
            request_id = data_array[3]
            request_key = (client_id, request_id, key)
            if request_key in self.required_num_responses:
                count = self.required_num_responses[request_key]
                if count == 1:
                    del self.required_num_responses[request_key]
                    msg = "client,insert_response,{},{}".format(self.process_id, result)
                    self.send_msg(msg, client_id, is_client=True)
                else:
                    self.required_num_responses[request_key] = count - 1

        elif command == 'update':
            key = data_array[0]
            value = data_array[1]
            level = data_array[2]
            request_id = data_array[3]

            request_key = (sender_id, request_id, key)
            if level == 1:  # 1: one
                self.required_num_responses[request_key] = 1
            else:  # 9: all
                self.required_num_responses[request_key] = self.NUM_REPLICAS

            self.update_key_value(key, value, sender_id, request_id)

        elif command == 'update_response':
            key = data_array[0]
            result = data_array[1]
            client_id = data_array[2]
            request_id = data_array[3]
            request_key = (client_id, request_id, key)
            if request_key in self.required_num_responses:
                count = self.required_num_responses[request_key]
                if count == 1:
                    del self.required_num_responses[request_key]
                    msg = "client,update_response,{},{}".format(self.process_id, result)
                    self.send_msg(msg, client_id, is_client=True)
                else:
                    self.required_num_responses[request_key] = count - 1

        elif command == 'delete':
            key = data_array[0]
            self.delete_key(key, sender_id)

        elif command == 'set_delay_times':
            self.set_delay_times(data_array[:self.NUM_REPLICAS])

        elif command == 'search':
            key = data_array[0]
            request_id = data_array[1]
            request_key = (sender_id, request_id, key)
            self.required_num_responses[request_key] = self.NUM_REPLICAS
            self.search_replica_values[request_key] = []
            self.search_key(key, sender_id, request_id)

        elif command == 'search_response':
            key = data_array[0]
            has_key = data_array[1]
            client_id = data_array[2]
            request_id = data_array[3]
            request_key = (client_id, request_id, key)
            self.search_replica_values[request_key].append((sender_id, has_key))

            if len(self.search_replica_values[request_key]) == self.NUM_REPLICAS:
                replica_list = self.search_replica_values[request_key]
                msg = "client,search_response,{},".format(self.process_id)
                msg += ','.join([str(pid) + "," + str(has_key) for pid, has_key in replica_list])
                self.send_msg(msg, client_id, is_client=True)
                del self.search_replica_values[request_key]

    def process_replica_msg(self, command, sender_id, data_array):
        if command == 'get':
            key = data_array[0]
            client_id = data_array[1]
            request_id = data_array[2]
            value = 'None'
            if key in self.local_storage:
                value = str(self.local_storage[key])

            if key in self.version_num:
                key_version_num = self.version_num[key]
            else:
                key_version_num = -1

            msg = "coordinator,get_response,{},{},{},{},{},{}".format(self.process_id, key, value, client_id, request_id, key_version_num)
            self.send_msg(msg, sender_id)

        elif command == 'insert':
            key = data_array[0]
            value = data_array[1]
            client_id = data_array[2]
            request_id = data_array[3]
            if key in self.local_storage:
                result = 0
            else:
                result = 1
                self.local_storage[key] = value
                if key not in self.version_num:
                    self.version_num[key] = 1
                else:
                    self.version_num[key] += 1
            msg = "coordinator,insert_response,{},{},{},{},{}".format(self.process_id, key, result, client_id, request_id)
            self.send_msg(msg, sender_id)

        elif command == 'update':
            key = data_array[0]
            value = data_array[1]
            client_id = data_array[2]
            request_id = data_array[3]
            if key in self.local_storage:
                result = 1
                self.local_storage[key] = value
                self.version_num[key] += 1
            else:
                result = 0
            msg = "coordinator,update_response,{},{},{},{},{}".format(self.process_id, key, result, client_id, request_id)
            self.send_msg(msg, sender_id)

        elif command == 'delete':
            key = data_array[0]
            client_id = data_array[1]
            if key in self.local_storage:
                del self.local_storage[key]
                self.version_num[key] += 1
                # TODO?: acknowledge delete
        elif command == 'repair':
            key = data_array[0]
            value = data_array[1]
            key_version_num = data_array[2]

            if (key not in self.version_num) or (key_version_num >= self.version_num[key]):
                self.version_num[key] = key_version_num

                if value != 'None':
                    self.local_storage[key] = value
                elif key in self.local_storage:
                    del self.local_storage[key]

        elif command == 'search':
            key = data_array[0]
            client_id = data_array[1]
            request_id = data_array[2]
            has_key = 0
            if key in self.local_storage:
                has_key = 1

            msg = "coordinator,search_response,{},{},{},{},{}".format(self.process_id, key, has_key, client_id, request_id)
            self.send_msg(msg, sender_id)


    def get_value(self, key, sender_id, request_id):
        replica_ids = self.get_replica_ids()
        msg = "replica,get,{},{},{},{}".format(self.process_id, key, sender_id, request_id)
        self.send_msg_concurrent(msg, replica_ids)

    def insert_key_value(self, key, value, sender_id, request_id):
        replica_ids = self.get_replica_ids()
        msg = "replica,insert,{},{},{},{},{}".format(self.process_id, key, value, sender_id, request_id)
        self.send_msg_concurrent(msg, replica_ids)

    def update_key_value(self, key, value, sender_id, request_id):
        replica_ids = self.get_replica_ids()
        msg = "replica,update,{},{},{},{},{}".format(self.process_id, key, value, sender_id, request_id)
        self.send_msg_concurrent(msg, replica_ids)

    def get_replica_ids(self, _pid=None):
        if _pid is None:
            pid = self.process_id
        else:
            pid = _pid

        replica_ids = []
        num_processes = len(self.config['hosts'])
        for i in range(self.NUM_REPLICAS):
            pid = (pid + 1) % num_processes
            replica_ids.append(pid)
        return replica_ids

    def delete_key(self, key, sender_id):
        replica_ids = self.get_replica_ids()
        msg = "replica,delete,{},{},{}".format(self.process_id, key, sender_id)
        for replica_id in replica_ids:
            self.send_msg(msg, replica_id)

    def search_key(self, key, sender_id, request_id):
        replica_ids = self.get_replica_ids()
        msg = "replica,search,{},{},{},{}".format(self.process_id, key, sender_id, request_id)
        for replica_id in replica_ids:
            self.send_msg(msg, replica_id)

    def send_msg(self, msg_str, target_pid, is_client=False):
        ip, client_port, port = self.config['hosts'][target_pid]
        if is_client:
            port = client_port
        msg = pack_message(msg_str)
        try:
            self.sock.sendto(msg, (ip, port))
        except OSError as e:
            pass

    def send_msg_delay(self, msg_str, target_pid, avg_delay):
        if not self.is_testing:
            delay = random.uniform(0, 2 * avg_delay)
        else:
            delay = avg_delay

        # print('send', self.process_id, '->', target_pid, 'with', delay, 'delay. message: ', msg_str)
        time.sleep(delay)
        self.send_msg(msg_str, target_pid)

    def send_msg_concurrent(self, msg_str, ids):
        for id in ids:
            tid = threading.Thread(target=self.send_msg_delay, args=(msg_str, id, self.delay_times[id]))
            tid.start()
    
    # Give a list of (key_version_num, value, sender_id),
    # return (list of replica id that need to be update, version number, value)
    def find_inconsistent_replicas(self, replica_values):
        max_version_num = -1
        max_version_copies = []
        for replica_value in replica_values:
            key_version_num, value, sender_id = replica_value
            if key_version_num > max_version_num:
                max_version_copies = [(value, sender_id)]
                max_version_num = key_version_num
            elif key_version_num == max_version_num:
                max_version_copies.append((value, sender_id))
                max_version_num = key_version_num

        value_frequencies = {}
        for max_version_copy in max_version_copies:
            value, sender_id = max_version_copy
            if value in value_frequencies:
                value_frequencies[value] += 1
            else:
                value_frequencies[value] = 1

        max_freq = -1
        max_freq_value = None
        for value, frequency in value_frequencies.items():
           if frequency > max_freq:
               max_freq = frequency
               max_freq_value = value

        replicas_to_be_updated = []
        for replica_value in replica_values:
            key_version_num, value, sender_id = replica_value
            if max_freq_value != value:
                replicas_to_be_updated.append(sender_id)

        return (replicas_to_be_updated, max_version_num, max_freq_value)
