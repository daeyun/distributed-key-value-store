import socket
import threading
import time
import random
from config import config
from helpers.network_helper import pack_message
from helpers.network_helper import unpack_message


class StorageHandler:
    def __init__(self, process_id, delay_times):
        self.process_id = process_id
        self.delay_times = delay_times
        self.MESSAGE_MAX_SIZE = 1024
        self.NUM_REPLICAS = 3
        self.local_storage = {2: 42}
        self.required_num_responses = {}

        # Initialize the UDP socket.
        ip, _, port = config['hosts'][process_id]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind((ip, port))
        #self.sock.settimeout(0.01)

    def run(self):
        self.thread_in = threading.Thread(target=self.incoming_message_handler)
        self.thread_out = threading.Thread(target=self.outgoing_message_handler)
        self.thread_in.daemon = True
        self.thread_out.daemon = True

        self.thread_in.start()
        self.thread_out.start()

    def join(self):
        self.thread_out.join()
        self.thread_in.join()

    def incoming_message_handler(self):
        while True:
            data, _ = self.sock.recvfrom(self.MESSAGE_MAX_SIZE)
            msg_type, command, sender_id, data_array = unpack_message(data)

            if msg_type == 'coordinator':
                self.process_coordinator_msg(command, sender_id, data_array)
            else:
                self.process_replica_msg(command, sender_id, data_array)

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

            request_key = (client_id, request_id, key)
            if request_key in self.required_num_responses:
                count = self.required_num_responses[request_key]
                if count == 1:
                    del self.required_num_responses[request_key]
                    msg = "client,get_response,{},{}".format(self.process_id, value)
                    self.send_msg(msg, client_id, is_client=True)
                else:
                    self.required_num_responses[request_key] = count - 1
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

    def process_replica_msg(self, command, sender_id, data_array):
        if command == 'get':
            key = data_array[0]
            client_id = data_array[1]
            request_id = data_array[2]
            value = 'None'
            if key in self.local_storage:
                value = str(self.local_storage[key])
            msg = "coordinator,get_response,{},{},{},{},{}".format(self.process_id, key, value, client_id, request_id)
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
            else:
                result = 0
            msg = "coordinator,update_response,{},{},{},{},{}".format(self.process_id, key, result, client_id, request_id)
            self.send_msg(msg, sender_id)
        elif command == 'delete':
            key = data_array[0]
            client_id = data_array[1]
            if key in self.local_storage:
                del self.local_storage[key]
                # TODO?: acknowledge delete

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
        num_processes = len(config['hosts'])
        for i in range(self.NUM_REPLICAS):
            pid = (pid + 1) % num_processes
            replica_ids.append(pid)
        return replica_ids

    def delete_key(self, key, sender_id):
        replica_ids = self.get_replica_ids()
        msg = "replica,delete,{},{},{}".format(self.process_id, key, sender_id)
        for replica_id in replica_ids:
            self.send_msg(msg, replica_id)

    def send_msg(self, msg_str, target_pid, is_client=False):
        ip, client_port, port = config['hosts'][target_pid]
        if is_client:
            port = client_port
        msg = pack_message(msg_str)
        self.sock.sendto(msg, (ip, port))

    def send_msg_delay(self, msg_str, target_pid, avg_delay):
        delay = random.uniform(0, 2 * avg_delay)
        time.sleep(delay)
        self.send_msg(msg_str, target_pid)

    def send_msg_concurrent(self, msg_str, ids):
        counter = 0
        for id in ids:
            tid = threading.Thread(target=self.send_msg_delay, args=(msg_str, id, self.delay_times[counter]))
            tid.start()
            counter += 1

    def outgoing_message_handler(self):
        pass
