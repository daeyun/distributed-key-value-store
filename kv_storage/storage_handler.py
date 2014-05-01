import socket
import threading
from config import config
from helpers.network_helper import unpack_message


class StorageHandler:
    def __init__(self, process_id, delay_times):
        self.process_id = process_id
        self.delay_times = delay_times
        self.MESSAGE_MAX_SIZE = 1024
        self.NUM_REPLICAS = 3

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
        data, _ = self.sock.recvfrom(self.MESSAGE_MAX_SIZE)
        data_str = unpack_message(data)

        command = data_str[0]
        if command == 'get':
            pass
        print('message received: ' + data_str)

    def get_replica_ids(self, _pid = None):
        if _pid == None:
            pid = self.process_id
        else:
            pid = _pid

        replica_ids = []
        num_processes = len(config['hosts'])
        for i in range(self.NUM_REPLICAS):
            pid = (pid + 1) % num_processes
            replica_ids.append(pid)
        return replica_ids

    def outgoing_message_handler(self):
        pass
