import socket
import threading
from config import config
from helpers.network_helper import unpack_message


class StorageHandler:
    def __init__(self, process_id, delay_times):
        self.process_id = process_id
        self.delay_times = delay_times
        self.message_max_size = 1024

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
        data, _ = self.sock.recvfrom(self.message_max_size)
        data_str = unpack_message(data)
        print('message received: ' + data_str)

    def outgoing_message_handler(self):
        pass
