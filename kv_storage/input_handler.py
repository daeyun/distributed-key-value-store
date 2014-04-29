import threading
from config import config
import socket
from helpers.network_helper import pack_message


class InputHandler:
    def __init__(self, process_id):
        self.process_id = process_id

        # Initialize the UDP socket.
        ip, port, _ = config['hosts'][process_id]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind((ip, port))
        self.sock.settimeout(0.01)

    def run(self):
        self.thread = threading.Thread(target=self.keyboard_input_handler)
        self.thread.daemon = True
        self.thread.start()

    def join(self):
        self.thread.join()

    def keyboard_input_handler(self):
        """ This is a REPL thread. """
        while True:
            command_str = input('> ')
            input_words = command_str.split(' ')
            command = input_words[0]

            if command == 'get':
                pass
            elif command == 'insert':
                pass
            elif command == 'delete':
                pass
            elif command == 'update':
                pass
            elif command == 'send':  # this is for testing sockets
                target_pid = int(input_words[1])
                self.send_msg(' '.join(input_words[2:]), target_pid)
            elif command == 'exit':
                return
            else:
                print('ERROR: Unknown command')

    def send_msg(self, msg_str, target_pid):
        ip, _, port = config['hosts'][target_pid]
        msg = pack_message(msg_str)
        self.sock.sendto(msg, (ip, port))
