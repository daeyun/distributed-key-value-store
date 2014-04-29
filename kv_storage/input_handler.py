import threading


class InputHandler:
    def __init__(self):
        pass

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
            elif command == 'exit':
                return
            else:
                print('ERROR: Unknown command')
