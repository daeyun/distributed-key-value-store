from input_handler import InputHandler
from storage_handler import StorageHandler
from helpers.distribution_helper import kv_hash
import socket
import sys
from config import config


def main():
    if len(sys.argv) != 5:
        print('Usage: {} [process ID] [delay time 1] [delay time 2]'
              '[delay time 3]'.format(sys.argv[0]))
        exit(1)

    process_id = int(sys.argv[1])
    delay_times = [float(time_str) for time_str in sys.argv[2:5]]

    storage_handler = StorageHandler(process_id, delay_times)
    input_handler = InputHandler(process_id, storage_handler)

    storage_handler.run()
    input_handler.run()

    storage_handler.join()
    input_handler.join()


if __name__ == '__main__':
    main()
