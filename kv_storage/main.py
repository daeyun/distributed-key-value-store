from input_handler import InputHandler
from storage_handler import StorageHandler
import socket
import sys
from config import config


def main():
    if len(sys.argv) != 5:
        print('Usage: {} [process ID] [delay time 1] [delay time 2]'
              '[delay time 3]'.format(sys.argv[0]))
        exit(1)

    # TODO: make this cleaner
    process_id = int(sys.argv[1])
    delay_time_1 = float(sys.argv[2])
    delay_time_2 = float(sys.argv[3])
    delay_time_3 = float(sys.argv[4])
    delay_times = [delay_time_1, delay_time_2, delay_time_3]

    input_handler = InputHandler(process_id)
    storage_handler = StorageHandler(process_id, delay_times)

    input_handler.run()
    storage_handler.run()

    input_handler.join()
    storage_handler.join()

if __name__ == '__main__':
    main()
