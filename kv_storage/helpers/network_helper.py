def pack_message(message):
    return message.encode('utf-8')


def unpack_message(message):
    try:
        msg_str = message.decode('utf-8')
    except:
        msg_str = message
    msg_array = msg_str.split(',')
    return (msg_array[0], [int(i) for i in msg_array[1:]])
