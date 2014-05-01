def pack_message(message):
    return message.encode('utf-8')


def unpack_message(message):
    try:
        msg_str = message.decode('utf-8')
    except:
        msg_str = message
    msg_array = msg_str.split(',')
    msg_type = msg_array[0]
    command = msg_array[1]
    sender_id = int(msg_array[2])
    data_array = [int(i) if i != 'None' else None for i in msg_array[3:]]
    return (msg_type, command, sender_id, data_array)
