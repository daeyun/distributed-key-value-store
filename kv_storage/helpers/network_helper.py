def pack_message(message):
    return message.encode('utf-8')


def unpack_message(message):
    return message.decode('utf-8')
