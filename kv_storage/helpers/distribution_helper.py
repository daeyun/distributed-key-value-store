import hashlib

def kv_hash(key, i=0):
    key = str(key).encode('utf-8')
    md5 = hashlib.md5()
    md5.update(key)
    md5.update(str(i).encode('utf-8'))
    int_hash = int(md5.hexdigest(), 16)

    m = 11
    return int_hash & ((1<<m)-1)
