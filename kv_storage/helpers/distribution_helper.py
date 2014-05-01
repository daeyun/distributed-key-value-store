import hashlib

def kv_hash(key):
    key = str(key)
    m = hashlib.md5()
    m.update(key)
    m.digest()
