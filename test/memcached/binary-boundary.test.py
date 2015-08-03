from llib.MemcachedBinaryConnection import MemcachedBinaryConnection as MBC
from llib.MemcachedBinaryConnection import STATUS, COMMANDS

mc = MBC("localhost", 11211)

def __check(res, flags, val):
    print res.get("flags", -1), flags
    print '(res.get("flags", -1) == flags)', (res.get("flags", -1) == flags)
    print res.get('val', None) + ' ::: ' + val
    print '(res.get("val", None) == val)', (res.get("val", None) == val)

def check(key, flags, val):
    res = mc.get(key)
    __check(res[0], flags, val)

def set(key, expire, flags, value):
    res = mc.set(key, value, expire, flags)
    check(key, flags, value)

def empty(key):
    res = mc.get(key)
    print(res[0]["status"] == STATUS["KEY_ENOENT"])

def delete(key, when):
    res = mc.delete(key)
    empty(key)

import time

print("""#---------------------# test protocol boundary overruns #---------------------#""")

for i in range(1900, 2100):
    print ("iteration I: %d" % i)
    key  = "test_key_%d" % i
    val  = "x" * i
    mc.setq(key, val, flags=82, nosend=True)
    mc.setq("alt_%s" % key, "blah", flags=82, nosend=True)
    data = "".join(mc.commands)
    mc.commands = []

    if (len(data) > 2024):
        for j in range(2024, min(2096, len(data))):
            mc.socket.sendall(data[:j])
            time.sleep(0.001)
            mc.socket.sendall(data[j:])
    else:
        mc.socket.sendall(data)
    check(key, 82, val)
    check("alt_%s" % key, 82, "blah")
