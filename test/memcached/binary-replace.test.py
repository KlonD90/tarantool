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

print("""
#-----------------------------------------------------------------------------#
############################# Diagnostics REPLACE #############################
#-----------------------------------------------------------------------------#
""")

# Diagnostics for replace
empty("j")
res = mc.replace("j", "ex", 5, 19)
print 'res[0]["status"] == STATUS["KEY_ENOENT"]', res[0]["status"] == STATUS["KEY_ENOENT"]
empty("j")
mc.add("j", "ex2", 0, 14)
check("j", 14, "ex2")
mc.replace("j", "ex3", 0, 24)
check("j", 24, "ex3")
