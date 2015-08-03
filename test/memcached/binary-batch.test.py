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
############################# Diagnostics multiGET ############################
#-----------------------------------------------------------------------------#
""")

# Diagnostics "MultiGet"
mc.add("xx", "ex", 5, 1)
mc.add("wye", "why", 5, 2)
mc.getq("xx", nosend=True)
mc.getq("wye", nosend=True)
mc.getq("zed", nosend=True)
res = mc.noop()
__check(res[0], 1, "ex")
__check(res[1], 2, "why")
print "len(res) == 3", len(res) == 3


