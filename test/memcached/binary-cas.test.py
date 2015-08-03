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
################################ Diagnostics CAS ##############################
#-----------------------------------------------------------------------------#
""")

mc.flush()
res = mc.set("x", "bad value", 5, 19, cas=0x7FFFFFF)
print 'res[0]["status"] == STATUS["KEY_ENOENT"]', res[0]["status"] == STATUS["KEY_ENOENT"]

res = mc.add("x", "original value", 19, 5)
ires2 = mc.get("x")
print 'res[0]["cas"] == ires2[0]["cas"]', res[0]["cas"] == ires2[0]["cas"]
print 'ires2[0]["val"] == "original value"', ires2[0]["val"] == "original value"

res = mc.set("x", "broken value", 5, 19, cas=ires2[0]["cas"] + 1)
print 'res[0]["status"] == STATUS["KEY_EEXISTS"]', res[0]["status"] == STATUS["KEY_EEXISTS"]

res = mc.set("x", "new value", 5, 19, cas=ires2[0]["cas"])
ires = mc.get("x")
print 'ires[0]["val"] == "new value"', ires[0]["val"] == "new value"
print 'ires[0]["cas"] == res[0]["cas"]', ires[0]["cas"] == res[0]["cas"]

res = mc.set("x", "replay value", 5, 19, cas=ires2[0]["cas"])
print 'res[0]["status"] == STATUS["KEY_EEXISTS"]', res[0]["status"] == STATUS["KEY_EEXISTS"]
