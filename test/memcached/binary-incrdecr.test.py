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
############################ Diagnostics INCR/DECR ############################
#-----------------------------------------------------------------------------#
""")

# Test Increment
res = mc.flush()
res = mc.incr("x", 0, expire=0)
print 'res[0]["val"] == 0', res[0]["val"] == 0
res = mc.incr("x", 1, expire=0)
print 'res[0]["val"] == 1', res[0]["val"] == 1
res = mc.incr("x", 211, expire=0)
print 'res[0]["val"] == 212', res[0]["val"] == 212
res = mc.incr("x", 2**33, expire=0)
print 'res[0]["val"] == 8589934804', res[0]["val"] == 8589934804

print("""#------------------------------# increment error #----------------------------#""")
mc.set("issue48", "text", 0, 0)
res = mc.incr("issue48")
print 'res[0]["status"] == STATUS["DELTA_BADVAL"]', res[0]["status"] == STATUS["DELTA_BADVAL"]
check("issue48", 0, "text")
res = mc.decr("issue48")
print 'res[0]["status"] == STATUS["DELTA_BADVAL"]', res[0]["status"] == STATUS["DELTA_BADVAL"]
check("issue48", 0, "text")
print("""#------------------------------# test decrement #-----------------------------#""")
mc.flush()
res = mc.incr("x", 0, 0, 5)
print 'res[0]["val"] == 5', res[0]["val"] == 5
res = mc.decr("x")
print 'res[0]["val"] == 4', res[0]["val"] == 4
res = mc.decr("x", 211)
print 'res[0]["val"] == 0', res[0]["val"] == 0

print("""#---------------------------------# bug 220 #---------------------------------#""")
res = mc.set("bug220", "100", 0, 0)
ires = mc.incr("bug220", 999)
print 'res[0]["cas"] != ires[0]["cas"]', res[0]["cas"] != ires[0]["cas"]
print 'ires[0]["val"] == 1099', ires[0]["val"] == 1099
ires2 = mc.get("bug220")
print 'ires2[0]["cas"] == ires[0]["cas"]', ires2[0]["cas"] == ires[0]["cas"]
ires = mc.incr("bug220", 999)
print 'res[0]["cas"] != ires[0]["cas"]', res[0]["cas"] != ires[0]["cas"]
print 'ires[0]["val"] == 2098', ires[0]["val"] == 2098
ires2 = mc.get("bug220")
print 'ires2[0]["cas"] == ires[0]["cas"]', ires2[0]["cas"] == ires[0]["cas"]

print("""#----------------------------------# bug 21 #---------------------------------#""")
mc.add("bug21", "9223372036854775807", 0, 0)
res = mc.incr("bug21")
res[0]["val"] == 9223372036854775808
res = mc.incr("bug21")
res[0]["val"] == 9223372036854775809
res = mc.decr("bug21")
res[0]["val"] == 9223372036854775808
