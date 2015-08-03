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

print("""#---------------------------# flush and noop tests #--------------------------#""")

mc.flush()
mc.noop()[0]["op"] == 10

set("x", 5, 19, "somevalue")
delete("x", 0)

set("x", 5, 19, "somevaluex")
set("y", 5, 17, "somevaluey")
mc.flush()
empty("x")
empty("y")

print("""
#-----------------------------------------------------------------------------#
################################ Diagnostics ADD ##############################
#-----------------------------------------------------------------------------#
""")

empty("i")
mc.add("i", "ex", 10, 5)
check("i", 5, "ex")
res = mc.add("i", "ex2", 10, 5)
print(res[0]["status"] == STATUS["KEY_EEXISTS"])
check("i", 5, "ex")

print("""
#-----------------------------------------------------------------------------#
################################# Toobig Tests ################################
#-----------------------------------------------------------------------------#
""")

# # Toobig tests
# empty("toobig")
# mc.set("toobig", "not too big", 10, 10)
# res = mc.set("toobig", "x" * 1024*1024*2, 10, 10);
# print(res[0]["status"] == STATUS["E2BIG"])
# empty("toobig")

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
mc.add("j", "ex2", 5, 14)
check("j", 14, "ex2")
mc.replace("j", "ex3", 5, 24)
check("j", 24, "ex3")

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

print("""
#-----------------------------------------------------------------------------#
############################## Diagnostics Touch ##############################
#-----------------------------------------------------------------------------#
""")

import time

# mc.flush()
# mc.set("totouch", "toast", 1, 0)
# res = mc.touch("totouch", 10)
# time.sleep(2)
# check("totouch", 0, "toast")
#
# mc.set("totouch", "toast2", 1, 0)
# res = mc.gat("totouch", 10)
# print 'res[0]["val"] == "toast2"', res[0]["val"] == "toast2"
# time.sleep(2)
# check("totouch", 0, "toast2")
#
# mc.set("totouch", "toast3", 1, 0)
# mc.touch("totouch", 1)
# time.sleep(3)
# empty("totouch")

def check_empty_response(con):
    res = con.noop()
    print "len(res) == 1", len(res) == 1
    print 'res[0]["op"] == COMMANDS["noop"][0]', res[0]["op"] == COMMANDS["noop"][0]

print("""#--------------------------------# silent get #-------------------------------#""")
key, val, flags = "silentset", "siltensetval", 82
empty(key)
mc.setq(key, val, flags=flags, expire=0, nosend=True)
check_empty_response(mc)
check(key, flags, val)

print("""#--------------------------------# silent put #-------------------------------#""")
key, val, flags = "silentadd", "siltenaddval", 82
empty(key)
mc.addq(key, val, flags=flags, expire=0, nosend=True)
check_empty_response(mc)
check(key, flags, val)

print("""#------------------------------# silent replace #-----------------------------#""")
key, val, flags = "silentreplace", "somevalue", 829
empty(key)
mc.add(key, "xval", 0, 831)
check(key, 831, "xval")
mc.replaceq(key, val, flags=flags, nosend=True)
check_empty_response(mc)
check(key, flags, val)

print("""#------------------------------# silent delete #------------------------------#""")
key, val, flags = "silentdelete", "someval", 19
empty(key)
mc.set(key, val, flags=flags, expire=0)
check(key, flags, val)
mc.deleteq(key, nosend=True)
empty(key)

print("""#-----------------------------# silent increment #----------------------------#""")
key, opaque = "silentincr", 98428747
empty(key)
mc.incrq(key, 0, 0, 0, nosend=True)
res = mc.incr (key, 0)
print 'res[0]["value"] == 0', res[0]["val"] == 0
mc.incrq(key, 8, 0, 0, nosend=True)
res = mc.incr (key, 0)
print 'res[0]["value"] == 8', res[0]["val"] == 8

# Silent decrement
print("""#-----------------------------# silent decrement #----------------------------#""")
key, opaque = "silentdecr", 98428747
empty(key)
mc.decrq(key, 0, 0, 185, nosend=True)
res = mc.decr (key, 0)
print 'res[0]["value"] == 185', res[0]["val"] == 185
mc.decrq(key, 8, 0, 0, nosend=True)
res = mc.decr (key, 0)
print 'res[0]["value"] == 177', res[0]["val"] == 177

print("""#-------------------------------# silent flush #------------------------------#""")
# stat1 = mc.stats()[0]
set("x", 5, 19, "somevaluex")
set("y", 5, 19, "somevaluey")
mc.flushq(nosend=True)
empty("x")
empty("y")

# stat2 = mc.stats()[0]
# print ('stat1["cmd_flush"] + 1 == stat2["cmd_flush"] ',
#         stat1["cmd_flush"] + 1 == stat2["cmd_flush"])

print("""#----------------------------# diagnostics append #---------------------------#""")
key, value, suffix = "appendkey", "some value", " more"
set(key, 8, 19, value)
mc.append(key, suffix)
check(key, 19, value + suffix)

print("""#---------------------------# diagnostics prepend #---------------------------#""")
key, value, prefix = "prependkey", "some value", "more "
set(key, 8, 19, value)
mc.prepend(key, prefix)
check(key, 19, prefix + value)

print("""#------------------------------# silent append #------------------------------#""")
key, value, suffix = "appendqkey", "some value", " more"
set(key, 8, 19, value)
mc.appendq(key, suffix, nosend=True)
check_empty_response(mc)
check(key, 19, value + suffix)

print("""#------------------------------# silent prepend #-----------------------------#""")
key, value, prefix = "prependqkey", "some value", "more "
set(key, 8, 19, value)
mc.prependq(key, prefix, nosend=True)
check_empty_response(mc)
check(key, 19, prefix + value)

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
