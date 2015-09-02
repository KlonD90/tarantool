-- fiber.lua (internal file)

local fiber = require('fiber')
local ffi = require('ffi')
ffi.cdef[[
double
fiber_time(void);
uint64_t
fiber_time64(void);
double
fiber_now(void);
double
fiber_clock(void);
]]

local function fiber_time()
    return tonumber(ffi.C.fiber_time())
end

local function fiber_time64()
    return ffi.C.fiber_time64()
end

local function fiber_now()
    return ffi.C.fiber_now()
end

local function fiber_clock()
    return ffi.C.fiber_clock()
end

fiber.time = fiber_time
fiber.time64 = fiber_time64
fiber.now = fiber_now
fiber.clock = fiber_clock

return fiber
