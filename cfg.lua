box.cfg{
    wal_mode = 'none',
    logger_nonblock = false
}

require('console').listen(3302)

local mc = box.schema.create_space('memcached', { if_not_exists = true })
mc:create_index('primary', { if_not_exists = true, parts = {1, 'str'} })

box.schema.user.grant('guest', 'read,write,execute', 'universe')
