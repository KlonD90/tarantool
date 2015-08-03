#!/usr/bin/env python
import logging
import bmemcached

logging.basicConfig(level=logging.DEBUG)

client = bmemcached.Client(('127.0.0.1:6380'))
print 'Must be true            : ', client.add('key', 'value1')
print 'Must return false       : ', client.add('key', 'value')
print 'Must be true            : ', client.replace('key', 'value2')
print 'Must be true            : ', client.delete('key')
print 'Must return false       : ', client.replace('key', 'value')
print 'Must be true            : ', client.set('key', 'value')
print 'Must be "value"         : ', client.get('key')
print 'Must be "value"         : ', client.get('key')
print 'Must be true            : ', client.delete('key')
print 'Must log error and None : ', client.get('key')
