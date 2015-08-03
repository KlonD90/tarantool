#ifndef   TARANTOOL_BOX_MEMCACHED_LAYER_H_INCLUDED
#define   TARANTOOL_BOX_MEMCACHED_LAYER_H_INCLUDED

void mc_process_set(struct memcached_msg *);
void mc_process_get(struct memcached_msg *);
void mc_process_del(struct memcached_msg *);
void mc_process_nop(struct memcached_msg *);
void mc_process_flush(struct memcached_msg *);
void mc_process_touch_or_gat(struct memcached_msg *);
void mc_process_version(struct memcached_msg *);
void mc_process_delta(struct memcached_msg *);
void mc_process_pend(struct memcached_msg *);

#endif /* TARANTOOL_BOX_MEMCACHED_LAYER_H_INCLUDED */
