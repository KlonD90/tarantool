#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>
#include <math.h>

#include "msgpuck/msgpuck.h"
#include "iobuf.h"
#include "fiber.h"
#include "say.h"
#include "memory.h"
#include "cbus.h"

#include "box.h"
#include "error.h"
#include "tuple.h"
#include "index.h"

#include "main.h"

#include "memcached_constants.h"
#include "memcached.h"
#include "memcached_layer.h"

const uint32_t  mc_space_id = 512;
uint64_t        _cas        = 0;
static uint64_t flush       = 0;

/* MEMCACHED_CONVERTION_FUNCTIONS */

#define xisspace(c) isspace((unsigned char)c)

bool safe_strtoull(const char *begin, const char *end, uint64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    unsigned long long ull = strtoull(begin, &endptr, 10);
    if ((errno == ERANGE) || (begin == endptr) || (endptr != end)) {
	say_info("quit 1");
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != begin) || endptr == end) {
        if ((long long) ull < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(begin, '-') != NULL) {
		say_info("quit 2");
                return false;
            }
        }
        *out = ull;
        return true;
    }
    say_info("quit 3");
    return false;
}

bool safe_strtoll(const char *begin, const char *end, int64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long long ll = strtoll(begin, &endptr, 10);
    if ((errno == ERANGE) || (begin == endptr) || (endptr != end)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != begin)) {
        *out = ll;
        return true;
    }
    return false;
}

bool safe_strtoul(const char *str, uint32_t *out) {
    char *endptr = NULL;
    unsigned long l = 0;
    assert(out);
    assert(str);
    *out = 0;
    errno = 0;

    l = strtoul(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long) l < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = l;
        return true;
    }

    return false;
}

bool safe_strtol(const char *str, int32_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long l = strtol(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }
    return false;
}


/*
 * default exptime is 30*24*60*60 seconds
 * \* 1000000 to convert it to usec (need this precision)
 **/
#define DEFAULT_EXPTIME (30*24*60*60*1000000LL)

static __attribute__((unused)) void
memcached_dump_hdr(struct memcached_hdr *hdr) {
	if (!hdr) return;
	say_info("memcached package");
	say_info("magic:     0x%" PRIX8,        hdr->magic);
	say_info("cmd:       0x%" PRIX8,        hdr->cmd);
	if (hdr->key_len > 0)
		say_info("key_len:   %" PRIu16, hdr->key_len);
	if (hdr->ext_len > 0)
		say_info("ext_len:   %" PRIu8,  hdr->ext_len);
	say_info("tot_len:   %" PRIu32,         hdr->tot_len);
	say_info("opaque:    0x%" PRIX32,       betoh32(hdr->opaque));
	say_info("cas:       %" PRIu64,         hdr->cas);
}

static inline uint64_t
get_current_time() {
	return floor(fiber_time() * 1000000);
}

static inline uint64_t
convert_exptime (uint64_t exptime) {
	if (exptime == 0) return 0; /* 0 means never expire */
	if (exptime < DEFAULT_EXPTIME)
		return get_current_time() + exptime * 1000000;
	return exptime * 1000000;
}

static inline int
is_expired (const char *key, uint32_t key_len,
	    uint64_t exptime, uint64_t time) {
	(void )time;
	uint64_t curtime = get_current_time();
	/* Expired by flush */
	if (flush <= curtime && time <= flush) {
		say_info("key '%.*s' expired by flush", key_len, key);
		say_info("flush time %lu, current time %lu, time of creation %lu",
			 flush, curtime, time);
		return 1;
	}
	/* Expired by TTL */
	if (exptime != 0 && exptime < curtime) {
		say_info("key '%.*s' expired by expire", key_len, key);
		say_info("exptime is %lu, current time %lu",
			 exptime, curtime);
		return 1;
	}
	return 0;
}

static inline int
is_expired_tuple(box_tuple_t *tuple) {
	const char *pos  = box_tuple_field(tuple, 0);
	uint32_t key_len = 0;
	const char *key  = mp_decode_str(&pos, &key_len);
	uint64_t exptime = mp_decode_uint(&pos);
	uint64_t time    = mp_decode_uint(&pos);
	return is_expired(key, key_len, exptime, time);
}

/* This function swaps byte order, so ... */
static __attribute__ ((unused)) void
write_header(struct obuf *out, struct memcached_hdr *hdr) {
	hdr->status    = htobe16(hdr->status);
	hdr->key_len   = htobe16(hdr->key_len);
	hdr->tot_len   = htobe32(hdr->tot_len);
	hdr->opaque    = htobe32(hdr->opaque);
	hdr->cas       = htobe64(hdr->cas);
	obuf_dup(out, hdr, sizeof(struct memcached_hdr));
}

static __attribute__ ((unused)) void
copy_response_header(struct memcached_hdr *hdri, struct memcached_hdr *hdro) {
	memcpy(hdro, hdri, sizeof(struct memcached_hdr));
	hdro->magic   = MEMCACHED_BIN_RESPONSE;
	hdro->key_len = 0;
	hdro->ext_len = 0;
	hdro->tot_len = 0;
	hdro->cas     = 0;
	hdro->status  = MEMCACHED_BIN_RES_OK;
}

static void
write_output(struct obuf *out, struct memcached_hdr *hdri,
	     uint16_t err, uint64_t cas,
	     uint8_t ext_len, uint16_t key_len, uint32_t val_len,
	     const char *ext, const char *key, const char *val
	     ) {
	struct memcached_hdr hdro;
	memcpy(&hdro, hdri, sizeof(struct memcached_hdr));
	hdro.magic   = MEMCACHED_BIN_RESPONSE;
	hdro.ext_len = ext_len;
	hdro.key_len = htobe16(key_len);
	hdro.status  = htobe16(err);
	hdro.tot_len = htobe32(ext_len + key_len + val_len);
	hdro.opaque  = htobe32(hdro.opaque);
	hdro.cas     = htobe64(cas);
	obuf_dup(out, &hdro, sizeof(struct memcached_hdr));
	if (ext && ext_len > 0) obuf_dup(out, ext, ext_len);
	if (key && key_len > 0) obuf_dup(out, key, key_len);
	if (val && val_len > 0) obuf_dup(out, val, val_len);
}

static int
mc_insert_tuple(const char *kpos, uint32_t klen, uint64_t expire,
		const char *vpos, uint32_t vlen, uint64_t cas,
		uint32_t flags)
{
	uint64_t time = get_current_time();
	uint32_t len = mp_sizeof_array(6)      +
		       mp_sizeof_str  (klen)   +
		       mp_sizeof_uint (expire) +
		       mp_sizeof_uint (time)   +
		       mp_sizeof_str  (vlen)   +
		       mp_sizeof_uint (cas)    +
		       mp_sizeof_uint (flags);
	char *begin  = (char *)region_alloc(&fiber()->gc, len);
	char *end    = NULL;
	end = mp_encode_array(begin, 6);
	end = mp_encode_str  (end, kpos, klen);
	end = mp_encode_uint (end, expire);
	end = mp_encode_uint (end, time);
	end = mp_encode_str  (end, vpos, vlen);
	end = mp_encode_uint (end, cas);
	end = mp_encode_uint (end, flags);
	return box_replace(mc_space_id, begin, end, NULL);
}

static void
mc_process_error(struct obuf *out, struct memcached_hdr *hdr,
		 uint16_t err, const char *errstr) {
	if (!errstr) {
		switch (err) {
		case MEMCACHED_BIN_RES_ENOMEM:
			errstr = "Out of memory";
			break;
		case MEMCACHED_BIN_RES_UNKNOWN_COMMAND:
			errstr = "Unknown command";
			break;
		case MEMCACHED_BIN_RES_KEY_ENOENT:
			errstr = "Not found";
			break;
		case MEMCACHED_BIN_RES_EINVAL:
			errstr = "Invalid arguments";
			break;
		case MEMCACHED_BIN_RES_KEY_EEXISTS:
			errstr = "Data exists for key.";
			break;
		case MEMCACHED_BIN_RES_E2BIG:
			errstr = "Too large.";
			break;
		case MEMCACHED_BIN_RES_DELTA_BADVAL:
			errstr = "Non-numeric server-side value for incr or decr";
			break;
		case MEMCACHED_BIN_RES_NOT_STORED:
			errstr = "Not stored.";
			break;
		case MEMCACHED_BIN_RES_AUTH_ERROR:
			errstr = "Auth failure.";
			break;
		default:
			say_error("UNHANDLED ERROR: %d", err);
			assert(false);
			errstr = "UNHANDLED ERROR";
		}
	}
	size_t len = 0;
	if (errstr) len = strlen(errstr);
	write_output(out, hdr, err, 0, 0, 0, len, NULL, NULL, errstr);
}

static void
mc_process_internal_error(struct obuf *out, struct memcached_hdr *hdr) {
	const box_error_t *err = box_error_last();
	uint16_t       errcode = box_error_code(err);
	const char     *errstr = box_error_message(err);
	switch(errcode) {
	case (ER_MEMORY_ISSUE):
		errcode = MEMCACHED_BIN_RES_ENOMEM;
		errstr  = NULL;
		break;
	case (ER_TUPLE_NOT_FOUND):
		errcode = MEMCACHED_BIN_RES_KEY_ENOENT;
		errstr  = NULL;
		break;
	case (ER_TUPLE_FOUND):
		errcode = MEMCACHED_BIN_RES_KEY_EEXISTS;
		errstr  = NULL;
		break;
	default:
		break;
	}
	mc_process_error(out, hdr, errcode, errstr);
}

static inline const char *
mc_get_command_name(uint8_t op) {
	const char *cmd = NULL;
	switch(op) {
	case (MEMCACHED_BIN_CMD_GET):
		cmd = "GET";
		break;
	case (MEMCACHED_BIN_CMD_SET):
		cmd = "SET";
		break;
	case (MEMCACHED_BIN_CMD_ADD):
		cmd = "ADD";
		break;
	case (MEMCACHED_BIN_CMD_REPLACE):
		cmd = "REPLACE";
		break;
	case (MEMCACHED_BIN_CMD_DELETE):
		cmd = "DELETE";
		break;
	case (MEMCACHED_BIN_CMD_INCR):
		cmd = "INCR";
		break;
	case (MEMCACHED_BIN_CMD_DECR):
		cmd = "DECR";
		break;
	case (MEMCACHED_BIN_CMD_QUIT):
		cmd = "QUIT";
		break;
	case (MEMCACHED_BIN_CMD_FLUSH):
		cmd = "FLUSH";
		break;
	case (MEMCACHED_BIN_CMD_GETQ):
		cmd = "GETQ";
		break;
	case (MEMCACHED_BIN_CMD_NOOP):
		cmd = "NOOP";
		break;
	case (MEMCACHED_BIN_CMD_VERSION):
		cmd = "VERSION";
		break;
	case (MEMCACHED_BIN_CMD_GETK):
		cmd = "GETK";
		break;
	case (MEMCACHED_BIN_CMD_GETKQ):
		cmd = "GETKQ";
		break;
	case (MEMCACHED_BIN_CMD_APPEND):
		cmd = "APPEND";
		break;
	case (MEMCACHED_BIN_CMD_PREPEND):
		cmd = "PREPEND";
		break;
	case (MEMCACHED_BIN_CMD_STAT):
		cmd = "STAT";
		break;
	case (MEMCACHED_BIN_CMD_SETQ):
		cmd = "SETQ";
		break;
	case (MEMCACHED_BIN_CMD_ADDQ):
		cmd = "ADDQ";
		break;
	case (MEMCACHED_BIN_CMD_REPLACEQ):
		cmd = "REPLACEQ";
		break;
	case (MEMCACHED_BIN_CMD_DELETEQ):
		cmd = "DELETEQ";
		break;
	case (MEMCACHED_BIN_CMD_INCRQ):
		cmd = "INCRQ";
		break;
	case (MEMCACHED_BIN_CMD_DECRQ):
		cmd = "DECRQ";
		break;
	case (MEMCACHED_BIN_CMD_QUITQ):
		cmd = "QUITQ";
		break;
	case (MEMCACHED_BIN_CMD_FLUSHQ):
		cmd = "FLUSHQ";
		break;
	case (MEMCACHED_BIN_CMD_APPENDQ):
		cmd = "APPENDQ";
		break;
	case (MEMCACHED_BIN_CMD_PREPENDQ):
		cmd = "PREPENDQ";
		break;
	case (MEMCACHED_BIN_CMD_TOUCH):
		cmd = "TOUCH";
		break;
	case (MEMCACHED_BIN_CMD_GAT):
		cmd = "GAT";
		break;
	case (MEMCACHED_BIN_CMD_GATQ):
		cmd = "GATQ";
		break;
	case (MEMCACHED_BIN_CMD_GATK):
		cmd = "GATK";
		break;
	case (MEMCACHED_BIN_CMD_GATKQ):
		cmd = "GATKQ";
		break;
	case (MEMCACHED_BIN_CMD_SASL_LIST_MECHS):
		cmd = "SASL_LIST_MECHS";
		break;
	case (MEMCACHED_BIN_CMD_SASL_AUTH):
		cmd = "SASL_AUTH";
		break;
	case (MEMCACHED_BIN_CMD_SASL_STEP):
		cmd = "SASL_STEP";
		break;
	default:
		cmd = "UNEXPECTED";
		break;
	}
	return cmd;
}

/*
 * Tuple schema is:
 *
 * - key
 * - exptime - expire time
 * - time - time of creation/latest access
 * - value
 * - cas
 * - flags
 */
void
mc_process_set(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	// say_info("%p %p %p", b->ext, b->key, b->val);
	assert(b->ext != NULL && b->key != NULL && b->val != NULL);
	say_info("%s '%.*s' '%.*s'", mc_get_command_name(h->cmd), b->key_len,
		  b->key, b->val_len, b->val);
	struct memcached_set_ext *ext = (struct memcached_set_ext *)b->ext;
	ext->flags = betoh32(ext->flags);
	uint64_t exptime = convert_exptime(betoh32(ext->expire));
	uint64_t cas     = _cas++;
	uint32_t len = mp_sizeof_array(1) +
		       mp_sizeof_str  (b->key_len);
	char *begin  = (char *) region_alloc(&fiber()->gc, len);
	char *end = NULL;
	box_tuple_t *tuple = NULL;
	end = mp_encode_array(begin, 1);
	end = mp_encode_str  (end, b->key, b->key_len);
	if (box_index_get(mc_space_id, 0, begin, end, &tuple) == -1) {
		return mc_process_internal_error(out, h);
	}
	if (h->cmd == MEMCACHED_BIN_CMD_REPLACE &&
			(tuple == NULL || is_expired_tuple(tuple))) {
		return mc_process_error(out, h,
				MEMCACHED_BIN_RES_KEY_ENOENT, NULL);
	} else if (h->cmd == MEMCACHED_BIN_CMD_ADD &&
			!(tuple == NULL || is_expired_tuple(tuple))) {
		return mc_process_error(out, h,
				MEMCACHED_BIN_RES_KEY_EEXISTS, NULL);
	} else if (h->cas != 0) {
		if (!tuple || is_expired_tuple(tuple)) {
			say_info("CAS is there, but no tuple");
			return mc_process_error(out, h,
				MEMCACHED_BIN_RES_KEY_ENOENT, NULL);
		} else if (tuple) {
			const char *pos   = box_tuple_field(tuple, 4);
			uint64_t cas_prev = mp_decode_uint(&pos);
			if (cas_prev != h->cas) {
				say_info("CAS is there, tuple is there, no match");
				return mc_process_error(out, h,
					MEMCACHED_BIN_RES_KEY_EEXISTS, NULL);
			}
		}
	}
	if (mc_insert_tuple(b->key, b->key_len, exptime, b->val, b->val_len,
			    cas, ext->flags) == -1) {
		mc_process_internal_error(out, h);
	} else {
		if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
			write_output(out, h, MEMCACHED_BIN_RES_OK, cas, 0, 0,
				     0, NULL, NULL, NULL);
		}
	}
}

void
mc_process_get(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->ext == NULL && b->key != NULL && b->val == NULL);
	say_info("%s '%.*s'", mc_get_command_name(h->cmd), b->key_len, b->key);
	uint32_t len = mp_sizeof_array(1) +
		       mp_sizeof_str  (b->key_len);
	char *begin = (char *) region_alloc(&fiber()->gc, len);
	char *end   = mp_encode_array(begin, 1);
	      end   = mp_encode_str  (end, b->key, b->key_len);
	box_tuple_t *tuple = NULL;
	if (box_index_get(mc_space_id, 0, begin, end, &tuple) == -1) {
		mc_process_internal_error(out, h);
	} else if (tuple != NULL && !is_expired_tuple(tuple)) {
		struct memcached_get_ext ext;
		uint32_t vlen = 0, klen = 0;
		const char *pos  = box_tuple_field(tuple, 0);
		const char *kpos = mp_decode_str(&pos, &klen);
		mp_next(&pos); mp_next(&pos);
		const char *vpos = mp_decode_str(&pos, &vlen);
		uint64_t cas     = mp_decode_uint(&pos);
		uint32_t flags   = mp_decode_uint(&pos);
		if (h->cmd == MEMCACHED_BIN_CMD_GET) {
			kpos = NULL;
			klen = 0;
		}
		ext.flags = htobe32(flags);
		write_output(out, h, MEMCACHED_BIN_RES_OK, cas,
			     sizeof(struct memcached_get_ext), 0, vlen,
			     (const char *)&ext, kpos, vpos);
	} else {
		if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
			mc_process_error(out, h, MEMCACHED_BIN_RES_KEY_ENOENT,
					 NULL);
		}
	}
}

void
mc_process_del(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->ext == NULL && b->key != NULL && b->val == NULL);
	uint32_t len = mp_sizeof_array(1) +
		       mp_sizeof_str  (b->key_len);
	char *begin = (char *) region_alloc(&fiber()->gc, len);
	char *end   = mp_encode_array(begin, 1);
	      end   = mp_encode_str  (end, b->key, b->key_len);
	box_tuple_t *tuple = NULL;
	if (box_delete(mc_space_id, 0, begin, end, &tuple) == -1) {
		mc_process_internal_error(out, h);
	} else if (tuple != NULL) {
		if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
			write_output(out, h, MEMCACHED_BIN_RES_OK, 0, 0, 0, 0,
				     NULL, NULL, NULL);
		}
	} else {
		mc_process_error(out, h, MEMCACHED_BIN_RES_KEY_ENOENT, NULL);
	}
}

void
mc_process_version(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->ext == NULL && b->key == NULL && b->val == NULL);
	const char *vers = tarantool_version();
	int vlen = strlen(vers);
	if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
		write_output(out, h, MEMCACHED_BIN_RES_OK, 0, 0, 0, vlen, NULL,
			     NULL, vers);
	}
}

void
mc_process_nop(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->ext == NULL && b->key == NULL && b->val == NULL);
	if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
		write_output(out, h, MEMCACHED_BIN_RES_OK, 0, 0, 0, 0, NULL,
			     NULL, NULL);
	}
}

void
mc_process_flush(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->key == NULL && b->val == NULL);
	struct memcached_flush_ext *ext = (struct memcached_flush_ext *)b->ext;
	uint64_t exptime = 0;
	if (ext != NULL) exptime = convert_exptime(betoh32(ext->expire));
	flush = (exptime > 0 ? exptime : get_current_time());
	if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
		write_output(out, h, MEMCACHED_BIN_RES_OK, 0, 0, 0, 0, NULL,
			     NULL, NULL);
	}
}

void
mc_process_touch_or_gat(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->ext != NULL && b->key != NULL && b->val == NULL);
	struct memcached_touch_ext *ext = (struct memcached_touch_ext *)b->ext;
	uint64_t exptime = convert_exptime(betoh32(ext->expire));
	uint64_t current = get_current_time();

	uint32_t len  = mp_sizeof_array (2)   +
			mp_sizeof_array (3)   +
			mp_sizeof_str   (1)   +
			mp_sizeof_uint  (1)   +
			mp_sizeof_uint  (exptime) +
			mp_sizeof_array (3)   +
			mp_sizeof_str   (1)   +
			mp_sizeof_uint  (2)   +
			mp_sizeof_array (1)   +
			mp_sizeof_str   (b->key_len);
	char *begin  = (char *) region_alloc(&fiber()->gc, len);
	char *end = NULL, *key = NULL;
	/* Encode  */
	end = mp_encode_array(begin, 2);
	/* Encode expire update */
	end = mp_encode_array(end, 3);
	end = mp_encode_str  (end, "=", 1);
	end = mp_encode_uint (end, 1);
	end = mp_encode_uint (end, exptime);
	/* Encode tuple touch time update */
	end = mp_encode_array(end, 3);
	end = mp_encode_str  (end, "=", 1);
	end = mp_encode_uint (end, 2);
	end = key = mp_encode_uint (end, current);
	/* Encode key for update */
	end = mp_encode_array(end, 1);
	end = mp_encode_str  (end, b->key, b->key_len);

	box_tuple_t *tuple = NULL;
	if (box_index_get(mc_space_id, 0, key, end, &tuple) == -1) {
		return mc_process_internal_error(out, h);
	} else if (tuple == NULL || is_expired_tuple(tuple)) {
		if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
			mc_process_error(out, h,
					MEMCACHED_BIN_RES_KEY_ENOENT, NULL);
		}
		return;
	}

	uint32_t vlen = 0, klen = 0;
	const char *kpos = NULL, *vpos = NULL;
	if (h->cmd >= MEMCACHED_BIN_CMD_GAT &&
	    h->cmd <= MEMCACHED_BIN_CMD_GATKQ) {
		const char *pos  = box_tuple_field(tuple, 0);
		kpos = mp_decode_str(&pos, &klen);
		if (h->cmd >= MEMCACHED_BIN_CMD_GATK) {
			mp_next(&pos); mp_next(&pos);
			vpos = mp_decode_str(&pos, &vlen);
		}
	}

	/* Tuple can't be NULL, because we already found this element */
	if (box_update(mc_space_id, 0, key, end, begin, key, 0, &tuple) == -1) {
		mc_process_internal_error(out, h);
	} else {
		write_output(out, h, MEMCACHED_BIN_RES_OK, 0,
			     0, klen, vlen,
			     NULL, kpos, vpos);
	}
}

void
mc_process_delta(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->ext != NULL && b->key != NULL && b->val == NULL);
	struct memcached_delta_ext *ext = (struct memcached_delta_ext *)b->ext;
	ext->expire  = betoh32(ext->expire);
	ext->delta   = betoh64(ext->delta);
	ext->initial = betoh64(ext->initial);
	say_info("%s '%.*s' by %lu", mc_get_command_name(h->cmd), b->key_len,
		 b->key, ext->delta);
	uint32_t len = mp_sizeof_array(1) +
		       mp_sizeof_str  (b->key_len);
	char *begin = (char *) region_alloc(&fiber()->gc, len);
	char *end   = mp_encode_array(begin, 1);
	      end   = mp_encode_str  (end, b->key, b->key_len);
	box_tuple_t *tuple = NULL;
	uint64_t val = 0;
	uint64_t cas = _cas++;
	const char *vpos = NULL;
	uint32_t    vlen = 0;
	char        strval[22]; uint8_t strvallen = 0;
	if (box_index_get(mc_space_id, 0, begin, end, &tuple) == -1) {
		return mc_process_internal_error(out, h);
	} else if (tuple == NULL || is_expired_tuple(tuple)) {
		if (ext->expire == 0xFFFFFFFFLL) {
			return mc_process_error(out, h,
					MEMCACHED_BIN_RES_KEY_ENOENT, NULL);
		} else {
			ext->expire = 0;
			val = ext->initial;
			/* Insert value */
			strvallen = snprintf(strval, 21, "%lu", val);
			int retval = mc_insert_tuple(b->key, b->key_len,
					ext->expire, (const char *)strval,
					strvallen, cas, 0);
			if (retval == -1) {
				return mc_process_internal_error(out, h);
			}
		}
	} else {
		if (ext->expire == 0xFFFFFFFFLL) {
			ext->expire = 0;
		}
		const char *pos = box_tuple_field(tuple, 0);
		mp_next(&pos); mp_next(&pos); mp_next(&pos);
		vpos = mp_decode_str(&pos, &vlen);
		say_info("value before '%.*s', %lu", vlen, vpos, val);
		if (!safe_strtoull(vpos, vpos + vlen, &val)) {
			say_info("ERROR DELTA_BADVAL");
			return mc_process_error(out, h,
					MEMCACHED_BIN_RES_DELTA_BADVAL, NULL);
		}
		if (h->cmd == MEMCACHED_BIN_CMD_INCR ||
		    h->cmd == MEMCACHED_BIN_CMD_INCRQ) {
			val += ext->delta;
		} else if (ext->delta > val) {
			say_info("becoming null");
			val = 0;
		} else {
			val -= ext->delta;
		}
		say_info("value after %lu", val);
		/* Insert value */
		strvallen = snprintf(strval, 21, "%lu", val);
		if (mc_insert_tuple(b->key, b->key_len,
				ext->expire, (const char *)strval,
				strvallen, cas, 0) == -1) {
			return mc_process_internal_error(out, h);
		}
	}
	val = htobe64(val);
	/* Send response */
	if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
		write_output(out, h, MEMCACHED_BIN_RES_OK, cas, 0, 0,
			     sizeof(val), NULL, NULL, (const char *)&val);
	}
}

void
mc_process_pend(struct memcached_msg *msg)
{
	/* default declarations */
	struct memcached_hdr  *h = &msg->hdr;
	struct memcached_body *b = &msg->body;
	struct obuf *out = &(msg->iobuf->out);

	assert(b->ext == NULL && b->key != NULL && b->val != NULL);
	uint64_t cas = _cas++;
	uint32_t len  = mp_sizeof_array (2)      +
			/* splice (app/prepend) operation */
			mp_sizeof_array (5)      +
			mp_sizeof_str   (1)      +
			mp_sizeof_uint  (3)      +
			/* in case of prepend */
			mp_sizeof_uint  (1)      +
			/* in case of append */
			mp_sizeof_int   (-1)     +
			mp_sizeof_uint  (0)      +
			mp_sizeof_str   (b->val_len) +
			/* set cas */
			mp_sizeof_array (3)      +
			mp_sizeof_str   (1)      +
			mp_sizeof_uint  (4)      +
			mp_sizeof_uint  (cas)    +
			mp_sizeof_array (1)      +
			mp_sizeof_str   (b->key_len);
	char *begin  = (char *) region_alloc(&fiber()->gc, len);
	char *end = NULL, *key = NULL;
	/* Encode  */
	end = mp_encode_array(begin, 2);
	/* Encode (app/prepend) */
	end = mp_encode_array(end, 5);
	end = mp_encode_str  (end, ":", 1);
	end = mp_encode_uint (end, 3);
	if (h->cmd == MEMCACHED_BIN_CMD_PREPEND ||
	    h->cmd == MEMCACHED_BIN_CMD_PREPENDQ)
		end = mp_encode_uint(end,  0);
	else
		end = mp_encode_int (end, -1);
	end = mp_encode_uint (end, 0);
	end = mp_encode_str  (end, b->val, b->val_len);
	/* Encode cas update */
	end = mp_encode_array(end, 3);
	end = mp_encode_str  (end, "=", 1);
	end = mp_encode_uint (end, 4);
	end = key = mp_encode_uint (end, cas);
	/* Encode key for update */
	end = mp_encode_array(end, 1);
	end = mp_encode_str  (end, b->key, b->key_len);

	box_tuple_t *tuple = NULL;
	if (box_index_get(mc_space_id, 0, key, end, &tuple) == -1) {
		return mc_process_internal_error(out, h);
	} else if (tuple == NULL || is_expired_tuple(tuple)) {
		return mc_process_error(out, h, MEMCACHED_BIN_RES_KEY_ENOENT,
		       NULL);
	}

	/* Tuple can't be NULL, because we already found this element */
	if (box_update(mc_space_id, 0, key, end, begin, key, 0, &tuple) == -1) {
		mc_process_internal_error(out, h);
	} else {
		if (!MEMCACHED_BIN_CMD_IS_QUITE(h->cmd)) {
			write_output(out, h, MEMCACHED_BIN_RES_OK, cas,
				     0, 0, 0, NULL, NULL, NULL);
		}
	}
	return;
}

void
memcached_layer_init() {
	flush = 0;
}
