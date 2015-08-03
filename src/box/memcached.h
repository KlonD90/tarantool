#ifndef TARANTOOL_MEMCACHED_H_INCLUDED
#define TARANTOOL_MEMCACHED_H_INCLUDED

#if defined(__linux__) || defined (__CYGWIN__)
#  include <endian.h>
#  define betoh16(x) be16toh(x)
#  define betoh32(x) be32toh(x)
#  define betoh64(x) be64toh(x)
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__DragonFly__)
#  include <sys/endian.h>
#  define betoh16(x) be16toh(x)
#  define betoh32(x) be32toh(x)
#  define betoh64(x) be64toh(x)
#elif defined(__OpenBSD__)
#  include <sys/types.h>
#elif defined(__APPLE__)
#  include <libkern/OSByteOrder.h>
#  define htobe16(x) OSSwapHostToBigInt16(x)
#  define betoh16(x) OSSwapBigToHostInt16(x)
#  define htobe32(x) OSSwapHostToBigInt32(x)
#  define betoh32(x) OSSwapBigToHostInt32(x)
#  define htobe64(x) OSSwapHostToBigInt64(x)
#  define betoh64(x) OSSwapBigToHostInt64(x)
#endif

/*
 ** Old text memcached API
 * int
 * memcached_parse_text(struct mc_request *req,
 * 		     const char **p,
 * 		     const char *pe);
 */

void
memcached_init();

void
memcached_set_listen(const char *uri);

/**
 * A single msg from io thread. All requests
 * from all connections are queued into a single queue
 * and processed in FIFO order.
 */
struct memcached_msg: public cmsg
{
	struct memcached_connection *connection;

	/* --- Box msgs - actual requests for the transaction processor --- */
	/* Request message code and sync. */
	struct memcached_hdr hdr;
	/* Box request, if this is a DML */
	struct memcached_body body;
	/*
	 * Remember the active iobuf of the connection,
	 * in which the request is stored. The response
	 * must be put into the out buffer of this iobuf.
	 */
	struct iobuf *iobuf;
	/**
	 * How much space the request takes in the
	 * input buffer (len, header and body - all of it)
	 * This also works as a reference counter to
	 * memcached_connection object.
	 */
	size_t len;
	/** End of write position in the output buffer */
	struct obuf_svp write_end;
	/**
	 * Used in "connect" msgs, true if connect trigger failed
	 * and the connection must be closed.
	 */
	bool close_connection;
};
#endif
