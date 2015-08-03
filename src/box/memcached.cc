/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>

#include "say.h"
#include "cbus.h"
#include "evio.h"
#include "main.h"
#include "fiber.h"
#include "iobuf.h"
#include "session.h"
// #include "user_def.h"

#include "memcached_constants.h"
#include "memcached.h"
#include "memcached_layer.h"

/* {{{ memcached_msg - declaration */

static __attribute__ ((unused)) void
memcached_dump_hdr(struct memcached_hdr *hdr) {
	if (!hdr) return;
	say_info("memcached package");
	say_info("magic:   0x%" PRIX8,  hdr->magic);
	say_info("cmd:     0x%" PRIX8,  hdr->cmd);
	say_info("key_len: %" PRIu16,   hdr->key_len);
	say_info("ext_len: %" PRIu8,    hdr->ext_len);
	say_info("tot_len: %" PRIu32,   hdr->tot_len);
	say_info("opaque:  0x%" PRIu32, hdr->opaque);
	say_info("cas:     %" PRIu64,   hdr->cas);
}

static struct mempool memcached_msg_pool;

/** Unchanged (iproto->memcached) */
static struct memcached_msg *
memcached_msg_new(struct memcached_connection *con, struct cmsg_hop *route)
{
	struct memcached_msg *msg =
		(struct memcached_msg *) mempool_alloc(&memcached_msg_pool);
	cmsg_init(msg, route);
	msg->connection = con;
	return msg;
}

/** Unchanged (iproto->memcached) */
static inline void
memcached_msg_delete(struct cmsg *msg)
{
	mempool_free(&memcached_msg_pool, msg);
}

/** Unchanged (iproto->memcached) */
struct MemcachedMsgGuard {
	struct memcached_msg *msg;
	MemcachedMsgGuard(struct memcached_msg *msg_arg):msg(msg_arg) {}
	~MemcachedMsgGuard()
	{ if (msg) memcached_msg_delete(msg); }
	struct memcached_msg *release()
	{ struct memcached_msg *tmp = msg; msg = NULL; return tmp; }
};

enum { IPROTO_FIBER_POOL_SIZE = 1024, IPROTO_FIBER_POOL_IDLE_TIMEOUT = 3 };

/* }}} */

/* {{{ iproto connection and requests */

/**
 * A single global queue for all requests in all connections. All
 * requests from all connections are processed concurrently.
 * Is also used as a queue for just established connections and to
 * execute disconnect triggers. A few notes about these triggers:
 * - they need to be run in a fiber
 * - unlike an ordinary request failure, on_connect trigger
 *   failure must lead to connection close.
 * - on_connect trigger must be processed before any other
 *   request on this connection.
 */
static struct cpipe tx_pipe;
static struct cpipe net_pipe;
static struct cbus net_tx_bus;
/* A pointer to the transaction processor cord. */
extern struct cord *tx_cord;

/** Context of a single client connection. */
struct memcached_connection
{
	/**
	 * Two rotating buffers for I/O. Input is always read into
	 * iobuf[0]. As soon as iobuf[0] input buffer becomes full,
	 * iobuf[0] is moved to iobuf[1], for flushing. As soon as
	 * all output in iobuf[1].out is sent to the client, iobuf[1]
	 * and iobuf[0] are moved around again.
	 */
	struct iobuf *iobuf[2];
	/*
	 * Size of readahead which is not parsed yet, i.e.
	 * size of a piece of request which is not fully read.
	 * Is always relative to iobuf[0]->in.wpos. In other words,
	 * iobuf[0]->in.wpos - parse_size gives the start of the
	 * unparsed request. A size rather than a pointer is used
	 * to be safe in case in->buf is reallocated. Being
	 * relative to in->wpos, rather than to in->rpos is helpful to
	 * make sure ibuf_reserve() or iobuf rotation don't make
	 * the value meaningless.
	/ */
	ssize_t parse_size;
	struct ev_io input;
	struct ev_io output;
	/** Logical session. */
	struct session *session;
	uint64_t cookie;
	ev_loop *loop;
	/* Pre-allocated disconnect msg. */
	struct memcached_msg *disconnect;
};

static struct mempool memcached_connection_pool;

/** Unchanged (iproto->memcached) */
/**
 * A connection is idle when the client is gone
 * and there are no outstanding msgs in the msg queue.
 * An idle connection can be safely garbage collected.
 * Note: a connection only becomes idle after memcached_connection_close(),
 * which closes the fd.  This is why here the check is for
 * evio_has_fd(), not ev_is_active()  (false if event is not
 * started).
 *
 * ibuf_size() provides an effective reference counter
 * on connection use in the tx request queue. Any request
 * in the request queue has a non-zero len, and ibuf_size()
 * is therefore non-zero as long as there is at least
 * one request in the tx queue.
 */
static inline bool
memcached_connection_is_idle(struct memcached_connection *con)
{
	return ibuf_used(&con->iobuf[0]->in) == 0 &&
		ibuf_used(&con->iobuf[1]->in) == 0;
}

static void
memcached_connection_on_input(ev_loop * /* loop */, struct ev_io *watcher,
			   int /* revents */);
static void
memcached_connection_on_output(ev_loop * /* loop */, struct ev_io *watcher,
			    int /* revents */);

/** Unchanged (iproto->memcached) */
/** Recycle a connection. Never throws. */
static inline void
memcached_connection_delete(struct memcached_connection *con)
{
	assert(memcached_connection_is_idle(con));
	assert(!evio_has_fd(&con->output));
	assert(!evio_has_fd(&con->input));
	assert(con->session == NULL);
	/*
	 * The output buffers must have been deleted
	 * in tx thread.
	 */
	iobuf_delete_mt(con->iobuf[0]);
	iobuf_delete_mt(con->iobuf[1]);
	if (con->disconnect)
		memcached_msg_delete(con->disconnect);
	mempool_free(&memcached_connection_pool, con);
}

static void
tx_memcached_process_msg(struct cmsg *msg);

static void
net_send_msg(struct cmsg *msg);

static void
net_process_quite_msg(struct cmsg *msg);

/**
 * Fire on_disconnect triggers in the tx
 * thread and destroy the session object,
 * as well as output buffers of the connection.
 */
static void
tx_memcached_process_disconnect(struct cmsg *m)
{
	struct memcached_msg *msg = (struct memcached_msg *) m;
	struct memcached_connection *con = msg->connection;
	if (con->session) {
		if (! rlist_empty(&session_on_disconnect))
			session_run_on_disconnect_triggers(con->session);
		session_destroy(con->session);
		con->session = NULL; /* safety */
	}
	/*
	 * Got to be done in iproto thread since
	 * that's where the memory is allocated.
	 */
	obuf_destroy(&con->iobuf[0]->out);
	obuf_destroy(&con->iobuf[1]->out);
}

/**
 * Cleanup the net thread resources of a connection
 * and close the connection.
 */
static void
net_finish_disconnect(struct cmsg *m)
{
	struct memcached_msg *msg = (struct memcached_msg *) m;
	/* Runs the trigger, which may yield. */
	memcached_connection_delete(msg->connection);
	memcached_msg_delete(msg);
}

static struct cmsg_hop disconnect_route[] = {
	{ tx_memcached_process_disconnect, &net_pipe },
	{ net_finish_disconnect, NULL },
};


static struct cmsg_hop request_route[] = {
	{ tx_memcached_process_msg, NULL }
};

static struct memcached_connection *
memcached_connection_new(const char *name, int fd, struct sockaddr *addr)
{
	(void) name;
	struct memcached_connection *con = (struct memcached_connection *)
		mempool_alloc(&memcached_connection_pool);
	con->input.data = con->output.data = con;
	con->loop = loop();
	ev_io_init(&con->input, memcached_connection_on_input, fd, EV_READ);
	ev_io_init(&con->output, memcached_connection_on_output, fd, EV_WRITE);
	con->iobuf[0] = iobuf_new_mt(&tx_cord->slabc);
	con->iobuf[1] = iobuf_new_mt(&tx_cord->slabc);
	con->parse_size = 0;
	con->session = NULL;
	con->cookie = *(uint64_t *) addr;
	/* It may be very awkward to allocate at close. */
	con->disconnect = memcached_msg_new(con, disconnect_route);
	return con;
}

/** Unchanged (iproto->memcached) */
/**
 * Initiate a connection shutdown. This method may
 * be invoked many times, and does the internal
 * bookkeeping to only cleanup resources once.
 */
static inline void
memcached_connection_close(struct memcached_connection *con)
{
	if (evio_has_fd(&con->input)) {
		/* Clears all pending events. */
		ev_io_stop(con->loop, &con->input);
		ev_io_stop(con->loop, &con->output);

		int fd = con->input.fd;
		/* Make evio_has_fd() happy */
		con->input.fd = con->output.fd = -1;
		close(fd);
		/*
		 * Discard unparsed data, to recycle the
		 * connection in net_send_msg() as soon as all
		 * parsed data is processed.  It's important this
		 * is done only once.
		 */
		con->iobuf[0]->in.wpos -= con->parse_size;
	}
	/*
	 * If the connection has no outstanding requests in the
	 * input buffer, then no one (e.g. tx thread) is referring
	 * to it, so it must be destroyed at once. Queue a msg to
	 * run on_disconnect() trigger and destroy the connection.
	 *
	 * Otherwise, it will be destroyed by the last request on
	 * this connection that has finished processing.
	 *
	 * The check is mandatory to not destroy a connection
	 * twice.
	 */
	if (memcached_connection_is_idle(con)) {
		assert(con->disconnect != NULL);
		struct memcached_msg *msg = con->disconnect;
		con->disconnect = NULL;
		cpipe_push(&tx_pipe, msg);
	}
}

/** Unchanged (iproto->memcached) */
/**
 * If there is no space for reading input, we can do one of the
 * following:
 * - try to get a new iobuf, so that it can fit the request.
 *   Always getting a new input buffer when there is no space
 *   makes the server susceptible to input-flood attacks.
 *   Therefore, at most 2 iobufs are used in a single connection,
 *   one is "open", receiving input, and the  other is closed,
 *   flushing output.
 * - stop input and wait until the client reads piled up output,
 *   so the input buffer can be reused. This complements
 *   the previous strategy. It is only safe to stop input if it
 *   is known that there is output. In this case input event
 *   flow will be resumed when all replies to previous requests
 *   are sent, in memcached_connection_gc_iobuf(). Since there are two
 *   buffers, the input is only stopped when both of them
 *   are fully used up.
 *
 * To make this strategy work, each iobuf in use must fit at
 * least one request. Otherwise, iobuf[1] may end
 * up having no data to flush, while iobuf[0] is too small to
 * fit a big incoming request.
 */
static struct iobuf *
memcached_connection_input_iobuf(struct memcached_connection *con)
{
	struct iobuf *oldbuf = con->iobuf[0];

	ssize_t to_read = 24; /* Smallest possible valid request. */

	/* The type code is checked in memcached_enqueue_batch() */
	if (con->parse_size > 24) {
		const char *pos = oldbuf->in.wpos - con->parse_size;
		struct memcached_hdr *hdr = \
				(struct memcached_hdr *)pos;
		assert(hdr->magic == MEMCACHED_BIN_REQUEST);
		to_read = betoh32(hdr->tot_len);
	}

	if (ibuf_unused(&oldbuf->in) >= to_read)
		return oldbuf;

	/** All requests are processed, reuse the buffer. */
	if (ibuf_used(&oldbuf->in) == con->parse_size) {
		ibuf_reserve(&oldbuf->in, to_read);
		return oldbuf;
	}

	if (! iobuf_is_idle(con->iobuf[1])) {
		/*
		 * Wait until the second buffer is flushed
		 * and becomes available for reuse.
		 */
		return NULL;
	}
	struct iobuf *newbuf = con->iobuf[1];

	ibuf_reserve(&newbuf->in, to_read + con->parse_size);
	/*
	 * Discard unparsed data in the old buffer, otherwise it
	 * won't be recycled when all parsed requests are processed.
	 */
	oldbuf->in.wpos -= con->parse_size;
	/* Move the cached request prefix to the new buffer. */
	memcpy(newbuf->in.rpos, oldbuf->in.wpos, con->parse_size);
	newbuf->in.wpos += con->parse_size;
	/*
	 * Rotate buffers. Not strictly necessary, but
	 * helps preserve response order.
	 */
	con->iobuf[1] = oldbuf;
	con->iobuf[0] = newbuf;
	return newbuf;
}

/** Enqueue one request which were read up. */
static inline int
memcached_enqueue_msg(struct memcached_connection *con, struct ibuf *in)
{
	say_info("in the ENQUEUE_MSG function");
	const char *reqstart = in->wpos - con->parse_size;

	if (reqstart + sizeof(struct memcached_hdr) > in->wpos) {
		say_info("can't parse headers");
		return -1;
	}
	struct memcached_hdr *hdr =
		(struct memcached_hdr *)reqstart;
	hdr->key_len = betoh16(hdr->key_len);
	hdr->tot_len = betoh32(hdr->tot_len);
	hdr->opaque  = betoh32(hdr->opaque);
	hdr->cas     = betoh64(hdr->cas);
	uint32_t psize = hdr->tot_len;
	const char *reqend = reqstart +
		sizeof(struct memcached_hdr) + psize;
	assert(hdr->magic == MEMCACHED_BIN_REQUEST);
	if (reqend > in->wpos) {
		say_info("can't parse, skip for now");
		say_info("need %u, have %lu", psize, con->parse_size);
		return -1;
	}
	struct memcached_msg *msg = memcached_msg_new(con, request_route);
	msg->iobuf = con->iobuf[0];
	msg->len   = sizeof(struct memcached_hdr) + psize;
	MemcachedMsgGuard guard(msg);
	memcpy(&msg->hdr, hdr, sizeof(struct memcached_hdr));

	memset((void *)&msg->body, 0, sizeof(struct memcached_body));
	char *body_begin = (char *)hdr + sizeof(struct memcached_hdr);
	if ((msg->body.ext_len = hdr->ext_len)) {
		msg->body.ext = body_begin;
		body_begin += msg->body.ext_len;
	}
	if ((msg->body.key_len = hdr->key_len)) {
		msg->body.key = body_begin;
		body_begin += msg->body.key_len;
	}
	if ((msg->body.val_len = psize -
			(hdr->ext_len + hdr->key_len))) {
		msg->body.val = body_begin;
		body_begin += msg->body.val_len;
	}

	cpipe_push(&tx_pipe, guard.release());

	/* Request is parsed */
	con->parse_size -= (body_begin - reqstart);
	return 0;
}

/** Unchanged (iproto->memcached) */
static void
memcached_connection_on_input(ev_loop *loop, struct ev_io *watcher,
			   int /* revents */)
{
	say_info("in the ON_INPUT function");
	struct memcached_connection *con =
		(struct memcached_connection *) watcher->data;
	int fd = con->input.fd;
	assert(fd >= 0);

	try {
		/* Ensure we have sufficient space for the next round.  */
		struct iobuf *iobuf = memcached_connection_input_iobuf(con);
		if (iobuf == NULL) {
			ev_io_stop(loop, &con->input);
			return;
		}

		struct ibuf *in = &iobuf->in;
		/* Read input. */
		int nrd = sio_read(fd, in->wpos, ibuf_unused(in));
		say_info("nrd == %d, parse_size == %ld", nrd, con->parse_size);
		if (nrd < 0) {                  /* Socket is not ready. */
			say_info("socket is not ready");
			ev_io_start(loop, &con->input);
		}
		if (nrd == 0) {                 /* EOF */
			say_info("EOF");
			memcached_connection_close(con);
		}
		if (con->parse_size == 0 && nrd <= 0) {
			say_info("con->parse_size == 0");
			return;
		}
		/* Update the read position and connection state. */
		if (nrd > 0) {
			say_info("nrd > 0");
			in->wpos += nrd;
			con->parse_size += nrd;
		}
		/* Enqueue all requests which are fully read up. */
		int retval = memcached_enqueue_msg(con, in);
		/* We need more data, so continue reading */
		if (retval == -1 && !ev_is_active(&con->input) && nrd != 0) {
			say_info("We need more data");
			ev_feed_event(con->loop, &con->input, EV_READ);
		}
	} catch (Exception *e) {
		e->log();
		memcached_connection_close(con);
	}
}

/** Unchanged (iproto->memcached) */
/** Get the iobuf which is currently being flushed. */
static inline struct iobuf *
memcached_connection_output_iobuf(struct memcached_connection *con)
{
	if (obuf_used(&con->iobuf[1]->out) > 0)
		return con->iobuf[1];
	/*
	 * Don't try to write from a newer buffer if an older one
	 * exists: in case of a partial write of a newer buffer,
	 * the client may end up getting a salad of different
	 * pieces of replies from both buffers.
	 */
	if (ibuf_used(&con->iobuf[1]->in) == 0 &&
	    obuf_used(&con->iobuf[0]->out) > 0)
		return con->iobuf[0];
	return NULL;
}

/** Unchanged (iproto->memcached) */
/** writev() to the socket and handle the result. */
static int
memcached_flush(struct iobuf *iobuf, struct memcached_connection *con)
{
	int fd = con->output.fd;
	struct obuf_svp *begin = &iobuf->out.wpos;
	struct obuf_svp *end = &iobuf->out.wend;
	assert(begin->used < end->used);
	struct iovec iov[SMALL_OBUF_IOV_MAX+1];
	struct iovec *src = iobuf->out.iov;
	int iovcnt = end->pos - begin->pos + 1;
	/*
	 * iov[i].iov_len may be concurrently modified in tx thread,
	 * but only for the last position.
	 */
	memcpy(iov, src + begin->pos, iovcnt * sizeof(struct iovec));
	sio_add_to_iov(iov, -begin->iov_len);
	/* *Overwrite* iov_len of the last pos as it may be garbage. */
	iov[iovcnt-1].iov_len = end->iov_len - begin->iov_len * (iovcnt == 1);

	ssize_t nwr = sio_writev(fd, iov, iovcnt);
	if (nwr > 0) {
		if (begin->used + nwr == end->used) {
			if (ibuf_used(&iobuf->in) == 0) {
				/* Quickly recycle the buffer if it's idle. */
				assert(end->used == obuf_size(&iobuf->out));
				/* resets wpos and wpend to zero pos */
				iobuf_reset(iobuf);
			} else { /* Avoid assignment reordering. */
				/* Advance write position. */
				*begin = *end;
			}
			return 0;
		}
		begin->used += nwr;             /* advance write position */
		begin->pos += sio_move_iov(iov, nwr, &begin->iov_len);
	}
	return -1;
}

/** Unchanged (iproto->memcached) */
static void
memcached_connection_on_output(ev_loop *loop, struct ev_io *watcher,
			    int /* revents */)
{
	struct memcached_connection *con = (struct memcached_connection *) watcher->data;

	try {
		struct iobuf *iobuf;
		while ((iobuf = memcached_connection_output_iobuf(con))) {
			if (memcached_flush(iobuf, con) < 0) {
				ev_io_start(loop, &con->output);
				return;
			}
			if (! ev_is_active(&con->input))
				ev_feed_event(loop, &con->input, EV_READ);
		}
		if (ev_is_active(&con->output))
			ev_io_stop(con->loop, &con->output);
	} catch (Exception *e) {
		e->log();
		memcached_connection_close(con);
	}
}

static void
memcached_reply_error(struct obuf * /* out */, Exception * /* e */, uint64_t) {
	return;
};

static void
tx_memcached_process_msg(struct cmsg *m)
{
	say_info("PROCESSING MESSAGE");
	struct memcached_msg *msg = (struct memcached_msg *) m;
	struct obuf *out = &msg->iobuf->out;
	// struct memcached_connection *con = msg->connection;
	struct session *session = msg->connection->session;
	fiber_set_session(fiber(), session);
	fiber_set_user(fiber(), &session->credentials);

	session->sync = msg->hdr.opaque;

	try {
		/* Process message */
		// memcached_dump_hdr(&msg->hdr);
		switch (msg->hdr.cmd) {
		case (MEMCACHED_BIN_CMD_ADD):
		case (MEMCACHED_BIN_CMD_ADDQ):
		case (MEMCACHED_BIN_CMD_SET):
		case (MEMCACHED_BIN_CMD_SETQ):
		case (MEMCACHED_BIN_CMD_REPLACE):
		case (MEMCACHED_BIN_CMD_REPLACEQ):
			mc_process_set(msg);
			break;
		case (MEMCACHED_BIN_CMD_GET):
		case (MEMCACHED_BIN_CMD_GETQ):
		case (MEMCACHED_BIN_CMD_GETK):
		case (MEMCACHED_BIN_CMD_GETKQ):
			mc_process_get(msg);
			break;
		case (MEMCACHED_BIN_CMD_DELETE):
		case (MEMCACHED_BIN_CMD_DELETEQ):
			mc_process_del(msg);
			break;
		case (MEMCACHED_BIN_CMD_NOOP):
			mc_process_nop(msg);
			break;
		case (MEMCACHED_BIN_CMD_QUIT):
		case (MEMCACHED_BIN_CMD_QUITQ):
			mc_process_nop(msg);
			memcached_connection_close(msg->connection);
			break;
		case (MEMCACHED_BIN_CMD_FLUSH):
		case (MEMCACHED_BIN_CMD_FLUSHQ):
			mc_process_flush(msg);
			break;
		case (MEMCACHED_BIN_CMD_GAT):
		case (MEMCACHED_BIN_CMD_GATQ):
		case (MEMCACHED_BIN_CMD_GATK):
		case (MEMCACHED_BIN_CMD_GATKQ):
		case (MEMCACHED_BIN_CMD_TOUCH):
			mc_process_touch_or_gat(msg);
			break;
		case (MEMCACHED_BIN_CMD_VERSION):
			mc_process_version(msg);
			break;
		case (MEMCACHED_BIN_CMD_INCR):
		case (MEMCACHED_BIN_CMD_DECR):
		case (MEMCACHED_BIN_CMD_INCRQ):
		case (MEMCACHED_BIN_CMD_DECRQ):
			mc_process_delta(msg);
			break;
		case (MEMCACHED_BIN_CMD_APPEND):
		case (MEMCACHED_BIN_CMD_PREPEND):
		case (MEMCACHED_BIN_CMD_APPENDQ):
		case (MEMCACHED_BIN_CMD_PREPENDQ):
			mc_process_pend(msg);
			break;
		default:
			assert(false);
		}
	} catch (Exception *e) {
		memcached_reply_error(out, e, msg->hdr.opaque);
	}
	msg->write_end = obuf_create_svp(out);
	if (!MEMCACHED_BIN_CMD_IS_QUITE(msg->hdr.cmd)) {
		say_info("command is not quite, send output");
		static cmsg_hop route[] = { { net_send_msg, NULL }, };
		cmsg_init(m, route);
		cpipe_push(&net_pipe, m);
	} else {
		say_info("command is quite, extedn");
		static cmsg_hop route[] = { { net_process_quite_msg, NULL }, };
		cmsg_init(m, route);
		cpipe_push(&net_pipe, m);
	}
}

static void
net_process_quite_msg(struct cmsg *m)
{
	struct memcached_msg *msg = (struct memcached_msg *) m;
	struct memcached_connection *con = msg->connection;
	struct iobuf *iobuf = msg->iobuf;
	iobuf->in.rpos += msg->len;
	iobuf->out.wend = msg->write_end;
	if (!ev_is_active(&con->input) && ibuf_used(&iobuf->in) > 0) {
		say_info("quite, and we have input, signal for it");
		ev_feed_event(con->loop, &con->input, EV_READ);
	}
}

/** Unchanged (iproto->memcached) */
static void
net_send_msg(struct cmsg *m)
{
	struct memcached_msg *msg = (struct memcached_msg *) m;
	struct memcached_connection *con = msg->connection;
	struct iobuf *iobuf = msg->iobuf;
	/* Discard request (see memcached_enqueue_batch()) */
	iobuf->in.rpos += msg->len;
	iobuf->out.wend = msg->write_end;

	if (evio_has_fd(&con->output)) {
		if (! ev_is_active(&con->output))
			ev_feed_event(con->loop, &con->output, EV_WRITE);
	} else if (memcached_connection_is_idle(con)) {
		memcached_connection_close(con);
	}
	if (!ev_is_active(&con->input) && ibuf_used(&iobuf->in) > 0) {
		say_info("not quite, and we have input, signal for it");
		ev_feed_event(con->loop, &con->input, EV_READ);
	}
	memcached_msg_delete(msg);
}

/**
 * Handshake a connection: invoke the on-connect trigger
 * and possibly authenticate. Try to send the client an error
 * upon a failure.
 */
static void
tx_memcached_process_connect(struct cmsg *m)
{
	struct memcached_msg *msg = (struct memcached_msg *) m;
	struct memcached_connection *con = msg->connection;
	struct obuf *out = &msg->iobuf->out;
	try {              /* connect. */
		con->session = session_create(con->input.fd, con->cookie);
		if (! rlist_empty(&session_on_connect))
			session_run_on_connect_triggers(con->session);
	} catch (Exception *e) {
		memcached_reply_error(out, e, 0/* zero sync for connect error */);
		msg->close_connection = true;
	}
}

/** }}} */

/**
 * Create a connection and start input.
 */
static void
memcached_on_accept(struct evio_service * /* service */, int fd,
		 struct sockaddr *addr, socklen_t addrlen)
{
	char name[SERVICE_NAME_MAXLEN];
	snprintf(name, sizeof(name), "%s/%s", "iobuf",
		sio_strfaddr(addr, addrlen));

	struct memcached_connection *con;

	con = memcached_connection_new(name, fd, addr);
	/*
	 * Ignore msg allocation failure - the queue size is
	 * fixed so there is a limited number of msgs in
	 * use, all stored in just a few blocks of the memory pool.
	 */
	static cmsg_hop route[] = { { tx_memcached_process_connect, NULL }, };
	struct memcached_msg *msg = memcached_msg_new(con, route);
	msg->iobuf = con->iobuf[0];
	msg->close_connection = false;
	cpipe_push(&tx_pipe, msg);
	/* Activate for reading */
	assert(evio_has_fd(&con->output));
	if (! ev_is_active(&con->input))
		ev_feed_event(con->loop, &con->input, EV_READ);
}

static struct evio_service memcached; /* memcached listener */

/**
 * The network io thread main function:
 * begin serving the message bus.
 */
static void
net_cord_f(va_list /* ap */)
{
	/* Got to be called in every thread using iobuf */
	iobuf_init();
	mempool_create(&memcached_msg_pool, &cord()->slabc,
		       sizeof(struct memcached_msg));
	cpipe_create(&net_pipe);
	mempool_create(&memcached_connection_pool, &cord()->slabc,
		       sizeof(struct memcached_connection));

	evio_service_init(loop(), &memcached, "memcached",
			  memcached_on_accept, NULL);

	cbus_join(&net_tx_bus, &net_pipe);
	/*
	 * Nothing to do in the fiber so far, the service
	 * will take care of creating events for incoming
	 * connections.
	 */
	fiber_yield();
}

/** Initialize the iproto subsystem and start network io thread */
void
memcached_init()
{
	/* tx_cord = cord(); */

	cbus_create(&net_tx_bus);
	cpipe_create(&tx_pipe);
	static struct cpipe_fiber_pool fiber_pool;

	cpipe_fiber_pool_create(&fiber_pool, "memcached", &tx_pipe,
				IPROTO_FIBER_POOL_SIZE,
				IPROTO_FIBER_POOL_IDLE_TIMEOUT);

	static struct cord net_cord;
	if (cord_costart(&net_cord, "memcached", net_cord_f, NULL))
		panic("failed to initialize memcached thread");

	cbus_join(&net_tx_bus, &tx_pipe);
}

/**
 * Since there is no way to "synchronously" change the
 * state of the io thread, to change the listen port
 * we need to bounce a couple of messages to and
 * from this thread.
 */
struct memcached_set_listen_msg: public cmsg
{
	/**
	 * If there was an error setting the listen port,
	 * this will contain the error when the message
	 * returns to the caller.
	 */
	struct diag diag;
	/**
	 * The uri to set.
	 */
	const char *uri;
	/**
	 * The way to tell the caller about the end of
	 * bind.
	 */
	struct cmsg_notify wakeup;
};

/**
 * The bind has finished, notify the caller.
 */
static void
memcached_on_bind(void *arg)
{
	cpipe_push(&tx_pipe, (struct cmsg_notify *) arg);
}

static void
memcached_do_set_listen(struct cmsg *m)
{
	struct memcached_set_listen_msg *msg =
		(struct memcached_set_listen_msg *) m;
	try {
		if (evio_service_is_active(&memcached))
			evio_service_stop(&memcached);

		if (msg->uri != NULL) {
			memcached.on_bind = memcached_on_bind;
			memcached.on_bind_param = &msg->wakeup;
			evio_service_start(&memcached, msg->uri);
		} else {
			memcached_on_bind(&msg->wakeup);
		}
	} catch (Exception *e) {
		diag_move(&fiber()->diag, &msg->diag);
	}
}

static void
memcached_set_listen_msg_init(struct memcached_set_listen_msg *msg,
			    const char *uri)
{
	static cmsg_hop route[] = { { memcached_do_set_listen, NULL }, };
	cmsg_init(msg, route);
	msg->uri = uri;
	diag_create(&msg->diag);

	cmsg_notify_init(&msg->wakeup);
}

void
memcached_set_listen(const char *uri)
{
	/**
	 * This is a tricky orchestration for something
	 * that should be pretty easy at the first glance:
	 * change the listen uri in the io thread.
	 *
	 * To do it, create a message which sets the new
	 * uri, and another one, which will alert tx
	 * thread when bind() on the new port is done.
	 */
	static struct memcached_set_listen_msg msg;
	memcached_set_listen_msg_init(&msg, uri);

	cpipe_push(&net_pipe, &msg);
	/** Wait for the end of bind. */
	fiber_yield();
	if (! diag_is_empty(&msg.diag)) {
		diag_move(&msg.diag, &fiber()->diag);
		diag_last_error(&fiber()->diag)->raise();
	}
}

/* vim: set foldmethod=marker */
