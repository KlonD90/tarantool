/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
 *
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

#include "info.h"

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
} /* extern "C" */

#include "box/replica.h"
#include "box/recovery.h"
#include "box/cluster.h"
#include "main.h"
#include "box/box.h"
#include "lua/utils.h"
#include "fiber.h"

static int
lbox_info_replication(struct lua_State *L)
{
	struct recovery_state *r = recovery;
	struct replica *replica = &r->replica;

	lua_newtable(L);

	lua_pushstring(L, "status");
	lua_pushstring(L, replica->status);
	lua_settable(L, -3);

	if (replica->reader) {
		lua_pushstring(L, "lag");
		lua_pushnumber(L, replica->lag);
		lua_settable(L, -3);

		lua_pushstring(L, "idle");
		lua_pushnumber(L, ev_now(loop()) - replica->last_row_time);
		lua_settable(L, -3);

		Exception *e = diag_last_error(&replica->reader->diag);
		if (e != NULL) {
			lua_pushstring(L, "message");
			lua_pushstring(L, e->errmsg());
			lua_settable(L, -3);
		}
	}

	return 1;
}

static int
lbox_info_server(struct lua_State *L)
{
	lua_createtable(L, 0, 2);
	lua_pushliteral(L, "id");
	lua_pushinteger(L, recovery->server_id);
	lua_settable(L, -3);
	lua_pushliteral(L, "uuid");
	lua_pushlstring(L, tt_uuid_str(&recovery->server_uuid), UUID_STR_LEN);
	lua_settable(L, -3);
	lua_pushliteral(L, "lsn");
	luaL_pushint64(L, vclock_get(&recovery->vclock, recovery->server_id));
	lua_settable(L, -3);
	lua_pushliteral(L, "ro");
	lua_pushboolean(L, box_is_ro());
	lua_settable(L, -3);

	return 1;
}

static int
lbox_info_vclock(struct lua_State *L)
{
	lua_createtable(L, 0, vclock_size(&recovery->vclock));
	/* Request compact output flow */
	luaL_setmaphint(L, -1);
	struct vclock_iterator it;
	vclock_iterator_init(&it, &recovery->vclock);
	vclock_foreach(&it, server) {
		lua_pushinteger(L, server.id);
		luaL_pushuint64(L, server.lsn);
		lua_settable(L, -3);
	}

	return 1;
}

static int
lbox_info_status(struct lua_State *L)
{
	lua_pushstring(L, box_status());
	return 1;
}

static int
lbox_info_uptime(struct lua_State *L)
{
	lua_pushnumber(L, (unsigned)tarantool_uptime() + 1);
	return 1;
}

static int
lbox_info_pid(struct lua_State *L)
{
	lua_pushnumber(L, getpid());
	return 1;
}

static const struct luaL_reg
lbox_info_dynamic_meta [] =
{
	{"vclock", lbox_info_vclock},
	{"server", lbox_info_server},
	{"replication", lbox_info_replication},
	{"status", lbox_info_status},
	{"uptime", lbox_info_uptime},
	{"pid", lbox_info_pid},
	{NULL, NULL}
};

/** Evaluate box.info.* function value and push it on the stack. */
static int
lbox_info_index(struct lua_State *L)
{
	lua_pushvalue(L, -1);			/* dup key */
	lua_gettable(L, lua_upvalueindex(1));   /* table[key] */

	if (!lua_isfunction(L, -1)) {
		/* No such key. Leave nil is on the stack. */
		return 1;
	}

	lua_call(L, 0, 1);
	lua_remove(L, -2);
	return 1;
}

/** Push a bunch of compile-time or start-time constants into a Lua table. */
static void
lbox_info_init_static_values(struct lua_State *L)
{
	/* tarantool version */
	lua_pushstring(L, "version");
	lua_pushstring(L, tarantool_version());
	lua_settable(L, -3);
}

/**
 * When user invokes box.info(), return a table of key/value
 * pairs containing the current info.
 */
static int
lbox_info_call(struct lua_State *L)
{
	lua_newtable(L);
	lbox_info_init_static_values(L);
	for (int i = 0; lbox_info_dynamic_meta[i].name; i++) {
		lua_pushstring(L, lbox_info_dynamic_meta[i].name);
		lbox_info_dynamic_meta[i].func(L);
		lua_settable(L, -3);
	}
	return 1;
}

/** Initialize box.info package. */
void
box_lua_info_init(struct lua_State *L)
{
	static const struct luaL_reg infolib [] = {
		{NULL, NULL}
	};

	luaL_register_module(L, "box.info", infolib);

	lua_newtable(L);		/* metatable for info */

	lua_pushstring(L, "__index");

	lua_newtable(L);
	luaL_register(L, NULL, lbox_info_dynamic_meta); /* table for __index */
	lua_pushcclosure(L, lbox_info_index, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "__call");
	lua_pushcfunction(L, lbox_info_call);
	lua_settable(L, -3);

	lua_pushstring(L, "__serialize");
	lua_pushcfunction(L, lbox_info_call);
	lua_settable(L, -3);

	lua_setmetatable(L, -2);

	lbox_info_init_static_values(L);

	lua_pop(L, 1); /* info module */
}

void sophia_info(void (*)(const char*, const char*, int, void*), void*);

static void
lbox_sophia_cb(const char *key, const char *value, int /* pos */, void *arg)
{
	struct lua_State *L;
	L = (struct lua_State*)arg;
	if (value == NULL)
		return;
	lua_pushstring(L, key);
	lua_pushstring(L, value);
	lua_settable(L, -3);
}

/**
 * When user invokes box.sophia(), return a table of key/value
 * pairs containing the current info.
 */
static int
lbox_sophia_call(struct lua_State *L)
{
	lua_newtable(L);
	sophia_info(lbox_sophia_cb, (void*)L);
	return 1;
}

/** Initialize box.sophia package. */
void
box_lua_sophia_init(struct lua_State *L)
{
	static const struct luaL_reg sophialib [] = {
		{NULL, NULL}
	};

	luaL_register_module(L, "box.sophia", sophialib);

	lua_newtable(L);

	lua_pushstring(L, "__call");
	lua_pushcfunction(L, lbox_sophia_call);
	lua_settable(L, -3);

	lua_pushstring(L, "__serialize");
	lua_pushcfunction(L, lbox_sophia_call);
	lua_settable(L, -3);

	lua_setmetatable(L, -2);
	lua_pop(L, 1);
}
