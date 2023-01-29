/*
 * Copyright (C) 2023-2024 Slava Monich <slava@monich.com>
 *
 * You may use this file under the terms of the BSD license as follows:
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer
 *     in the documentation and/or other materials provided with the
 *     distribution.
 *
 *  3. Neither the names of the copyright holders nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation
 * are those of the authors and should not be interpreted as representing
 * any official policies, either expressed or implied.
 */

#include "giorpc_peer_p.h"

#include "giorpc_error.h"
#include "giorpc_log_p.h"
#include "giorpc_request_p.h"
#include "giorpc_util_p.h"

#include <gutil_datapack.h>
#include <gutil_macros.h>
#include <gutil_misc.h>
#include <gutil_weakref.h>

#include <glib-object.h>

#define GIORPC_MIN_PACKET_SIZE  2
#define GIORPC_MAX_ID   0xfffffff  /* 28 bits */
#define GIORPC_INTERNAL_ID_MASK  0x3fff

typedef enum giorpc_packet_type {
    GIORPC_PACKET_START = 0x01,
    GIORPC_PACKET_FINISH = 0x02,
    GIORPC_PACKET_NOTIFY = 0x03,
    GIORPC_PACKET_REQUEST = 0x04,
    GIORPC_PACKET_RESULT = 0x05,
    GIORPC_PACKET_ERROR = 0x06,
    GIORPC_PACKET_CANCEL = 0x07
} GIORPC_PACKET_TYPE;

typedef enum giorpc_start_tag {
    GIORPC_START_TAG_VERSION = 1
} GIORPC_START_TAG;

typedef struct giorpc_peer_packet GIoRpcPeerPacket;
typedef struct giorpc_peer_sync_call_data GIoRpcPeerSyncCall;
typedef struct giorpc_peer_write GIoRpcPeerWrite;
typedef GObjectClass GIoRpcPeerObjectClass;

typedef struct giorpc_peer_object {
    GObject object;
    GIoRpcPeer peer;
    GIOStream* stream;
    GCancellable* cancel;
    GMutex requests_mutex;
    GHashTable* requests; /* ID => GIoRpcPeerCall */
    GByteArray* buf;
    GUtilWeakRef* ref;
    GIoRpcPeerSyncCall* sync;
    GQueue blocked;
    GIoRpcPeerWrite* write;
    GQueue writeq;
    guint tag;
} GIoRpcPeerObject;

#define PARENT_CLASS giorpc_peer_object_parent_class
#define THIS_TYPE giorpc_peer_object_get_type()
#define THIS(obj) G_TYPE_CHECK_INSTANCE_CAST(obj, THIS_TYPE, GIoRpcPeerObject)

GType giorpc_peer_object_get_type(void) G_GNUC_INTERNAL;
G_DEFINE_TYPE(GIoRpcPeerObject, giorpc_peer_object, G_TYPE_OBJECT)

typedef enum giorpc_peer_signal {
    SIGNAL_STATE_CHANGED,
    SIGNAL_REQUEST,
    SIGNAL_NOTIFY,
    SIGNAL_CANCEL,
    SIGNAL_INTERNAL,
    SIGNAL_COUNT
} GIORPC_PEER_SIGNAL;

#define SIGNAL_STATE_CHANGED_NAME   "giorpc-peer-state-changed"
#define SIGNAL_REQUEST_NAME         "giorpc-peer-request"
#define SIGNAL_NOTIFY_NAME          "giorpc-peer-notify"
#define SIGNAL_CANCEL_NAME          "giorpc-peer-cancel"
#define SIGNAL_INTERNAL_NAME        "giorpc-peer-internal"

#define SIGNAL_CODE_DETAIL_FORMAT   "%u"
#define SIGNAL_CODE_DETAIL_MAX_LEN  (10) /* Actually 7 since it's 21-bit */

static guint giorpc_peer_signals[SIGNAL_COUNT] = { 0 };

#define RLOG(self,str) GDEBUG("[%08x] %s", (self)->tag, str)
#define RLOG1(self,fmt,p1) GDEBUG("[%08x] " fmt, (self)->tag, p1)
#define RLOG2(self,fmt,p1,p2) GDEBUG("[%08x] " fmt, (self)->tag, p1, p2)
#define RLOG3(self,fmt,p1,p2,p3) GDEBUG("[%08x] " fmt, (self)->tag, p1, p2, p3)
#define RLOG4(self,fmt,p1,p2,p3,p4) \
    GDEBUG("[%08x] " fmt, (self)->tag, p1, p2, p3, p4)
#define RWARN2(self,fmt,p1,p2) GWARN("[%08x] " fmt, (self)->tag, p1, p2)

typedef struct giorpc_peer_call_data GIoRpcPeerCall;
typedef struct giorpc_peer_request_data GIoRpcPeerRequestData;

typedef
void
(*GIoRpcPeerObjectFunc)(
    GIoRpcPeerObject* obj,
    gpointer user_data);

typedef
void
(*GIoRpcPeerObjectNoArgFunc)(
    GIoRpcPeerObject* obj);

typedef
void
(*GIoRpcPeerCallFunc)(
    GIoRpcPeerCall* call);

typedef
void
(*GIoRpcPeerRequestDataFunc)(
    GIoRpcPeerRequestData* req);

typedef
void
(*GIoRpcPeerSubmitCallFunc)(
    GIoRpcPeerObject* obj,
    GIoRpcPeerCall* call);

typedef
void
(*GIoRpcPeerPacketDoneFunc)(
    GIoRpcPeerObject* obj,
    GIORPC_PEER_ERROR error);

typedef
void
(*GIoRpcPeerPacketFunc)(
    GIoRpcPeerObject* obj,
    GIoRpcPeerPacket* packet);

struct giorpc_peer_sync_call_data {
    GIoRpcPeerSyncCall* prev;
    GIoRpcPeerObject* owner;
    GMainContext* context;
    GMainLoop* loop;
    GSource* timeout_source;
    GSource* cancel_source;
    GBytes* response;
    GError** error;
    guint call_id;
    GIORPC_SYNC_FLAGS flags;
};

typedef struct giorpc_peer_packet_type {
    const char* name;
    GIORPC_SYNC_FLAGS flag;
    GIoRpcPeerPacketFunc handle;
    GIoRpcPeerPacketFunc free;
} GIoRpcPeerPacketType;

struct giorpc_peer_packet {
    const GIoRpcPeerPacketType* type;
};

typedef struct giorpc_peer_packet_notify {
    GIoRpcPeerPacket packet;
    guint iid;
    guint code;
    GBytes* data;
} GIoRpcPeerPacketNotify;

typedef struct giorpc_peer_packet_request {
    GIoRpcPeerPacket packet;
    guint iid;
    guint code;
    GBytes* data;
    GIoRpcRequest* req;
} GIoRpcPeerPacketRequest;

typedef struct giorpc_peer_packet_result {
    GIoRpcPeerPacket packet;
    guint rid;
    GBytes* data;
} GIoRpcPeerPacketResult;

typedef struct giorpc_peer_packet_error {
    GIoRpcPeerPacket packet;
    guint rid;
    GIORPC_PEER_ERROR err;
} GIoRpcPeerPacketError;

typedef struct giorpc_peer_packet_cancel {
    GIoRpcPeerPacket packet;
    guint rid;
} GIoRpcPeerPacketCancel;

typedef struct giorpc_peer_notify_data {
    guint iid;
    guint code;
    GBytes* data;
} GIoRpcPeerNotifyData;

typedef struct giorpc_peer_result_data {
    guint id;
    GBytes* data;
} GIoRpcPeerResultData;

typedef struct giorpc_peer_error_data {
    guint id;
    GIORPC_PEER_ERROR code;
} GIoRpcPeerErrorData;

typedef struct giorpc_peer_write_data {
    GBytes* bytes;
    GIoRpcPeerPacketDoneFunc done;
    void* user_data;
} GIoRpcPeerWriteData;

struct giorpc_peer_call_data {
    gint refcount;
    guint id;
    guint iid;
    guint code;
    GUtilWeakRef* ref;
    GIoRpcPeerResponseFunc response;
    GDestroyNotify destroy;
    void* user_data;
};

struct giorpc_peer_request_data {
    GIoRpcPeerCall* call;
    GBytes* data;
};

typedef struct giorpc_peer_invoke_later_data {
    GUtilWeakRef* ref;
    GIoRpcPeerObjectFunc fn;
    GDestroyNotify destroy;
    void* user_data;
} GIoRpcPeerInvokeLater;

typedef struct giorpc_peer_closure {
    GCClosure cclosure;
    GCallback handler;
    void* user_data;
} GIoRpcPeerClosure;

typedef struct giorpc_peer_closure2 {
    GCClosure cclosure;
    GCallback handler;
    void* user_data;
    guint code; /* GIORPC_CODE_ANY for any */
} GIoRpcPeerClosure2;

#define giorpc_peer_call_drop(self,id) \
    giorpc_peer_call_unref(giorpc_peer_call_steal(self, id));

static
void
giorpc_peer_write_bytes(
    GIoRpcPeerObject* self,
    GBytes* bytes,
    GIoRpcPeerWrite* write);

static
void
giorpc_peer_write_done(
    GIoRpcPeerWrite* write,
    const GError* error);

#if GUTIL_LOG_DEBUG
static
const char*
giorpc_peer_state_name(
    GIORPC_PEER_STATE state)
{
    switch (state) {
    case GIORPC_PEER_STATE_INIT:     return "INIT";
    case GIORPC_PEER_STATE_STARTING: return "STARTING";
    case GIORPC_PEER_STATE_STARTED:  return "STARTED";
    case GIORPC_PEER_STATE_STOPPING: return "STOPPING";
    case GIORPC_PEER_STATE_DONE:     return "DONE";
    }
    return NULL;
}
#endif /* GUTIL_LOG_DEBUG */

static inline gboolean giorpc_peer_valid_iid(guint64 code)
    { return code >= GIORPC_MIN_IID && code <= GIORPC_MAX_IID; }

static inline gboolean giorpc_peer_valid_code(guint64 code)
    { return code >= GIORPC_MIN_CODE && code <= GIORPC_MAX_CODE; }

static inline gboolean giorpc_peer_valid_id(guint64 id)
    { return id <= GIORPC_MAX_ID; }

static inline GIoRpcPeerObject* giorpc_peer_object_cast(GIoRpcPeer* peer)
    { return peer ? THIS(G_CAST(peer, GIoRpcPeerObject, peer)) : NULL; }

/*==========================================================================*
 * Invocation on the context thread
 *==========================================================================*/

static
void
giorpc_peer_invoke_later_destroy(
    gpointer user_data)
{
    GIoRpcPeerInvokeLater* data = user_data;

    if (data->destroy) {
        data->destroy(data->user_data);
    }
    gutil_weakref_unref(data->ref);
    gutil_slice_free(data);
}

static
gboolean
giorpc_peer_invoke_later_proc(
    gpointer user_data)
{
    GIoRpcPeerInvokeLater* data = user_data;
    GIoRpcPeerObject* self = gutil_weakref_get(data->ref);

    if (self) {
        data->fn(self, data->user_data);
        g_object_unref(self);
    }
    return G_SOURCE_REMOVE;
}

static
void
giorpc_peer_invoke_no_arg(
    GIoRpcPeerObject* obj,
    gpointer user_data)
{
    GIoRpcPeerObjectNoArgFunc fn = user_data;

    fn(obj);
}

static
void
giorpc_peer_invoke_later_full(
    GIoRpcPeerObject* self,
    GIoRpcPeerObjectFunc fn,
    GDestroyNotify destroy,
    gpointer user_data)
{
    GSource* src = g_idle_source_new();
    GIoRpcPeerInvokeLater* data = g_slice_new(GIoRpcPeerInvokeLater);

    data->fn = fn;
    data->ref = gutil_weakref_ref(self->ref);
    data->destroy = destroy;
    data->user_data = user_data;
    g_source_set_priority(src, G_PRIORITY_DEFAULT);
    g_source_set_callback(src, giorpc_peer_invoke_later_proc, data,
        giorpc_peer_invoke_later_destroy);
    g_source_attach(src, self->peer.context);
    g_source_unref(src);
}

static
void
giorpc_peer_invoke_later(
    GIoRpcPeerObject* self,
    GIoRpcPeerObjectNoArgFunc fn)
{
    giorpc_peer_invoke_later_full(self, giorpc_peer_invoke_no_arg, NULL, fn);
}

static
void
giorpc_peer_invoke_now_or_later(
    GIoRpcPeerObject* self,
    GIoRpcPeerObjectNoArgFunc fn)
{
    if (G_LIKELY(self)) {
        GIoRpcPeer* peer = &self->peer;

        if (g_main_context_acquire(peer->context)) {
            fn(self);
            g_main_context_release(peer->context);
        } else {
            giorpc_peer_invoke_later(self, fn);
        }
    }
}

/*==========================================================================*
 * Miscellaneous
 *==========================================================================*/

static
GError*
giorpc_peer_error_not_owner()
{
    return g_error_new_literal(GIORPC_ERROR, GIORPC_ERROR_NOT_OWNER,
        "Failed to aquire context");
}

static
GError*
giorpc_peer_error_abandoned()
{
    return g_error_new_literal(GIORPC_ERROR, GIORPC_ERROR_ABANDONED,
        "Request abandoned");
}

static
GError*
giorpc_peer_error_invalid_args()
{
    return g_error_new_literal(GIORPC_ERROR, GIORPC_ERROR_INVALID_ARGS,
        "Invalid arguments");
}

static
GError*
giorpc_peer_error_cancelled()
{
    return g_error_new_literal(GIORPC_ERROR, GIORPC_ERROR_CANCELLED,
        "Call cancelled");
}

static
GError*
giorpc_peer_error_timeout()
{
    return g_error_new_literal(GIORPC_ERROR, GIORPC_ERROR_TIMEOUT,
        "Call timed out");
}

static
GError*
giorpc_peer_error_new(
    guint code,
    GIORPC_PEER_ERROR error)
{
    switch (error) {
    case GIORPC_PEER_ERROR_NONE:
    case GIORPC_PEER_ERROR_UNSPECIFIED:
    case GIORPC_PEER_ERROR_PROTOCOL:
        /* Use default */
        break;
    case GIORPC_PEER_ERROR_UNHANDLED:
        return g_error_new(GIORPC_ERROR, GIORPC_ERROR_UNHANDLED,
            "Unhandled request code %u", code);
    case GIORPC_PEER_ERROR_CANCELLED:
        return giorpc_peer_error_cancelled();
    }

    /* Default */
    return g_error_new_literal(GIORPC_ERROR, GIORPC_ERROR_UNSPECIFIED,
        "Call failed");
}

static
void
giorpc_peer_set_error(
    GError** dest,
    GError* (*make_error)())
{
    if (dest && !*dest) {
        *dest = (*make_error)();
    }
}

static
GQuark
giorpc_peer_iid_quark(
    guint iid)
{
    if (iid) {
        char detail[SIGNAL_CODE_DETAIL_MAX_LEN + 1];

        snprintf(detail, sizeof(detail), SIGNAL_CODE_DETAIL_FORMAT, iid);
        detail[SIGNAL_CODE_DETAIL_MAX_LEN] = 0;
        return g_quark_from_string(detail);
    } else {
        return 0;
    }
}

static
GBytes*
giorpc_peer_steal_bytes(
    GIoRpcPeerObject* self,
    const GUtilData* data)
{
    if (data->size) {
        GByteArray* buf = self->buf;
        gsize offset = data->bytes - buf->data;
        GBytes* all = g_byte_array_free_to_bytes(buf);
        GBytes* part = g_bytes_new_from_bytes(all, offset, data->size);

        g_bytes_unref(all);
        self->buf = g_byte_array_new();
        return part;
    } else {
        return g_bytes_new(NULL, 0);
    }
}

static
void
giorpc_peer_call_free(
    GIoRpcPeerCall* call)
{
    if (call->destroy) {
        call->destroy(call->user_data);
    }
    gutil_weakref_unref(call->ref);
    gutil_slice_free(call);
}

static
void
giorpc_peer_call_unref(
    GIoRpcPeerCall* call)
{
    if (call && g_atomic_int_dec_and_test(&call->refcount)) {
        giorpc_peer_call_free(call);
    }
}

static
void
giorpc_peer_call_destroy(
    gpointer call)
{
    giorpc_peer_call_unref(call);
}

static
gboolean
giorpc_peer_call_id_valid(
    GIoRpcPeerObject* self,
    guint id)
{
    gboolean valid = FALSE;

    if (id && self->requests) {
        /* Lock */
        g_mutex_lock(&self->requests_mutex);
        valid = g_hash_table_contains(self->requests, GUINT_TO_POINTER(id));
        g_mutex_unlock(&self->requests_mutex);
        /* Unlock */
    }
    return valid;
}

static
GIoRpcPeerCall*
giorpc_peer_call_steal(
    GIoRpcPeerObject* self,
    guint id)
{
    GIoRpcPeerCall* call = NULL;

    if (id) {
        /* Lock */
        g_mutex_lock(&self->requests_mutex);
        if (self->requests) {
            const gconstpointer key = GUINT_TO_POINTER(id);

            call = g_hash_table_lookup(self->requests, key);
            if (call) {
                /* Caller will unref */
                g_hash_table_steal(self->requests, key);
            }
        }
        g_mutex_unlock(&self->requests_mutex);
        /* Unlock */
    }
    return call;
}

static
GSList*
giorpc_peer_call_steal_all(
    GIoRpcPeerObject* self)
{
    GSList* calls = NULL;

    /* Lock */
    g_mutex_lock(&self->requests_mutex);
    if (self->requests) {
        GHashTableIter it;
        gpointer value;

        g_hash_table_iter_init(&it, self->requests);
        while (g_hash_table_iter_next(&it, NULL, &value)) {
            calls = g_slist_append(calls, value);
            g_hash_table_iter_steal(&it);
        }
    }
    g_mutex_unlock(&self->requests_mutex);
    /* Unlock */
    return calls;
}

static
void
giorpc_peer_call_abandon_all(
    GIoRpcPeerObject* self)
{
    GSList* calls = giorpc_peer_call_steal_all(self);

    if (calls) {
        GSList* l;
        GError* error = giorpc_peer_error_abandoned();

        for (l = calls; l; l = l->next) {
            GIoRpcPeerCall* call = l->data;

            if (call->response) {
                call->response(&self->peer, NULL, error, call->user_data);
            }
            giorpc_peer_call_unref(call);
        }
        g_error_free(error);
        g_slist_free(calls);
    }
}

static
GIoRpcPeerCall*
giorpc_peer_call_new(
    GIoRpcPeerObject* self,
    guint iid,
    guint code,
    GIoRpcPeerResponseFunc response,
    GDestroyNotify destroy,
    void* user_data)
{
    GIoRpcPeerCall* call = NULL;

    if (code >= GIORPC_MIN_CODE && code <= GIORPC_MAX_CODE) {
        guint i;

        /*
         * The initial refcount is 2 because one reference is returned to
         * the caller and is owned by the hashtable.
         */
        call = g_slice_new(GIoRpcPeerCall);
        g_atomic_int_set(&call->refcount, 2);
        call->id = 0;
        call->iid = iid;
        call->code = code;
        call->ref = gutil_weakref_ref(self->ref);
        call->response = response;

        /* Lock */
        g_mutex_lock(&self->requests_mutex);
        if (!self->requests) {
            /* Create request table on demand */
            self->requests = g_hash_table_new_full(g_direct_hash,
                g_direct_equal, NULL, giorpc_peer_call_destroy);
        }
        for (i = 1; i < GIORPC_INTERNAL_ID_MASK; i++) {
            /*
             * It makes debugging easier when ids are unique across all
             * peers within the process.
             */
            static gint last_id = 0;
            guint id = g_atomic_int_add(&last_id, 1) & GIORPC_INTERNAL_ID_MASK;

            /* Skip rare (very rare) zeros */
            if (id) {
                const gpointer key = GUINT_TO_POINTER(id);

                if (!g_hash_table_contains(self->requests, key)) {
                    g_hash_table_insert(self->requests, key, call);
                    call->destroy = destroy;
                    call->user_data = user_data;
                    call->id = id;
                    break;
                }
            }
        }
        g_mutex_unlock(&self->requests_mutex);
        /* Unlock */

        if (!call->id) {
            /* That shouldn't happen in real life, ever */
            gutil_slice_free(call);
            call = NULL;
        }
    }
    return call;
}

static
GIoRpcPeerRequestData*
giorpc_peer_request_data_new(
    GIoRpcPeerCall* call,
    GBytes* data)
{
    GIoRpcPeerRequestData* req = g_slice_new(GIoRpcPeerRequestData);

    req->call = call;  /* Take ownership */
    req->data = data ? g_bytes_ref(data) : NULL;
    return req;
}

static
void
giorpc_peer_request_data_free(
    GIoRpcPeerRequestData* req)
{
    if (req->data) {
        g_bytes_unref(req->data);
    }
    giorpc_peer_call_unref(req->call);
    gutil_slice_free(req);
}

static
void
giorpc_peer_request_data_destroy(
    gpointer req)
{
    giorpc_peer_request_data_free(req);
}

static
GIoRpcPeerWriteData*
giorpc_peer_write_data_new_take(
    GBytes* bytes,
    GIoRpcPeerPacketDoneFunc done)
{
    GIoRpcPeerWriteData* wd = g_slice_new(GIoRpcPeerWriteData);

    wd->bytes = bytes; /* Take ownership */
    wd->done = done;
    return wd;
}

static
void
giorpc_peer_write_data_done(
    GIoRpcPeerWriteData* wd,
    GIoRpcPeerObject* self,
    GIORPC_PEER_ERROR error)
{
    if (wd) {
        if (self && wd->done) {
            wd->done(self, error);
        }
        if (wd->bytes) {
            g_bytes_unref(wd->bytes);
        }
        gutil_slice_free(wd);
    }
}

static
void
giorpc_peer_writeq_flush(
    GIoRpcPeerObject* self)
{
    while (!g_queue_is_empty(&self->writeq)) {
        giorpc_peer_write_data_done(g_queue_pop_head(&self->writeq),
            self, GIORPC_PEER_ERROR_CANCELLED);
    }
}

static
gboolean
giorpc_peer_set_state(
   GIoRpcPeerObject* self,
   GIORPC_PEER_STATE state)
{
    GIoRpcPeer* peer = &self->peer;

    if (state > peer->state) {
        RLOG2(self, "%s => %s", giorpc_peer_state_name(peer->state),
            giorpc_peer_state_name(state));
        peer->state = state;
        if (state == GIORPC_PEER_STATE_DONE) {
            giorpc_peer_call_abandon_all(self);
            giorpc_peer_writeq_flush(self);
            g_cancellable_cancel(self->cancel);
        }
        g_signal_emit(self, giorpc_peer_signals[SIGNAL_STATE_CHANGED], 0);
        return TRUE;
    }
    return FALSE;
}

static
void
giorpc_peer_done(
    GIoRpcPeerObject* self)
{
    giorpc_peer_set_state(self, GIORPC_PEER_STATE_DONE);
}

static
void
giorpc_peer_emit_request(
    GIoRpcPeerObject* self,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req)
{
    g_signal_emit(self, giorpc_peer_signals[SIGNAL_REQUEST],
        giorpc_peer_iid_quark(iid), iid, code, data, req);
}

static
void
giorpc_peer_emit_notify(
    GIoRpcPeerObject* self,
    guint iid,
    guint code,
    GBytes* data)
{
    g_signal_emit(self, giorpc_peer_signals[SIGNAL_NOTIFY],
        giorpc_peer_iid_quark(iid), iid, code, data);
}

static
void
giorpc_peer_emit_cancel(
    GIoRpcPeerObject* self,
    guint rid)
{
    g_signal_emit(self, giorpc_peer_signals[SIGNAL_CANCEL], 0, rid);
}

static
void
giorpc_peer_call_complete_result(
    GIoRpcPeerObject* self,
    guint rid,
    GBytes* data)
{
    GIoRpcPeerCall* call = giorpc_peer_call_steal(self, rid);

    if (call) {
        if (call->response) {
            call->response(&self->peer, data, NULL, call->user_data);
        }
        giorpc_peer_call_unref(call);
    }
}

static
void
giorpc_peer_call_complete_error(
    GIoRpcPeerObject* self,
    guint rid,
    GIORPC_PEER_ERROR err)
{
    GIoRpcPeerCall* call = giorpc_peer_call_steal(self, rid);

    if (call) {
        if (call->response) {
            GError* error = giorpc_peer_error_new(call->code, err);

            call->response(&self->peer, NULL, error, call->user_data);
            g_error_free(error);
        }
        giorpc_peer_call_unref(call);
    }
}

/*==========================================================================*
 * Read context
 *
 * The read buffer must remain alive until completion of the read operation,
 * possibly outliving GIoRpcPeer. That's achieved by keeping an additional
 * reference to GByteArray for the duration of the read.
 *==========================================================================*/

typedef struct giorpc_peer_read {
    GUtilWeakRef* ref;
    GByteArray* buf;
    guchar type;
    gsize n;
} GIoRpcPeerRead;

static
GIoRpcPeerRead*
giorpc_peer_read_new(
    GUtilWeakRef* ref,
    GByteArray* buf)
{
    GIoRpcPeerRead* read = g_slice_new0(GIoRpcPeerRead);

    read->ref = gutil_weakref_ref(ref);
    read->buf = g_byte_array_ref(buf);
    return read;
}

static
void
giorpc_peer_read_free(
    GIoRpcPeerRead* read)
{
    if (read) {
        gutil_weakref_unref(read->ref);
        g_byte_array_unref(read->buf);
        gutil_slice_free(read);
    }
}

/*==========================================================================*
 * Write context
 *
 * The data must remain alive until completion of the write operation,
 * possibly outliving GIoRpcPeer. That's achieved by keeping an additional
 * reference to GBytes object for the duration of the write.
 *==========================================================================*/

struct giorpc_peer_write {
    GUtilWeakRef* ref;
    GIoRpcPeerWriteData* data; /* NULL when flushing */
};

static
GIoRpcPeerWrite*
giorpc_peer_write_new(
    GUtilWeakRef* ref,
    GIoRpcPeerWriteData* wd)
{
    GIoRpcPeerWrite* write = g_slice_new(GIoRpcPeerWrite);

    write->ref = gutil_weakref_ref(ref);
    write->data = wd;
    return write;
}

static
void
giorpc_peer_write_free(
    GIoRpcPeerWrite* write)
{
    if (write) {
        if (write->data) {
            GIoRpcPeerObject* self = gutil_weakref_get(write->ref);

            giorpc_peer_write_data_done(write->data, self,
                GIORPC_PEER_ERROR_NONE);
            gutil_object_unref(self);
        }
        gutil_weakref_unref(write->ref);
        gutil_slice_free(write);
    }
}

/*==========================================================================*
 * Packet handlers
 *==========================================================================*/

static
void
giorpc_peer_packet_notify_handle(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketNotify* notify = G_CAST(packet,
        GIoRpcPeerPacketNotify, packet);

    giorpc_peer_emit_notify(self, notify->iid, notify->code, notify->data);
}

static
void
giorpc_peer_packet_notify_free(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketNotify* notify = G_CAST(packet,
        GIoRpcPeerPacketNotify, packet);

    g_bytes_unref(notify->data);
    g_free(notify);
}

static
GIoRpcPeerPacket*
giorpc_peer_packet_notify_new_take(
    guint iid,
    guint code,
    GBytes* data)
{
    static const GIoRpcPeerPacketType block_notify_type = {
        "NOTIFY",
        GIORPC_SYNC_FLAG_BLOCK_NOTIFY,
        giorpc_peer_packet_notify_handle,
        giorpc_peer_packet_notify_free
    };

    GIoRpcPeerPacketNotify* notify = g_new(GIoRpcPeerPacketNotify, 1);
    GIoRpcPeerPacket* packet = &notify->packet;

    packet->type = &block_notify_type;
    notify->iid = iid;
    notify->code = code;
    notify->data = data; /* Take ownership */
    return packet;
}

static
void
giorpc_peer_packet_request_handle(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketRequest* request = G_CAST(packet,
        GIoRpcPeerPacketRequest, packet);

    giorpc_peer_emit_request(self, request->iid, request->code, request->data,
        request->req);
    giorpc_request_delivered(request->req, &self->peer);
}

static
void
giorpc_peer_packet_request_free(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketRequest* request = G_CAST(packet,
        GIoRpcPeerPacketRequest, packet);

    giorpc_request_unref(request->req);
    g_bytes_unref(request->data);
    g_free(request);
}

static
GIoRpcPeerPacket*
giorpc_peer_packet_request_new_take(
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req)
{
    static const GIoRpcPeerPacketType block_request_type = {
        "REQUEST",
        GIORPC_SYNC_FLAG_BLOCK_REQUEST,
        giorpc_peer_packet_request_handle,
        giorpc_peer_packet_request_free
    };

    GIoRpcPeerPacketRequest* request = g_new(GIoRpcPeerPacketRequest, 1);
    GIoRpcPeerPacket* packet = &request->packet;

    packet->type = &block_request_type;
    request->iid = iid;
    request->code = code;
    request->data = data; /* Take ownership */
    request->req = req;   /* Take ownership */
    return packet;
}

static
void
giorpc_peer_packet_result_handle(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketResult* result = G_CAST(packet,
        GIoRpcPeerPacketResult, packet);

    giorpc_peer_call_complete_result(self, result->rid, result->data);
    result->rid = 0;
}

static
void
giorpc_peer_packet_result_free(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketResult* result = G_CAST(packet,
        GIoRpcPeerPacketResult, packet);

    giorpc_peer_call_drop(self, result->rid);
    g_bytes_unref(result->data);
    g_free(result);
}

static
GIoRpcPeerPacket*
giorpc_peer_packet_result_new_take(
    guint rid,
    GBytes* data)
{
    static const GIoRpcPeerPacketType block_result_type = {
        "RESULT",
        GIORPC_SYNC_FLAG_BLOCK_RESPONSE,
        giorpc_peer_packet_result_handle,
        giorpc_peer_packet_result_free
    };

    GIoRpcPeerPacketResult* result = g_new(GIoRpcPeerPacketResult, 1);
    GIoRpcPeerPacket* packet = &result->packet;

    packet->type = &block_result_type;
    result->rid = rid;
    result->data = data; /* Take ownership */
    return packet;
}

static
void
giorpc_peer_packet_error_handle(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketError* error = G_CAST(packet,
        GIoRpcPeerPacketError, packet);

    giorpc_peer_call_complete_error(self, error->rid, error->err);
    error->rid = 0;
}

static
void
giorpc_peer_packet_error_free(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketError* error = G_CAST(packet,
        GIoRpcPeerPacketError, packet);

    giorpc_peer_call_drop(self, error->rid);
    g_free(error);
}

static
GIoRpcPeerPacket*
giorpc_peer_packet_error_new(
    guint rid,
    GIORPC_PEER_ERROR err)
{
    static const GIoRpcPeerPacketType block_error_type = {
        "ERROR",
        GIORPC_SYNC_FLAG_BLOCK_RESPONSE,
        giorpc_peer_packet_error_handle,
        giorpc_peer_packet_error_free
    };

    GIoRpcPeerPacketError* error = g_new(GIoRpcPeerPacketError, 1);
    GIoRpcPeerPacket* packet = &error->packet;

    packet->type = &block_error_type;
    error->rid = rid;
    error->err = err;
    return packet;
}

static
void
giorpc_peer_packet_cancel_handle(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketCancel* cancel = G_CAST(packet,
        GIoRpcPeerPacketCancel, packet);

    giorpc_peer_emit_cancel(self, cancel->rid);
    cancel->rid = 0;
}

static
void
giorpc_peer_packet_cancel_free(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    GIoRpcPeerPacketCancel* cancel = G_CAST(packet,
        GIoRpcPeerPacketCancel, packet);

    giorpc_peer_call_drop(self, cancel->rid);
    g_free(cancel);
}

static
GIoRpcPeerPacket*
giorpc_peer_packet_cancel_new(
    guint rid)
{
    static const GIoRpcPeerPacketType block_cancel_type = {
        "CANCEL",
        GIORPC_SYNC_FLAG_BLOCK_RESPONSE,
        giorpc_peer_packet_cancel_handle,
        giorpc_peer_packet_cancel_free
    };

    GIoRpcPeerPacketCancel* cancel = g_new(GIoRpcPeerPacketCancel, 1);
    GIoRpcPeerPacket* packet = &cancel->packet;

    packet->type = &block_cancel_type;
    cancel->rid = rid;
    return packet;
}

static
void
giorpc_peer_packet_block(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    RLOG1(self, "Queuing %s", packet->type->name);
    g_queue_push_tail(&self->blocked, packet);
}

static
GIoRpcPeerPacket*
giorpc_peer_packet_unblock(
    GIoRpcPeerObject* self)
{
    GList* l;
    const GIORPC_SYNC_FLAGS block_flags = self->sync ?
        self->sync->flags : GIORPC_SYNC_FLAGS_NONE;

    for (l = self->blocked.head; l; l = l->next) {
        GIoRpcPeerPacket* packet = l->data;

        if (!(packet->type->flag & block_flags)) {
            g_queue_delete_link(&self->blocked, l);
            return packet;
        }
    }
    return NULL;
}

static
gboolean
giorpc_peer_packet_handle(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket* packet)
{
    if (packet) {
        const GIoRpcPeerPacketType* type = packet->type;

        type->handle(self, packet);
        type->free(self, packet);
        return TRUE;
    }
    return FALSE;
}

/*==========================================================================*
 * Sync call implementation
 *==========================================================================*/

static
void
giorpc_peer_call_sync_flush(
    GIoRpcPeerObject* self)
{
    /*
     * Dequeue blocked callbacks one by one because the handler may decide
     * to cancel the request which has already been responded to, but  the
     * response was blocked. In such case we need to drop the response
     * rather than deliver it. This is also the reason why access to
     * the block list has to be synchronized.
     */
    while (giorpc_peer_packet_handle(self, giorpc_peer_packet_unblock(self)));
}

static
void
giorpc_peer_call_sync_done(
    GIoRpcPeerSyncCall* sync)
{
    if (sync->cancel_source) {
        g_source_destroy(sync->cancel_source);
        g_source_unref(sync->cancel_source);
        sync->cancel_source = NULL;
    }
    if (sync->timeout_source) {
        g_source_destroy(sync->timeout_source);
        g_source_unref(sync->timeout_source);
        sync->timeout_source = NULL;
    }
    if (sync->call_id) {
        giorpc_peer_call_drop(sync->owner, sync->call_id);
        sync->call_id = 0;
    }
    g_main_loop_quit(sync->loop);
}

static
gboolean
giorpc_peer_call_sync_timeout(
    gpointer user_data)
{
    GIoRpcPeerSyncCall* sync = user_data;

    RLOG1(sync->owner, "Sync call [%06u] timed_out", sync->call_id);
    g_source_unref(sync->timeout_source);
    sync->timeout_source = NULL;
    giorpc_peer_set_error(sync->error, giorpc_peer_error_timeout);
    giorpc_peer_call_sync_done(sync);
    return G_SOURCE_REMOVE;
}

static
gboolean
giorpc_peer_call_sync_cancelled(
    GCancellable* cancel,
    gpointer user_data)
{
    GIoRpcPeerSyncCall* sync = user_data;

    RLOG1(sync->owner, "Sync call [%06u] cancelled", sync->call_id);
    g_source_unref(sync->cancel_source);
    sync->cancel_source = NULL;
    giorpc_peer_set_error(sync->error, giorpc_peer_error_cancelled);
    giorpc_peer_call_sync_done(sync);
    return G_SOURCE_REMOVE;
}

static
void
giorpc_peer_call_sync_complete(
    GIoRpcPeer* peer,
    GBytes* response,
    const GError* error,
    gpointer user_data)
{
    GIoRpcPeerSyncCall* sync = user_data;

    if (!error) {
        sync->response = g_bytes_ref(response);
    } else if (sync->error) {
        g_propagate_error(sync->error, g_error_copy(error));
    }
    sync->call_id = 0;
    giorpc_peer_call_sync_done(sync);
}

static
void
giorpc_peer_call_sync_init(
    GIoRpcPeerSyncCall* sync,
    GIoRpcPeerObject* owner,
    guint call_id,
    GIORPC_SYNC_FLAGS flags,
    gint timeout_ms,
    GCancellable* cancel,
    GError** error)
{
    GIoRpcPeer* peer = &owner->peer;

    memset(sync, 0, sizeof(*sync));
    sync->owner = owner;
    sync->loop = g_main_loop_new(peer->context, FALSE);
    sync->flags = flags;
    sync->call_id = call_id;
    sync->error = error;
    if (cancel) {
        sync->cancel_source = g_cancellable_source_new(cancel);
        g_source_set_callback(sync->cancel_source, (GSourceFunc)
            giorpc_peer_call_sync_cancelled, sync, NULL);
        g_source_attach(sync->cancel_source, peer->context);
    }
    if (timeout_ms > 0) {
        sync->timeout_source = g_timeout_source_new(timeout_ms);
        g_source_set_callback(sync->timeout_source,
            giorpc_peer_call_sync_timeout, sync, NULL);
        g_source_attach(sync->timeout_source, peer->context);
    }
}


static
void
giorpc_peer_call_sync_cleanup(
    GIoRpcPeerSyncCall* sync)
{
    g_main_loop_unref(sync->loop);
}

/*==========================================================================*
 * Packet handlers
 *==========================================================================*/

static
gboolean
giorpc_peer_parse_start(
    GIoRpcPeerObject* self,
    const void* payload,
    guint n)
{
    GUtilRange data;
    guint64 version;
    guint tags[2];
    GUtilData val[G_N_ELEMENTS(tags) - 1];

    /*
     *  START
     *  =====
     *
     *  +---+---+---+---+---+---+---+---+
     *  |  0x01                         |
     *  +---+---+---+---+---+---+---+---+
     *  |  N (unsigned MBN)
     *
     *  payload starts here
     *
     *  +---
     *  |  M (unsigned MBN)
     *  +---
     *  |  TLVs (repeated M times)
     *  +---
     */

    data.end = (data.ptr = payload) + n;
    tags[0] = GIORPC_START_TAG_VERSION;
    tags[1] = 0;
    if (giorpc_tlv_array_decode(&data, tags, val) &&
        gutil_unsigned_mbn_decode2(val, &version) &&
        version > 0) {
        GIoRpcPeer* peer = &self->peer;

        if (data.ptr == data.end || version > GIORPC_PROTOCOL_VERSION) {
            if (peer->state == GIORPC_PEER_STATE_STARTING) {
                if (version <= (guint) G_MAXINT) {
                    RLOG1(self, "> START v%u", (guint) version);
                    peer->version = version;

                    /* Unblock the packet queue if needed */
                    if (!self->write) {
                        GIoRpcPeerWriteData* wd =
                            g_queue_pop_head(&self->writeq);

                        if (wd) {
                            giorpc_peer_write_bytes(self, wd->bytes,
                                self->write = giorpc_peer_write_new(self->ref,
                                    wd));
                        }
                    }
                    giorpc_peer_set_state(self, GIORPC_PEER_STATE_STARTED);
                    return TRUE;
                } else {
                    RLOG1(self, "Version %" G_GUINT64_FORMAT " is too large",
                        version);
                }
            } else {
                RLOG1(self, "Unexpected START in state %s",
                    giorpc_peer_state_name(peer->state));
            }
        } else {
            RLOG(self, "Trailing garbage in START payload");
        }
    } else {
        RLOG(self, "Broken START payload");
    }

    return FALSE;
}

static
gboolean
giorpc_peer_parse_finish(
    GIoRpcPeerObject* self,
    const void* payload,
    guint n)
{
    /*
     *  FINISH
     *  ======
     *
     *  +---+---+---+---+---+---+---+---+
     *  |  0x02                         |
     *  +---+---+---+---+---+---+---+---+
     *  |  0x00
     *  +---+---+---+---+---+---+---+---+
     */
    RLOG(self, "> FINISH");
    /* Emit the state signal first, handlers may cancel some requests */
    giorpc_peer_done(self);
    /* The complete all the remaining ones with GIORPC_ERROR_ABANDONED */
    giorpc_peer_call_abandon_all(self);
    return TRUE;
}

static
gboolean
giorpc_peer_parse_notify(
    GIoRpcPeerObject* self,
    const void* payload,
    guint n,
    GIoRpcPeerPacket** out)
{
    GUtilRange pos;
    guint64 iface, code;

    /*
     *  NOTIFY
     *  ======
     *
     *  +---+---+---+---+---+---+---+---+
     *  |  0x03                         |
     *  +---+---+---+---+---+---+---+---+
     *  |  N (unsigned MBN)
     *
     *  payload starts here
     *
     *  +---
     *  |  IID (unsigned MBN)
     *  +---
     *  |  CODE (unsigned MBN)
     *  +---
     *  |  NOTIFY DATA (N - sizeof CODE bytes)
     *  +---
     */

    pos.end = (pos.ptr = payload) + n;
    if (gutil_unsigned_mbn_decode(&pos, &iface) &&
        gutil_unsigned_mbn_decode(&pos, &code)) {
        if (!giorpc_peer_valid_iid(iface)) {
            RLOG1(self, "Invalid NOTIFY IID %" G_GUINT64_FORMAT, iface);
        } else if (!giorpc_peer_valid_code(code)) {
            RLOG1(self, "Invalid NOTIFY code %" G_GUINT64_FORMAT, code);
        } else {
            const GIoRpcPeer* peer = &self->peer;

            if (peer->state == GIORPC_PEER_STATE_STARTED) {
                const guint iid = (guint) iface;
                const guint ncode = (guint) code;
                GIoRpcPeerPacket* packet;
                GUtilData data;

                data.size = pos.end - pos.ptr;
                data.bytes = (data.size ? pos.ptr : NULL);
#if GUTIL_LOG_DEBUG
                if (data.size) {
                    RLOG3(self, "> NOTIFY %u:%u %u byte(s)", iid, ncode,
                        (guint) data.size);
                    DUMP_DATA(data.bytes, data.size);
                } else {
                    RLOG2(self, "> NOTIFY %u:%u", iid, ncode);
                }
#endif /* GUTIL_LOG_DEBUG */
                packet = giorpc_peer_packet_notify_new_take(iid, ncode,
                    giorpc_peer_steal_bytes(self, &data));
                if (self->sync &&
                   (self->sync->flags & GIORPC_SYNC_FLAG_BLOCK_NOTIFY)) {
                    /* Notify signals are disallowed by the sync call */
                    giorpc_peer_packet_block(self, packet);
                } else {
                    /* Handle it after submitting the next read */
                    *out = packet;
                }
                return TRUE;
            } else {
                RLOG1(self, "Unexpected NOTIFY in state %s",
                    giorpc_peer_state_name(peer->state));
            }
        }
    } else {
        RLOG(self, "Broken NOTIFY payload");
    }
    return FALSE;
}

static
gboolean
giorpc_peer_parse_request(
    GIoRpcPeerObject* self,
    const void* payload,
    guint n,
    GIoRpcPeerPacket** out)
{
    GUtilRange pos;
    guint64 iface, code, id, flags;

    /*
     *  REQUEST
     *  =======
     *
     *  +---+---+---+---+---+---+---+---+
     *  |  0x04                         |
     *  +---+---+---+---+---+---+---+---+
     *  |  N (unsigned MBN)
     *
     *  payload starts here
     *
     *  +---
     *  |  IID (unsigned MBN)
     *  +---
     *  |  CODE (unsigned MBN)
     *  +---
     *  |  ID (unsigned MBN)
     *  +---
     *  |  FLAGS (unsigned MBN)
     *  +---
     *  |  REQUEST DATA (the remaining part of the packet)
     *  +---
     */
    pos.end = (pos.ptr = payload) + n;
    if (gutil_unsigned_mbn_decode(&pos, &iface) &&
        gutil_unsigned_mbn_decode(&pos, &code) &&
        gutil_unsigned_mbn_decode(&pos, &id) &&
        gutil_unsigned_mbn_decode(&pos, &flags)) {
        if (!giorpc_peer_valid_iid(iface)) {
            RLOG1(self, "Invalid REQUEST IID %" G_GUINT64_FORMAT, iface);
        } else if (!giorpc_peer_valid_code(code)) {
            RLOG1(self, "Invalid REQUEST code %" G_GUINT64_FORMAT, code);
        } else if (!giorpc_peer_valid_id(id)) {
            RLOG1(self, "Invalid REQUEST id %" G_GUINT64_FORMAT, id);
        } else {
            const GIoRpcPeer* peer = &self->peer;
            const guint rc = (guint) code;
            const guint rid = (guint) id;
            const GIORPC_REQUEST_FLAGS rf = (GIORPC_REQUEST_FLAGS)flags;

            if (peer->state == GIORPC_PEER_STATE_STARTED) {
                const guint iid = (guint) iface;
                GIoRpcRequest* req = giorpc_request_new(self->ref, rid, rf);
                GIoRpcPeerPacket* packet;
                GUtilData data;

                data.size = pos.end - pos.ptr;
                data.bytes = (data.size ? pos.ptr : NULL);
#if GUTIL_LOG_DEBUG
                if (data.size) {
                    RLOG4(self, "> REQUEST [%06u] %u:%u %u byte(s)", rid, iid,
                        rc, (guint) data.size);
                    DUMP_DATA(data.bytes, data.size);
                } else {
                    RLOG3(self, "> REQUEST [%06u] %u:%u", rid, iid, rc);
                }
#endif /* GUTIL_LOG_DEBUG */
                packet = giorpc_peer_packet_request_new_take(iid, rc,
                    giorpc_peer_steal_bytes(self, &data), req);
                if (self->sync &&
                   (self->sync->flags & GIORPC_SYNC_FLAG_BLOCK_REQUEST)) {
                    /* Request signals are disallowed by the sync call */
                    giorpc_peer_packet_block(self, packet);
                } else {
                    /* Will be handled after submitting the next read */
                    *out = packet;
                }
                return TRUE;
            } else {
                RLOG3(self, "Unexpected REQUEST [%06u] %u in state %s", rid,
                    rc, giorpc_peer_state_name(peer->state));
            }
        }
    } else {
        RLOG(self, "Broken REQUEST payload");
    }
    return FALSE;
}

static
gboolean
giorpc_peer_parse_result(
    GIoRpcPeerObject* self,
    const void* payload,
    guint n,
    GIoRpcPeerPacket** out)
{
    GUtilRange pos;
    guint64 id;

    /*
     *  RESULT
     *  ======
     *
     *  +---+---+---+---+---+---+---+---+
     *  |  0x05                         |
     *  +---+---+---+---+---+---+---+---+
     *  |  N (unsigned MBN)
     *
     *  payload starts here
     *
     *  +---
     *  |  ID (unsigned MBN)
     *  +---
     *  |  RESULT DATA (N - sizeof ID) bytes)
     *  +---
     */
    pos.end = (pos.ptr = payload) + n;
    if (gutil_unsigned_mbn_decode(&pos, &id)) {
        GIoRpcPeer* peer = &self->peer;

        if (peer->state < GIORPC_PEER_STATE_STARTED) {
            RLOG1(self, "Unexpected RESULT in state %s",
                giorpc_peer_state_name(peer->state));
        } else if (!giorpc_peer_valid_id(id)) {
            RLOG1(self, "Invalid RESULT id %" G_GUINT64_FORMAT, id);
        } else {
            const guint rid = (guint) id;
            GUtilData data;

            data.size = pos.end - pos.ptr;
            data.bytes = (data.size ? pos.ptr : NULL);
#if GUTIL_LOG_DEBUG
            if (data.size) {
                RLOG2(self, "> RESULT [%06u] %u byte(s)", rid, (guint)
                    data.size);
                DUMP_DATA(data.bytes, data.size);
            } else {
                RLOG1(self, "> RESULT [%06u]", rid);
            }
#endif /* GUTIL_LOG_DEBUG */
            if (giorpc_peer_call_id_valid(self, rid)) {
                GIoRpcPeerPacket* packet =
                    giorpc_peer_packet_result_new_take(rid,
                        giorpc_peer_steal_bytes(self, &data));

                if (self->sync && self->sync->call_id != rid &&
                    (self->sync->flags & GIORPC_SYNC_FLAG_BLOCK_RESPONSE)) {
                    /*
                     * Response handling is disallowed by the sync call.
                     * The call is left in the table for the time being
                     * to allow it to be cancelled before getting unblocked.
                     */
                    giorpc_peer_packet_block(self, packet);
                } else {
                    /* Will be handled after submitting the next read */
                    *out = packet;
                }
            } else {
                RLOG1(self, "Unknown (cancelled) request [%06u]", rid);
            }
            return TRUE;
        }
    } else {
        RLOG(self, "Broken RESULT payload");
    }
    return FALSE;
}

static
gboolean
giorpc_peer_parse_error(
    GIoRpcPeerObject* self,
    const void* payload,
    guint n,
    GIoRpcPeerPacket** out)
{
    GUtilRange pos;
    guint64 id, error;

    /*
     *  ERROR
     *  =====
     *
     *  +---+---+---+---+---+---+---+---+
     *  |  0x06                         |
     *  +---+---+---+---+---+---+---+---+
     *  |  N (unsigned MBN)
     *
     *  payload starts here
     *
     *  +---
     *  |  ID (unsigned MBN)
     *  +---
     *  |  ERROR CODE (unsigned MBN)
     *  +---
     */
    pos.end = (pos.ptr = payload) + n;
    if (gutil_unsigned_mbn_decode(&pos, &id) &&
        gutil_unsigned_mbn_decode(&pos, &error)) {
        GIoRpcPeer* peer = &self->peer;

        if (peer->state < GIORPC_PEER_STATE_STARTED) {
            RLOG1(self, "Unexpected ERROR in state %s",
                giorpc_peer_state_name(peer->state));
        } else if (!giorpc_peer_valid_id(id)) {
            RLOG1(self, "Invalid ERROR id %" G_GUINT64_FORMAT, id);
        } else {
            const guint rid = (guint) id;
            const guint err = (guint) error;

            RLOG2(self, "> ERROR [%06u] %u", rid, err);
            if (giorpc_peer_call_id_valid(self, rid)) {
                GIoRpcPeerPacket* packet =
                    giorpc_peer_packet_error_new(rid, err);

                if (self->sync && self->sync->call_id != rid &&
                   (self->sync->flags & GIORPC_SYNC_FLAG_BLOCK_RESPONSE)) {
                    /*
                     * Error handling is disallowed by the sync call.
                     * We leave this call in the table for the time being
                     * to allow it to be cancelled before getting unblocked.
                     */
                    giorpc_peer_packet_block(self, packet);
                } else {
                    /* Will be handled after submitting the next read */
                    *out = packet;
                }
            } else {
                RLOG1(self, "Unknown (cancelled) request [%06u]", rid);
            }
            return TRUE;
        }
    } else {
        RLOG(self, "Broken ERROR payload");
    }
    return FALSE;
}

static
gboolean
giorpc_peer_parse_cancel(
    GIoRpcPeerObject* self,
    const void* payload,
    guint n,
    GIoRpcPeerPacket** out)
{
    GUtilRange pos;
    guint64 id;

    /*
     *  CANCEL
     *  =====
     *
     *  +---+---+---+---+---+---+---+---+
     *  |  0x07                         |
     *  +---+---+---+---+---+---+---+---+
     *  |  N (unsigned MBN)
     *
     *  payload starts here
     *
     *  +---
     *  |  ID (unsigned MBN)
     *  +---
     */
    pos.end = (pos.ptr = payload) + n;
    if (gutil_unsigned_mbn_decode(&pos, &id)) {
        GIoRpcPeer* peer = &self->peer;

        if (peer->state < GIORPC_PEER_STATE_STARTED) {
            RLOG1(self, "Unexpected CANCEL in state %s",
                giorpc_peer_state_name(peer->state));
        } else if (!giorpc_peer_valid_id(id)) {
            RLOG1(self, "Invalid CANCEL id %" G_GUINT64_FORMAT, id);
        } else {
            const guint rid = (guint) id;
            GIoRpcPeerPacket* packet = giorpc_peer_packet_cancel_new(rid);

            RLOG1(self, "> CANCEL [%06u]", rid);
            if (self->sync && self->sync->call_id != rid &&
               (self->sync->flags & GIORPC_SYNC_FLAG_BLOCK_CANCEL)) {
                /* Cancel signals are disallowed by the sync call */
                giorpc_peer_packet_block(self, packet);
            } else {
                /* Will be handled after submitting the next read */
                *out = packet;
            }
            return TRUE;
        }
    } else {
        RLOG(self, "Broken CANCEL payload");
    }
    return FALSE;
}

static
gboolean
giorpc_peer_parse_packet(
    GIoRpcPeerObject* self,
    guchar type,
    const void* payload,
    guint size,
    GIoRpcPeerPacket** out)
{
    switch ((GIORPC_PACKET_TYPE)type) {
    case GIORPC_PACKET_START:
        return giorpc_peer_parse_start(self, payload, size);
    case GIORPC_PACKET_FINISH:
        return giorpc_peer_parse_finish(self, payload, size);
    case GIORPC_PACKET_NOTIFY:
        return giorpc_peer_parse_notify(self, payload, size, out);
    case GIORPC_PACKET_REQUEST:
        return giorpc_peer_parse_request(self, payload, size, out);
    case GIORPC_PACKET_RESULT:
        return giorpc_peer_parse_result(self, payload, size, out);
    case GIORPC_PACKET_ERROR:
        return giorpc_peer_parse_error(self, payload, size, out);
    case GIORPC_PACKET_CANCEL:
        return giorpc_peer_parse_cancel(self, payload, size, out);
    }

    /* Skip unknown packets */
    RLOG1(self, "Unhandled packet %02x", type);
    return TRUE;
}

/*==========================================================================*
 * Input/output
 *==========================================================================*/

static
gboolean
giorpc_peer_read_packet(
    GIoRpcPeerObject* self,
    GIoRpcPeerPacket** packet)
{
    GByteArray* buf = self->buf;
    GUtilRange pos;
    guint64 n;

    /*
     *    +---+---+---+---+---+---+---+---+
     *    |  TYPE                         |
     *    +---+---+---+---+---+---+---+---+
     *    |  N (unsigned MBN)
     *    +---
     *    |  DATA (N bytes)
     *    +---
     *
     * The maximum N is 0x1fffff (2097151) bytes, i.e. the length fits into
     * 21 bits (which takes no more than 3 bytes in MBN encoding). If the N
     * is larger than that, the connection gets terminated.
     */
    GASSERT(buf->len >= GIORPC_MIN_PACKET_SIZE);
    pos.ptr = buf->data + 1;
    pos.end = buf->data + buf->len;

    /* We may need more data to decode the N */
    if (buf->data[1] & 0x80) {
        if (buf->len < 3) {
            /* Need more data */
            g_byte_array_set_size(self->buf, 3);
            return TRUE;
        } else if (buf->data[2] & 0x80) {
            if (buf->len < 4) {
                /* Need more data */
                g_byte_array_set_size(self->buf, 4);
                return TRUE;
            } else if (buf->data[3] & 0x80) {
                /* Packet length takes no more than 4 bytes */
                RLOG(self, "Broken packet length");
                DUMP_DATA(buf->data, buf->len);
                return FALSE;
            }
        }
    }

    /* There shouldn't be any failure here but let's still check */
    if (gutil_unsigned_mbn_decode(&pos, &n)) {
        const guint full_size = (pos.ptr - buf->data) + n;

        /*
         * The size of the entire packet can't be greater than the buffer
         * length because we never read more than we actually need. It can
         * be smaller though, after we have just parsed the N and N turned
         * out to be greater than zero.
         */
        if (full_size <= buf->len) {
            const guchar type = buf->data[0];

            RLOG(self, "Incoming packet:");
            DUMP_DATA(buf->data, buf->len);

            /* Parse the complete packet */
            if (giorpc_peer_parse_packet(self, type, pos.ptr, n, packet)) {
                /* Start reading the next packet */
                g_byte_array_set_size(self->buf, 0);
                return TRUE;
            }
        } else {
            /* Read DATA (N bytes) */
            g_byte_array_set_size(self->buf, (pos.end - buf->data) + n);
            return TRUE;
        }
    }
    return FALSE;
}

static
void
giorpc_peer_read_done(
    GInputStream* in,
    gsize nbytes,
    const GError* error,
    gpointer user_data)
{
    GIoRpcPeerRead* read = user_data;
    GIoRpcPeerObject* self = gutil_weakref_get(read->ref);

    if (self) {
        if (error) {
            RLOG1(self, "Read error: %s", error->message);
            giorpc_peer_done(self);
        } else if (!nbytes) {
            RLOG(self, "EOF");
            giorpc_peer_done(self);
        } else {
            GIoRpcPeerPacket* packet = NULL;
            const guint offset = read->buf->len;

            RLOG1(self, "Received %u byte(s)", (guint) nbytes);
            DUMP_DATA(read->buf->data + (offset - nbytes), nbytes);
            if (giorpc_peer_read_packet(self, &packet)) {
                GByteArray* buf = self->buf;
                GInputStream* in = g_io_stream_get_input_stream(self->stream);

                /*
                 * Read handler must update the buffer size reserving space
                 * for the next read. If the reserved size doesn't grow, then
                 * we assume that we are starting reading a new packet.
                 */
                if (buf->len > offset) {
                    RLOG1(self, "Reading %u byte(s)", buf->len - offset);
                    giorpc_read_all_async(in, buf->data + offset,
                        buf->len - offset, self->cancel,
                        giorpc_peer_read_done, read);
                } else {
                    /*
                     * If the entire packet has been read and handled,
                     * self->buf may have been stolen, converted to GBytes
                     * and replaced with a new array. If that has happened,
                     * we need to release the remaining GIoRpcPeerRead ref
                     * and update read->buf with the new one.
                     */
                    if (read->buf != buf) {
                        g_byte_array_unref(read->buf);
                        read->buf = g_byte_array_ref(buf);
                    }

                    /*
                     * Submit next read before handling the packet. The
                     * packet handler may submit a sync call which would
                     * block forever or timeout without an active read.
                     */
                    g_byte_array_set_size(buf, GIORPC_MIN_PACKET_SIZE);
                    RLOG(self, "Receiving next packet");
                    giorpc_read_all_async(in, buf->data, buf->len,
                        self->cancel, giorpc_peer_read_done, read);

                    /* Now we can handle the packet */
                    giorpc_peer_packet_handle(self, packet);
                }
                /* GIoRpcPeerRead has been reused, don't free it */
                read = NULL;
            } else {
                /* Some sort of a protocol violation */
               giorpc_peer_done(self);
            }
        }
        g_object_unref(self);
    }
    giorpc_peer_read_free(read);
}

static
void
giorpc_peer_flush_done(
    GObject* stream,
    GAsyncResult* result,
    gpointer user_data)
{
    GIoRpcPeerWrite* write = user_data;
    GOutputStream* out = G_OUTPUT_STREAM(stream);
    GError* error = NULL;

    if (g_output_stream_flush_finish(out, result, &error)) {
        giorpc_peer_write_done(write, NULL);
    } else {
        giorpc_peer_write_done(write, error);
        g_error_free(error);
    }
}

static
void
giorpc_peer_write_data_finished(
    GOutputStream* out,
    const GError* error,
    gpointer user_data)
{
    giorpc_peer_write_done((GIoRpcPeerWrite*)user_data, error);
}

static
void
giorpc_peer_write_data(
    GIoRpcPeerObject* self,
    const GUtilData* data,
    GIoRpcPeerWrite* write)
{
    RLOG1(self, "Sending %u byte(s)", (guint) data->size);
    DUMP_DATA(data->bytes, data->size);
    giorpc_write_all_async(g_io_stream_get_output_stream(self->stream),
        data->bytes, data->size, self->cancel,
        giorpc_peer_write_data_finished, write);
}

static
void
giorpc_peer_write_bytes(
    GIoRpcPeerObject* self,
    GBytes* bytes,
    GIoRpcPeerWrite* write)
{
    GUtilData data;

    giorpc_peer_write_data(self, gutil_data_from_bytes(&data, bytes), write);
}

static
void
giorpc_peer_write_done(
    GIoRpcPeerWrite* write,
    const GError* error)
{
    GIoRpcPeerObject* self = gutil_weakref_get(write->ref);

    if (self) {
        GIoRpcPeerWriteData* wd = write->data;

        if (error) {
            RLOG2(self, "%s error: %s", wd ? "Write" : "Flush",
                error->message);
            self->write = NULL;
            giorpc_peer_write_data_done(wd, self,
                GIORPC_PEER_ERROR_UNSPECIFIED);
            write->data = NULL;
            giorpc_peer_done(self);
        } else {
            /* Only send queued packets after we have started */
            GIoRpcPeer* peer = &self->peer;
            GIoRpcPeerWriteData* next =
                (peer->state >= GIORPC_PEER_STATE_STARTED) ?
                g_queue_pop_head(&self->writeq) : NULL;

            if (next) {
                /* Reuse GIoRpcPeerWrite structure */
                GASSERT(self->write == write);
                write->data = next;
                giorpc_peer_write_bytes(self, next->bytes, write);
                giorpc_peer_write_data_done(wd, self, GIORPC_PEER_ERROR_NONE);
                write = NULL;  /* Don't free it just yet */
            } else if (wd) {
                RLOG(self, "Flushing");
                write->data = NULL;
                g_output_stream_flush_async
                    (g_io_stream_get_output_stream(self->stream),
                        G_PRIORITY_DEFAULT, self->cancel,
                        giorpc_peer_flush_done, write);
                giorpc_peer_write_data_done(wd, self, GIORPC_PEER_ERROR_NONE);
                write = NULL;  /* Don't free it just yet */
            } else {
                self->write = NULL;
            }
        }
        g_object_unref(self);
    }
    giorpc_peer_write_free(write);
}

static
void
giorpc_peer_write_queue_bytes(
    GIoRpcPeerObject* self,
    GBytes* bytes,
    GIoRpcPeerPacketDoneFunc done)
{
    GIoRpcPeerWriteData* wd = giorpc_peer_write_data_new_take(bytes, done);
    GIoRpcPeer* peer = &self->peer;

    if (self->write || peer->state != GIORPC_PEER_STATE_STARTED) {
        /* We are already writing a packet or our state is wrong */
        g_queue_push_tail(&self->writeq, wd);
    } else {
        /* Submit a new write */
        giorpc_peer_write_bytes(self, bytes, self->write =
            giorpc_peer_write_new(self->ref, wd));
    }
}

static
void
giorpc_peer_write_queue_static(
    GIoRpcPeerObject* self,
    const GUtilData* data,
    GIoRpcPeerPacketDoneFunc done)
{
    GIoRpcPeer* peer = &self->peer;

    /*
     * This is a slightly optimized version of giorpc_peer_write_queue_bytes()
     * which sometimes avoids allocating an unnecessary GBytes* object.
     */
    if (self->write || peer->state != GIORPC_PEER_STATE_STARTED) {
        /* We are already writing a packet or our state is wrong */
        g_queue_push_tail(&self->writeq, giorpc_peer_write_data_new_take
            (g_bytes_new_static(data->bytes, data->size), done));
    } else {
        /* Submit a new write, no need to allocate GBytes */
        giorpc_peer_write_data(self, data,  self->write =
            giorpc_peer_write_new(self->ref, giorpc_peer_write_data_new_take
                (NULL, done)));
    }
}

static
void
giorpc_peer_start_locked(
    GIoRpcPeerObject* self)
{
    GIoRpcPeer* peer = &self->peer;

    /* We can only start once */
    if (peer->state == GIORPC_PEER_STATE_INIT) {

        /*
         *  START
         *  =====
         *
         *  +---+---+---+---+---+---+---+---+
         *  |  0x01                         |
         *  +---+---+---+---+---+---+---+---+
         *  |  N (unsigned MBN)
         *  +---
         *  |  M (unsigned MBN)
         *  +---
         *  |  TLVs (repeated M times)
         *  +---
         */
        static const guchar start[] = {
            GIORPC_PACKET_START,
            4,  /* N */
            1,  /* M */
            GIORPC_START_TAG_VERSION,
            1,
            GIORPC_PROTOCOL_VERSION
        };
        static const GUtilData start_packet = { start, sizeof(start) };

        GByteArray* buf = g_byte_array_new();

        self->buf = buf;
        self->cancel = g_cancellable_new();
        g_byte_array_set_size(buf, GIORPC_MIN_PACKET_SIZE);

        RLOG(self, "Starting");

        /* Start reading the incoming data */
        RLOG(self, "Reading first packet");
        giorpc_read_all_async(g_io_stream_get_input_stream(self->stream),
            buf->data, buf->len, self->cancel, giorpc_peer_read_done,
            giorpc_peer_read_new(self->ref, buf));

        /*
         * Send the START packet. This is the only kind of packet that
         * we are sending in the GIORPC_PEER_STATE_INIT state, that's why
         * we can be sure that there's no write in progress and directly
         * call giorpc_peer_write_data()
         */
        RLOG1(self, "< START v%u", GIORPC_PROTOCOL_VERSION);
        GASSERT(!self->write);
        giorpc_peer_write_data(self, &start_packet, self->write =
            giorpc_peer_write_new(self->ref, giorpc_peer_write_data_new_take
                (NULL, NULL)));

        /* Switch to the STARTING state */
        giorpc_peer_set_state(self, GIORPC_PEER_STATE_STARTING);
    }
}

static
void
giorpc_peer_finish_done(
    GIoRpcPeerObject* self,
    GIORPC_PEER_ERROR error)
{
    giorpc_peer_done(self);
}

static
void
giorpc_peer_stop_locked(
    GIoRpcPeerObject* self)
{

    GIoRpcPeer* peer = &self->peer;

    if (peer->state > GIORPC_PEER_STATE_INIT &&
        peer->state < GIORPC_PEER_STATE_STOPPING) {

        /*
         *  FINISH
         *  ======
         *
         *  +---+---+---+---+---+---+---+---+
         *  |  0x02                         |
         *  +---+---+---+---+---+---+---+---+
         *  |  0x00
         *  +---+---+---+---+---+---+---+---+
         */
        static const guchar finish[] = { GIORPC_PACKET_FINISH, 0 };
        static const GUtilData finish_packet = { finish, sizeof(finish) };

        RLOG(self, "< FINISH");
        giorpc_peer_write_queue_static(self, &finish_packet,
            giorpc_peer_finish_done);

        /* Switch to the STOPPING state */
        giorpc_peer_set_state(self, GIORPC_PEER_STATE_STOPPING);
    } else if (peer->state == GIORPC_PEER_STATE_INIT) {
        /* Stop right from the INIT state - fairly useless but allowed */
        giorpc_peer_done(self);
    }
}

static
void
giorpc_peer_notify_locked(
    GIoRpcPeerObject* self,
    guint iid,
    guint code,
    GBytes* bytes)
{
    const GIoRpcPeer* peer = &self->peer;

    if (peer->state < GIORPC_PEER_STATE_STOPPING) {
        gsize data_size = 0;
        const void* data = bytes ? g_bytes_get_data(bytes, &data_size) : NULL;
        const guint iid_size = gutil_unsigned_mbn_size(iid);
        const guint code_size = gutil_unsigned_mbn_size(code);
        const guint n = iid_size + code_size + data_size;
        const guint n_size = gutil_unsigned_mbn_size(n);
        guchar* buf = g_malloc(1 + n_size + n);
        guint k = 0;

        /* Make sure we are started */
        giorpc_peer_start_locked(self);

        /*
         *  NOTIFY
         *  ======
         *
         *  +---+---+---+---+---+---+---+---+
         *  |  0x03                         |
         *  +---+---+---+---+---+---+---+---+
         *  |  N (unsigned MBN)
         *  +---
         *  |  IID (unsigned MBN)
         *  +---
         *  |  CODE (unsigned MBN)
         *  +---
         *  |  DATA (N - sizeof IID - sizeof PARAM) bytes
         *  +---
         */
        buf[k++] = GIORPC_PACKET_NOTIFY;
        k += gutil_unsigned_mbn_encode2(buf + k, n, n_size);
        k += gutil_unsigned_mbn_encode2(buf + k, iid, iid_size);
        k += gutil_unsigned_mbn_encode2(buf + k, code, code_size);
        if (data_size) {
            memcpy(buf + k, data, data_size);
            k += data_size;
        }

#if GUTIL_LOG_DEBUG
        if (data_size) {
            RLOG3(self, "< NOTIFY %u:%u %u byte(s)", iid, code,
                (guint) data_size);
            DUMP_DATA(data, data_size);
        } else {
            RLOG2(self, "< NOTIFY %u:%u", iid, code);
        }
#endif /* GUTIL_LOG_DEBUG */
        giorpc_peer_write_queue_bytes(self, g_bytes_new_take(buf, k), NULL);
    }
}

static
gboolean
giorpc_peer_request_locked(
    GIoRpcPeerObject* self,
    GIoRpcPeerCall* call,
    GBytes* data)
{
    const GIoRpcPeer* peer = &self->peer;

    /* Refuse to submit calls in a wrong state */
    if (peer->state < GIORPC_PEER_STATE_STOPPING) {
        gsize data_size = 0;
        gconstpointer data_bytes = data ?
            g_bytes_get_data(data, &data_size) :
            NULL;
        const GIORPC_REQUEST_FLAGS flags =
            call->response ? GIORPC_REQUEST_FLAGS_NONE :
            call->destroy ? GIORPC_REQUEST_FLAG_NO_RESULT_DATA :
            GIORPC_REQUEST_FLAG_ONE_WAY;
        const guint iid_size = gutil_unsigned_mbn_size(call->iid);
        const guint code_size = gutil_unsigned_mbn_size(call->code);
        const guint id_size = gutil_unsigned_mbn_size(call->id);
        const guint flags_size = gutil_unsigned_mbn_size(flags);
        const guint n = iid_size + code_size + id_size + flags_size + data_size;
        const guint n_size = gutil_unsigned_mbn_size(n);
        guchar* buf = g_malloc(1 + n_size + n);
        guint k = 0;

        /* Make sure we are started */
        giorpc_peer_start_locked(self);

        /*
         *  REQUEST
         *  =======
         *
         *  +---+---+---+---+---+---+---+---+
         *  |  0x04                         |
         *  +---+---+---+---+---+---+---+---+
         *  |  N (unsigned MBN)
         *  +---
         *  |  IID (unsigned MBN)
         *  +---
         *  |  CODE (unsigned MBN)
         *  +---
         *  |  ID (unsigned MBN)
         *  +---
         *  |  FLAGS (unsigned MBN)
         *  +---
         *  |  REQUEST DATA (the remaining part of the packet)
         *  +---
         */

        buf[k++] = GIORPC_PACKET_REQUEST;
        k += gutil_unsigned_mbn_encode2(buf + k, n, n_size);
        k += gutil_unsigned_mbn_encode2(buf + k, call->iid, iid_size);
        k += gutil_unsigned_mbn_encode2(buf + k, call->code, code_size);
        k += gutil_unsigned_mbn_encode2(buf + k, call->id, id_size);
        k += gutil_unsigned_mbn_encode2(buf + k, flags, flags_size);
        if (data_size) {
            memcpy(buf + k, data_bytes, data_size);
            k += data_size;
        }
#if GUTIL_LOG_DEBUG
        if (data_size) {
            RLOG4(self, "< REQUEST [%06x] %u:%u %u byte(s)", call->id,
                call->iid, call->code, (guint) data_size);
            DUMP_DATA(data_bytes, data_size);
        } else {
            RLOG3(self, "< REQUEST [%06x] %u:%u", call->id,
                call->iid, call->code);
        }
#endif /* GUTIL_LOG_DEBUG */
        giorpc_peer_write_queue_bytes(self, g_bytes_new_take(buf, k), NULL);

        /*
         * Keep this GIoRpcPeerCall around even if it has no callbacks, i.e.
         * there's nothing to do when the call completes - that way we can
         * be sure that ids of the active calls are unique.
         */
        return TRUE;
    } else {
        return FALSE;
    }
}

static
void
giorpc_peer_result_locked(
    GIoRpcPeerObject* self,
    guint id,
    GBytes* bytes)
{
    const GIoRpcPeer* peer = &self->peer;

    if (peer->state > GIORPC_PEER_STATE_INIT &&
        peer->state < GIORPC_PEER_STATE_STOPPING) {
        gsize data_size = 0;
        const void* data = bytes ? g_bytes_get_data(bytes, &data_size) : NULL;
        const guint id_size = gutil_unsigned_mbn_size(id);
        const guint n = id_size + data_size;
        const guint n_size = gutil_unsigned_mbn_size(n);
        guchar* buf = g_malloc(1 + n_size + n);
        guint k = 0;

        /*
         *  RESULT
         *  ======
         *
         *  +---+---+---+---+---+---+---+---+
         *  |  0x05                         |
         *  +---+---+---+---+---+---+---+---+
         *  |  N (unsigned MBN)
         *  +---
         *  |  ID (unsigned MBN)
         *  +---
         *  |  DATA (N - sizeof ID) bytes
         *  +---
         */
        buf[k++] = GIORPC_PACKET_RESULT;
        k += gutil_unsigned_mbn_encode2(buf + k, n, n_size);
        k += gutil_unsigned_mbn_encode2(buf + k, id, id_size);
        if (data_size) {
            memcpy(buf + k, data, data_size);
            k += data_size;
        }
#if GUTIL_LOG_DEBUG
        if (data_size) {
            RLOG2(self, "< RESULT [%06x] %u byte(s)", id, (guint) data_size);
            DUMP_DATA(data, data_size);
        } else {
            RLOG1(self, "< RESULT [%06x]", id);
        }
#endif /* GUTIL_LOG_DEBUG */
        giorpc_peer_write_queue_bytes(self, g_bytes_new_take(buf, k), NULL);
    }
}

static
void
giorpc_peer_error_locked(
    GIoRpcPeerObject* self,
    guint id,
    GIORPC_PEER_ERROR code)
{
    const GIoRpcPeer* peer = &self->peer;

    if (peer->state > GIORPC_PEER_STATE_INIT &&
        peer->state < GIORPC_PEER_STATE_STOPPING) {
        const guint code_size = gutil_unsigned_mbn_size(code);
        const guint id_size = gutil_unsigned_mbn_size(id);
        const guint n = id_size + code_size;
        const guint n_size = gutil_unsigned_mbn_size(n);
        guchar* buf = g_malloc(1 + n_size + n);
        guint k = 0;

        /*
         *  ERROR
         *  =====
         *
         *  +---+---+---+---+---+---+---+---+
         *  |  0x06                         |
         *  +---+---+---+---+---+---+---+---+
         *  |  N (unsigned MBN)
         *  +---
         *  |  ID (unsigned MBN)
         *  +---
         *  |  ERROR CODE (unsigned MBN)
         *  +---
         */
        buf[k++] = GIORPC_PACKET_ERROR;
        k += gutil_unsigned_mbn_encode2(buf + k, n, n_size);
        k += gutil_unsigned_mbn_encode2(buf + k, id, id_size);
        k += gutil_unsigned_mbn_encode2(buf + k, code, code_size);
        RLOG2(self, "< ERROR [%06x] %u", id, code);
        giorpc_peer_write_queue_bytes(self, g_bytes_new_take(buf, k), NULL);
    }
}

static
void
giorpc_peer_cancel_locked(
    GIoRpcPeerObject* self,
    guint rid)
{
    const GIoRpcPeer* peer = &self->peer;

    if (peer->state > GIORPC_PEER_STATE_INIT &&
        peer->state < GIORPC_PEER_STATE_STOPPING) {
        const guint rid_size = gutil_unsigned_mbn_size(rid);
        const guint n = rid_size;
        const guint n_size = gutil_unsigned_mbn_size(n);
        guchar* buf = g_malloc(1 + n_size + n);
        guint k = 0;

        /*
         *  CANCEL
         *  ======
         *
         *  +---+---+---+---+---+---+---+---+
         *  |  0x07                         |
         *  +---+---+---+---+---+---+---+---+
         *  |  N (unsigned MBN)
         *  +---
         *  |  ID (unsigned MBN)
         *  +---
         */
        buf[k++] = GIORPC_PACKET_CANCEL;
        k += gutil_unsigned_mbn_encode2(buf + k, n, n_size);
        k += gutil_unsigned_mbn_encode2(buf + k, rid, rid_size);
        RLOG1(self, "< CANCEL [%06x]", rid);
        giorpc_peer_write_queue_bytes(self, g_bytes_new_take(buf, k), NULL);
    }
}

static
void
giorpc_peer_request_proc(
    GIoRpcPeerObject* self,
    gpointer user_data)
{
    GIoRpcPeerRequestData* req = user_data;
    GIoRpcPeerCall* call = req->call;

    /*
     * Since we have returned the call id to the caller, we must now
     * complete the call even though we haven't actually submitted it.
     */
    if (!giorpc_peer_request_locked(self, call, req->data)) {
        if (giorpc_peer_call_steal(self, call->id)) {
            GError* error = giorpc_peer_error_abandoned();

            if (call->response) {
                call->response(&self->peer, NULL, error, call->user_data);
                g_error_free(error);
            }
            giorpc_peer_call_unref(call);
        }
    }
}

static
void
giorpc_peer_notify_proc(
    GIoRpcPeerObject* self,
    gpointer user_data)
{
    GIoRpcPeerNotifyData* notify = user_data;

    giorpc_peer_notify_locked(self, notify->iid, notify->code, notify->data);
}

static
void
giorpc_peer_notify_destroy(
    gpointer user_data)
{
    GIoRpcPeerNotifyData* notify = user_data;

    if (notify->data) {
        g_bytes_unref(notify->data);
    }
    gutil_slice_free(notify);
}

static
void
giorpc_peer_result_proc(
    GIoRpcPeerObject* self,
    gpointer user_data)
{
    GIoRpcPeerResultData* result = user_data;

    giorpc_peer_result_locked(self, result->id, result->data);
}

static
void
giorpc_peer_result_destroy(
    gpointer user_data)
{
    GIoRpcPeerResultData* result = user_data;

    if (result->data) {
        g_bytes_unref(result->data);
    }
    gutil_slice_free(result);
}

static
void
giorpc_peer_error_proc(
    GIoRpcPeerObject* self,
    gpointer user_data)
{
    GIoRpcPeerErrorData* error = user_data;

    giorpc_peer_error_locked(self, error->id, error->code);
}

static
void
giorpc_peer_error_destroy(
    gpointer error)
{
    g_slice_free(GIoRpcPeerErrorData, error);
}

static
void
giorpc_peer_cancel_proc(
    GIoRpcPeerObject* self,
    gpointer user_data)
{
    giorpc_peer_cancel_locked(self, GPOINTER_TO_UINT(user_data));
}

static
void
giorpc_peer_handler(
    GIoRpcPeerObject* self,
    GIoRpcPeerClosure* closure)
{
    ((GIoRpcPeerFunc)(closure->handler))(&self->peer, closure->user_data);
}

static
void
giorpc_peer_request_handler(
    GIoRpcPeerObject* self,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    GIoRpcPeerClosure2* closure)
{
    /* IID filtering is done for us by glib */
    if (closure->code == GIORPC_CODE_ANY || closure->code == code) {
        ((GIoRpcPeerRequestFunc)(closure->handler))(&self->peer,
            iid, code, data, req, closure->user_data);
    }
}

static
void
giorpc_peer_notify_handler(
    GIoRpcPeerObject* self,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcPeerClosure2* closure)
{
    /* IID filtering is done for us by glib */
    if (closure->code == GIORPC_CODE_ANY || closure->code == code) {
        ((GIoRpcPeerNotifyFunc)(closure->handler))(&self->peer,
            iid, code, data, closure->user_data);
    }
}

static
void
giorpc_peer_cancel_handler(
    GIoRpcPeerObject* self,
    guint rid,
    GIoRpcPeerClosure* closure)
{
    ((GIoRpcPeerCancelFunc)(closure->handler))(&self->peer, rid,
        closure->user_data);
}

static
gulong
giorpc_peer_add_handler(
    GIoRpcPeer* peer,
    GIORPC_PEER_SIGNAL signal,
    GCallback fn,
    GCallback handler,
    void* user_data)
{
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    /*
     * We can't directly connect the provided callback because it
     * expects the first parameter to point to public part of the
     * object but glib will call it with GIoRpcPeerObject as the
     * first parameter.
     */
    if (G_LIKELY(self) && G_LIKELY(handler)) {
        GIoRpcPeerClosure* c = giorpc_closure_new(GIoRpcPeerClosure);
        GCClosure* cc = &c->cclosure;

        cc->closure.data = c;
        cc->callback = fn;
        c->handler = handler;
        c->user_data = user_data;
        return g_signal_connect_closure_by_id(self, giorpc_peer_signals
            [signal], 0, &cc->closure, FALSE);
    }
    return 0;
}

static
gulong
giorpc_peer_add_handler2(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GIORPC_PEER_SIGNAL signal,
    GCallback fn,
    GCallback handler,
    void* user_data)
{
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    /*
     * We can't directly connect the provided callback because it
     * expects the first parameter to point to public part of the
     * object but glib will call it with GIoRpcPeerObject as the
     * first parameter.
     */
    if (G_LIKELY(self) && G_LIKELY(handler)) {
        GIoRpcPeerClosure2* c = giorpc_closure_new(GIoRpcPeerClosure2);
        GCClosure* cc = &c->cclosure;

        cc->closure.data = c;
        cc->callback = fn;
        c->handler = handler;
        c->user_data = user_data;
        c->code = code;
        return g_signal_connect_closure_by_id(self, giorpc_peer_signals
            [signal], giorpc_peer_iid_quark(iid), &cc->closure, FALSE);
    }
    return 0;
}

/*==========================================================================*
 * Internal API
 *==========================================================================*/

GIoRpcPeer*
giorpc_peer_weakref_get(
    GUtilWeakRef* ref)
{
    GObject* obj = gutil_weakref_get(ref);

    return obj ? &THIS(obj)->peer : NULL;
}

void
giorpc_peer_result(
    GIoRpcPeer* peer,
    guint id,
    GBytes* data)
{
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    if (G_LIKELY(self)) {
        GIoRpcPeer* peer = &self->peer;

        if (g_main_context_acquire(peer->context)) {
            giorpc_peer_result_locked(self, id, data);
            g_main_context_release(peer->context);
        } else {
            GIoRpcPeerResultData* result = g_slice_new(GIoRpcPeerResultData);

            result->id = id;
            result->data = data ? g_bytes_ref(data) : NULL;
            giorpc_peer_invoke_later_full(self, giorpc_peer_result_proc,
                giorpc_peer_result_destroy, result);
        }
    }
}

void
giorpc_peer_error(
    GIoRpcPeer* peer,
    guint id,
    GIORPC_PEER_ERROR code)
{
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    if (G_LIKELY(self)) {
        GIoRpcPeer* peer = &self->peer;

        if (g_main_context_acquire(peer->context)) {
            giorpc_peer_error_locked(self, id, code);
            g_main_context_release(peer->context);
        } else {
            GIoRpcPeerErrorData* error = g_slice_new(GIoRpcPeerErrorData);

            error->id = id;
            error->code = code;
            giorpc_peer_invoke_later_full(self, giorpc_peer_error_proc,
                giorpc_peer_error_destroy, error);
        }
    }
}

gulong
giorpc_peer_connect_internal(
    GIoRpcPeer* peer,
    GQuark detail,
    GClosure* closure)
{
    return g_signal_connect_closure_by_id(giorpc_peer_object_cast(peer),
        giorpc_peer_signals[SIGNAL_INTERNAL], detail, closure, FALSE);
}

void
giorpc_peer_disconnect_internal(
    GIoRpcPeer* peer,
    GQuark detail)
{
    g_signal_handlers_disconnect_matched(giorpc_peer_object_cast(peer),
        G_SIGNAL_MATCH_DETAIL, giorpc_peer_signals[SIGNAL_INTERNAL], detail,
        NULL, NULL, NULL);
}

void
giorpc_peer_emit_internal(
    GIoRpcPeer* peer,
    GQuark detail)
{
    g_signal_emit(giorpc_peer_object_cast(peer), giorpc_peer_signals
        [SIGNAL_INTERNAL], detail);
}

/*==========================================================================*
 * GObject
 *==========================================================================*/

static
void
giorpc_peer_object_finalize(
    GObject* object)
{
    GIoRpcPeerObject* self = THIS(object);
    GIoRpcPeer* peer = &self->peer;

    RLOG(self, "Destroyed");
    giorpc_peer_writeq_flush(self);
    if (self->requests) {
        /*
         * There's no need to synchronize access to the hashtable because
         * there're no more references to this object.
         */
        g_hash_table_destroy(self->requests);
    }
    gutil_weakref_unref(self->ref);
    g_queue_clear(&self->blocked);
    g_queue_clear(&self->writeq);
    g_mutex_clear(&self->requests_mutex);
    gutil_object_unref(self->stream);
    if (self->buf) {
        g_byte_array_unref(self->buf);
    }
    if (self->cancel) {
        g_cancellable_cancel(self->cancel);
        g_object_unref(self->cancel);
    }
    if (peer->context) {
        g_main_context_unref(peer->context);
    }
    G_OBJECT_CLASS(PARENT_CLASS)->finalize(object);
}

static
void
giorpc_peer_object_init(
    GIoRpcPeerObject* self)
{
    static gint next_tag = 1;
    GIoRpcPeer* peer = &self->peer;

    peer->state = GIORPC_PEER_STATE_INIT;
    g_mutex_init(&self->requests_mutex);
    g_queue_init(&self->blocked);
    g_queue_init(&self->writeq);
    self->tag = (g_atomic_int_add(&next_tag, 1) & G_MAXINT);
    RLOG(self, "Created");
}

static
void
giorpc_peer_object_class_init(
    GIoRpcPeerObjectClass* klass)
{
    const GType type = G_OBJECT_CLASS_TYPE(klass);

    G_OBJECT_CLASS(klass)->finalize = giorpc_peer_object_finalize;

    giorpc_peer_signals[SIGNAL_STATE_CHANGED] =
        g_signal_new(SIGNAL_STATE_CHANGED_NAME, type,
            G_SIGNAL_RUN_FIRST, 0, NULL, NULL, NULL,
            G_TYPE_NONE, 0);
    giorpc_peer_signals[SIGNAL_REQUEST] =
        g_signal_new(SIGNAL_REQUEST_NAME, type,
            G_SIGNAL_RUN_FIRST | G_SIGNAL_DETAILED, 0, NULL, NULL, NULL,
            G_TYPE_NONE, 4,
            G_TYPE_UINT, G_TYPE_UINT, G_TYPE_BYTES, G_TYPE_POINTER);
    giorpc_peer_signals[SIGNAL_NOTIFY] =
        g_signal_new(SIGNAL_NOTIFY_NAME, type,
            G_SIGNAL_RUN_FIRST | G_SIGNAL_DETAILED, 0, NULL, NULL, NULL,
            G_TYPE_NONE, 3,
            G_TYPE_UINT, G_TYPE_UINT, G_TYPE_BYTES);
    giorpc_peer_signals[SIGNAL_CANCEL] =
        g_signal_new(SIGNAL_CANCEL_NAME, type,
            G_SIGNAL_RUN_FIRST, 0, NULL, NULL, NULL,
            G_TYPE_NONE, 1,
            G_TYPE_UINT);
    giorpc_peer_signals[SIGNAL_INTERNAL] =
        g_signal_new(SIGNAL_INTERNAL_NAME, type,
            G_SIGNAL_RUN_FIRST | G_SIGNAL_DETAILED, 0, NULL, NULL, NULL,
            G_TYPE_NONE, 0);
}

/*==========================================================================*
 * API
 *==========================================================================*/

GIoRpcPeer*
giorpc_peer_new(
    GIOStream* stream,
    GMainContext* context)
{
    if (stream) {
        GIoRpcPeerObject* self = g_object_new(THIS_TYPE, NULL);
        GIoRpcPeer* peer = &self->peer;

        g_object_ref(self->stream = stream);
        self->ref = gutil_weakref_new(self);
        peer->context = g_main_context_ref(context ?
            context : g_main_context_default());
        return peer;
    }
    return NULL;
}

GIoRpcPeer*
giorpc_peer_ref(
    GIoRpcPeer* peer)
{
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    if (G_LIKELY(self)) {
        g_object_ref(self);
    }
    return peer;
}

void
giorpc_peer_unref(
    GIoRpcPeer* peer)
{
    gutil_object_unref(giorpc_peer_object_cast(peer));
}

void
giorpc_peer_start(
    GIoRpcPeer* peer)
{
    giorpc_peer_invoke_now_or_later(giorpc_peer_object_cast(peer),
        giorpc_peer_start_locked);
}

void
giorpc_peer_stop(
    GIoRpcPeer* peer)
{
    giorpc_peer_invoke_now_or_later(giorpc_peer_object_cast(peer),
        giorpc_peer_stop_locked);
}

GBytes*
giorpc_peer_call_sync(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIORPC_SYNC_FLAGS flags,
    gint timeout_ms,
    GCancellable* cancel,
    GError** error)
{
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    if (G_LIKELY(self)) {
        GIoRpcPeer* peer = &self->peer;

        if (g_main_context_acquire(peer->context)) {
            /*
             * It's ok to pass the pointer to uninitialized GIoRpcPeerSyncCall
             * because it's not going to be used until we run the event loop.
             */
            GIoRpcPeerSyncCall sync;
            GIoRpcPeerCall* call = giorpc_peer_call_new(self, iid, code,
                giorpc_peer_call_sync_complete, NULL, &sync);

            if (call) {
                /* Now we can setup the context and attach it to GIoRpcPeer */
                giorpc_peer_call_sync_init(&sync, self, call->id, flags,
                    timeout_ms, cancel, error);
                if (self->sync) {
                    /* Also block whatever is blocked by the outer sync call */
                    sync.flags |= self->sync->flags;
                    sync.prev = self->sync;
                }
                self->sync = &sync;

                /* Send the request and run the event loop */
                if (giorpc_peer_request_locked(self, call, data)) {
                    g_main_loop_run(sync.loop);
                } else {
                    giorpc_peer_call_drop(self, call->id);
                    giorpc_peer_set_error(error, giorpc_peer_error_abandoned);
                    call->id = 0;
                }

                /* Done with the event loop, release the context */
                self->sync = sync.prev;
                g_main_context_release(peer->context);

                /* Return the result to the caller */
                giorpc_peer_call_unref(call);
                giorpc_peer_call_sync_cleanup(&sync);
                if (!g_queue_is_empty(&self->blocked)) {
                    /* Handle blocked packets on a fresh stack */
                    giorpc_peer_invoke_later(self,
                        giorpc_peer_call_sync_flush);
                }
                return sync.response;
            } else {
                g_main_context_release(peer->context);
                giorpc_peer_set_error(error, giorpc_peer_error_invalid_args);
            }
        } else {
            RWARN2(self, "Sync call %u:%u from a wrong context, bailing out",
                iid, code);
            giorpc_peer_set_error(error, giorpc_peer_error_not_owner);
        }
    } else {
        giorpc_peer_set_error(error, giorpc_peer_error_invalid_args);
    }
    return NULL;
}

guint
giorpc_peer_call(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcPeerResponseFunc response,
    void* user_data,
    GDestroyNotify destroy)
{
    guint id = 0;
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    if (G_LIKELY(self)) {
        GIoRpcPeerCall* call = giorpc_peer_call_new(self, iid, code,
            response, destroy, user_data);

        if (call) {
            GIoRpcPeer* peer = &self->peer;

            if (g_main_context_acquire(peer->context)) {
                if (giorpc_peer_request_locked(self, call, data)) {
                    id = call->id;
                } else {
                    giorpc_peer_call_drop(self, call->id);
                    call->response = NULL;
                }
                g_main_context_release(peer->context);
                giorpc_peer_call_unref(call);
            } else {
                /* GIoRpcPeerRequestData takes ownership of call ref */
                id = call->id;
                giorpc_peer_invoke_later_full(self, giorpc_peer_request_proc,
                    giorpc_peer_request_data_destroy,
                    giorpc_peer_request_data_new(call, data));
            }
        }
    } else if (destroy) {
        destroy(user_data);
    }
    return id;
}

gboolean
giorpc_peer_cancel(
    GIoRpcPeer* peer,
    guint id)
{
    gboolean cancelled = FALSE;
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    if (G_LIKELY(self) && id) {
        /* The call is cancelled if we have removed it from the table */
        GIoRpcPeerCall* call = giorpc_peer_call_steal(self, id);

        if (call) {
            RLOG1(self, "Request %u was cancelled", id);
            if (g_main_context_acquire(peer->context)) {
                giorpc_peer_cancel_locked(self, id);
                g_main_context_release(peer->context);
            } else {
                giorpc_peer_invoke_later_full(self, giorpc_peer_cancel_proc,
                    NULL, GUINT_TO_POINTER(id));
            }
            giorpc_peer_call_unref(call);
            cancelled = TRUE;
        } else {
            RLOG1(self, "Request %u couldn't be cancelled", id);
        }
    }
    return cancelled;
}

void
giorpc_peer_notify(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data)
{
    GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

    if (G_LIKELY(self)) {
        GIoRpcPeer* peer = &self->peer;

        if (g_main_context_acquire(peer->context)) {
            giorpc_peer_notify_locked(self, iid, code, data);
            g_main_context_release(peer->context);
        } else {
            GIoRpcPeerNotifyData* notify = g_slice_new(GIoRpcPeerNotifyData);

            notify->iid = iid;
            notify->code = code;
            notify->data = data ? g_bytes_ref(data) : NULL;
            giorpc_peer_invoke_later_full(self, giorpc_peer_notify_proc,
                giorpc_peer_notify_destroy, notify);
        }
    }
}

gulong
giorpc_peer_add_state_handler(
    GIoRpcPeer* peer,
    GIoRpcPeerFunc fn,
    gpointer user_data)
{
    return giorpc_peer_add_handler(peer, SIGNAL_STATE_CHANGED,
        G_CALLBACK(giorpc_peer_handler),
        G_CALLBACK(fn), user_data);
}

gulong
giorpc_peer_add_request_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GIoRpcPeerRequestFunc fn,
    gpointer user_data)
{
    return G_LIKELY(code <= GIORPC_MAX_CODE) ?
        giorpc_peer_add_handler2(peer, iid, code, SIGNAL_REQUEST,
            G_CALLBACK(giorpc_peer_request_handler),
            G_CALLBACK(fn), user_data) : 0;
}

gulong
giorpc_peer_add_notify_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GIoRpcPeerNotifyFunc fn,
    gpointer user_data)
{
    return G_LIKELY(code <= GIORPC_MAX_CODE) ?
        giorpc_peer_add_handler2(peer, iid, code, SIGNAL_NOTIFY,
            G_CALLBACK(giorpc_peer_notify_handler),
            G_CALLBACK(fn), user_data) : 0;
}

gulong
giorpc_peer_add_cancel_handler(
    GIoRpcPeer* peer,
    GIoRpcPeerCancelFunc fn,
    gpointer user_data)
{
    return giorpc_peer_add_handler(peer, SIGNAL_CANCEL,
        G_CALLBACK(giorpc_peer_cancel_handler),
        G_CALLBACK(fn), user_data);
}

void
giorpc_peer_remove_handler(
    GIoRpcPeer* peer,
    gulong id)
{
    if (G_LIKELY(id)) {
        GIoRpcPeerObject* self = giorpc_peer_object_cast(peer);

        if (G_LIKELY(self)) {
            g_signal_handler_disconnect(self, id);
        }
    }
}

void
giorpc_peer_remove_handlers(
    GIoRpcPeer* peer,
    gulong* ids,
    int count)
{
    gutil_disconnect_handlers(giorpc_peer_object_cast(peer), ids, count);
}

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
