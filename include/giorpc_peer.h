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

#ifndef GIORPC_PEER_H
#define GIORPC_PEER_H

#include "giorpc_types.h"

#include <gio/gio.h>

G_BEGIN_DECLS

/*
 * The GIoRpcPeer API is thread-save.
 *
 * All callbacks, including the signal handlers, are invoked on the thread
 * associated with GMainContext specified at the creation time (NULL meaning
 * the default context i.e. the main thread). Beware of deadlocks.
 *
 * Particularly mind the race condition between the completion routine
 * invoked on the context thread and giorpc_peer_cancel() which can be
 * invoked from an arbitrary thread.
 *
 * In the simplest case (NULL context) this gets reduced to the trivial
 * single-threaded model which guarantees that there won't be any races
 * and deadlocks. At the same time allows associating different GIoRpcPeer
 * instances with different worker threads in applications which need that
 * for performance or some other reasons (e.g. if the main thread is running
 * non-glib event loop, which is often the case for Qt-based apps)
 */

typedef enum giorpc_peer_state {
    GIORPC_PEER_STATE_INIT,
    GIORPC_PEER_STATE_STARTING,
    GIORPC_PEER_STATE_STARTED,
    GIORPC_PEER_STATE_STOPPING,
    GIORPC_PEER_STATE_DONE
} GIORPC_PEER_STATE;

struct giorpc_peer {
    GMainContext* context;
    GIORPC_PEER_STATE state;
    guint version;
};

#define GIORPC_MIN_IID  (1)
#define GIORPC_MAX_IID  (0x1fffff)  /* 21 bit */
#define GIORPC_MIN_CODE (1)
#define GIORPC_MAX_CODE (0x1fffff)  /* 21 bit */

/* Wildcards for giorpc_peer_add_request/notify_handler() */
#define GIORPC_IID_ANY  (0)
#define GIORPC_CODE_ANY (0)

typedef enum giorpc_sync_flags {
    GIORPC_SYNC_FLAGS_NONE = 0,
    GIORPC_SYNC_FLAG_BLOCK_REQUEST = 0x01,
    GIORPC_SYNC_FLAG_BLOCK_RESPONSE = 0x02,
    GIORPC_SYNC_FLAG_BLOCK_NOTIFY = 0x04,
    GIORPC_SYNC_FLAG_BLOCK_CANCEL = 0x08,
    GIORPC_SYNC_FLAG_BLOCK_ALL =
        GIORPC_SYNC_FLAG_BLOCK_REQUEST |
        GIORPC_SYNC_FLAG_BLOCK_RESPONSE |
        GIORPC_SYNC_FLAG_BLOCK_NOTIFY |
        GIORPC_SYNC_FLAG_BLOCK_CANCEL
} GIORPC_SYNC_FLAGS;

#define GIORPC_SYNC_TIMEOUT_INFINITE (0)

typedef
void
(*GIoRpcPeerRequestFunc)(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data);

typedef
void
(*GIoRpcPeerResponseFunc)(
    GIoRpcPeer* peer,
    GBytes* response,       /* NULL on failure */
    const GError* error,    /* NULL on success */
    gpointer user_data);

typedef
void
(*GIoRpcPeerNotifyFunc)(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data);

typedef
void
(*GIoRpcPeerCancelFunc)(
    GIoRpcPeer* peer,
    guint id,
    gpointer user_data);

typedef
void
(*GIoRpcPeerFunc)(
    GIoRpcPeer* peer,
    void* user_data);

GIoRpcPeer*
giorpc_peer_new(
    GIOStream* stream,
    GMainContext* context);

GIoRpcPeer*
giorpc_peer_ref(
    GIoRpcPeer* peer);

void
giorpc_peer_unref(
    GIoRpcPeer* peer);

/*
 * Explicit giorpc_peer_start() is required if you're not making calls
 * or sending notifications, only receiving them. giorpc_peer_call() and
 * giorpc_peer_notify() implicitly start the session if necessary.
 */
void
giorpc_peer_start(
    GIoRpcPeer* peer);

/*
 * Once the session is stopped, it can't be restarted, no more calls or
 * notifications can be submitted or received.
 */
void
giorpc_peer_stop(
    GIoRpcPeer* peer);

/*
 * Synchronous RPC call, blocks until the response arrives. Preferably,
 * caller should own the GIoRpcPeer context, otherwise the call may fail
 * with GIORPC_ERROR_NOT_OWNER  if giorpc_peer_call_sync() fails to acquire
 * the context.
 *
 * Returns the result of the call, or  NULL on any failure, GError pointer
 * may be passed in to receive the exact reason for the failure. On success,
 * the caller must g_bytes_unref() the result.
 *
 * Implicitly starts the peer if it's not started yet.
 */
GBytes*
giorpc_peer_call_sync(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIORPC_SYNC_FLAGS flags,
    gint timeout_ms,
    GCancellable* cancel,
    GError** error);

/*
 * giorpc_peer_call() returns non-zero call id which can be passed to
 * giorpc_peer_cancel() or zero on failure. Implicitly starts the peer
 * if it's not started yet. The destroy callback (if provided) is always
 * called even if giorpc_peer_call() fails or the call was cancelled.
 */
guint
giorpc_peer_call(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* req_data,
    GIoRpcPeerResponseFunc response,
    void* user_data,
    GDestroyNotify destroy);

void
giorpc_peer_notify(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data);

/*
 * If the call is successfully cancelled, its response callback won't
 * be invoked, but destroy callback will be.
 *
 * giorpc_peer_cancel() fails and returns FALSE either because the input
 * parameters are invalid (e.g. invalid call id) or the call has already
 * completed (and the completion callback is or about to be invoked).
 *
 * To avoid race conditions with the completion callback, the caller
 * of giorpc_peer_cancel() must be the owner of GMainContext passed
 * to giorpc_peer_new().
 */
gboolean
giorpc_peer_cancel(
    GIoRpcPeer* peer,
    guint id);

gulong
giorpc_peer_add_state_handler(
    GIoRpcPeer* peer,
    GIoRpcPeerFunc fn,
    gpointer user_data);

gulong
giorpc_peer_add_request_handler(
    GIoRpcPeer* peer,
    guint iid,  /* GIORPC_IID_ANY for any */
    guint code, /* GIORPC_CODE_ANY for any */
    GIoRpcPeerRequestFunc fn,
    gpointer user_data);

gulong
giorpc_peer_add_notify_handler(
    GIoRpcPeer* peer,
    guint iid,  /* GIORPC_IID_ANY for any */
    guint code, /* GIORPC_CODE_ANY for any */
    GIoRpcPeerNotifyFunc fn,
    gpointer user_data);

gulong
giorpc_peer_add_cancel_handler(
    GIoRpcPeer* peer,
    GIoRpcPeerCancelFunc fn,
    gpointer user_data);

void
giorpc_peer_remove_handler(
    GIoRpcPeer* peer,
    gulong id);

void
giorpc_peer_remove_handlers(
    GIoRpcPeer* peer,
    gulong* ids,
    int count);

#define giorpc_peer_remove_all_handlers(peer, ids) \
    giorpc_peer_remove_handlers(peer, ids, G_N_ELEMENTS(ids))

G_END_DECLS

#endif /* GIORPC_PEER_H */

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
