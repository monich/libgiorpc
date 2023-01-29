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

#include "giorpc_request_p.h"

#include "giorpc_log_p.h"
#include "giorpc_peer_p.h"

#include <gutil_macros.h>
#include <gutil_misc.h>
#include <gutil_weakref.h>

struct giorpc_request {
    GUtilWeakRef* ref;
    gint refcount;
    gint done;
    guint id;
    gulong cancel_id;
    gboolean cancelled;
    gboolean have_cancel_handler;
    GIORPC_REQUEST_FLAGS flags;
};

typedef struct giorpc_request_closure {
    GCClosure cclosure;
    GIoRpcRequestCancelFunc handler;
    void* user_data;
    GIoRpcRequest* req;
} GIoRpcRequestClosure;

#define giorpc_request_closure_new() giorpc_closure_new(GIoRpcRequestClosure)

static
void
giorpc_request_free(
    GIoRpcRequest* self)
{
    /* Check if the response has been sent (and expected) */
    const gboolean send_error = (!self->done && !self->cancelled &&
        !(self->flags & GIORPC_REQUEST_FLAG_ONE_WAY));

    if (send_error || self->cancel_id) {
        GIoRpcPeer* peer = giorpc_peer_weakref_get(self->ref);

        if (peer) {
            if (send_error) {
                giorpc_peer_error(peer, self->id, GIORPC_PEER_ERROR_UNHANDLED);
            }
            if (self->have_cancel_handler) {
                giorpc_peer_disconnect_internal(peer, self->id);
            }
            giorpc_peer_remove_handler(peer, self->cancel_id);
            giorpc_peer_unref(peer);
        }
    }
    gutil_weakref_unref(self->ref);
    gutil_slice_free(self);
}

static
void
giorpc_request_cancelled(
    GIoRpcPeer* peer,
    guint id,
    gpointer req)
{
    GIoRpcRequest* self = req;

    if (self->id == id) {
        GIoRpcPeer* peer = giorpc_peer_weakref_get(self->ref);

        giorpc_request_ref(self);
        GDEBUG("Request %u has been cancelled", id);
        g_atomic_int_set(&self->done, TRUE);
        self->cancelled = TRUE;
        if (peer) {
            giorpc_peer_remove_handler(peer, self->cancel_id);
            self->cancel_id = 0;

            /*
             * Avoid unnecessary overhead if there are no cancel handlers
             * for this request.
             */
            if (self->have_cancel_handler) {
                giorpc_peer_emit_internal(peer, self->id);
                giorpc_peer_disconnect_internal(peer, self->id);
            }
            giorpc_peer_unref(peer);
        }
        giorpc_request_unref(self);
    }
}

static
void
giorpc_request_cancel_handler_cb(
    GObject* obj,
    GIoRpcRequestClosure* closure)
{
    closure->handler(closure->req, closure->user_data);
}

/*==========================================================================*
 * Internal API
 *==========================================================================*/

GIoRpcRequest*
giorpc_request_new(
    GUtilWeakRef* ref,
    guint id,
    GIORPC_REQUEST_FLAGS flags)
{
    GIoRpcRequest* self = g_slice_new0(GIoRpcRequest);

    self->id = id;
    self->flags = flags;
    self->ref = gutil_weakref_ref(ref);
    g_atomic_int_set(&self->refcount, 1);
    g_atomic_int_set(&self->done, FALSE);
    return self;
}

void
giorpc_request_delivered(
    GIoRpcRequest* self,
    GIoRpcPeer* peer)
{
    /*
     * This function is invoked exactly once, after "giorpc-peer-request" has
     * been emitted.
     *
     * If the caller's reference is not the last one and the request hasn't
     * been completed yet, then it may be put on hold by the handler - in
     * which case we start listening for CANCEL packets in an attempt to
     * avoid sending useless RESULT or ERROR packets for cancelled requests.
     */
    if (!self->done && self->refcount > 1) {
        /*
         * Since requests are typically completed by the handler before it
         * returns, we shouldn't get here too often (and certainly no more
         * than once per request).
         */
        GASSERT(!self->cancel_id);
        self->cancel_id = giorpc_peer_add_cancel_handler(peer,
            giorpc_request_cancelled, self);
    }
}

/*==========================================================================*
 * API
 *==========================================================================*/

GIoRpcRequest*
giorpc_request_ref(
    GIoRpcRequest* self)
{
    if (G_LIKELY(self)) {
        GASSERT(self->refcount > 0);
        g_atomic_int_inc(&self->refcount);
    }
    return self;
}

void
giorpc_request_unref(
    GIoRpcRequest* self)
{
    if (G_LIKELY(self)) {
        GASSERT(self->refcount > 0);
        if (g_atomic_int_dec_and_test(&self->refcount)) {
            giorpc_request_free(self);
        }
    }
}

GIORPC_REQUEST_FLAGS
giorpc_request_flags(
    GIoRpcRequest* self)
{
    return G_LIKELY(self) ? self->flags : GIORPC_REQUEST_FLAGS_NONE;
}

guint
giorpc_request_id(
    GIoRpcRequest* self)
{
    return G_LIKELY(self) ? self->id : 0;
}

gboolean
giorpc_request_is_cancelled(
    GIoRpcRequest* self)
{
    return G_LIKELY(self) && self->cancelled;
}

gulong
giorpc_request_add_cancel_handler(
    GIoRpcRequest* self,
    GIoRpcRequestCancelFunc handler,
    gpointer user_data)
{
    if (G_LIKELY(self) && G_LIKELY(handler)) {
        GIoRpcPeer* peer = giorpc_peer_weakref_get(self->ref);

        if (peer) {
            GIoRpcRequestClosure* closure = giorpc_request_closure_new();
            GCClosure* cc = &closure->cclosure;
            gulong id;

            cc->closure.data = closure;
            cc->callback = giorpc_request_cancel_handler_cb;
            closure->handler = handler;
            closure->user_data = user_data;
            closure->req = self;
            id = giorpc_peer_connect_internal(peer, self->id, &cc->closure);
            if (id) {
                /*
                 * Set the one-way flag telling us that at least one
                 * cancel handler has been successfully registered for
                 * this request. This flags doesn't get cleared when the
                 * handler is unregistered and that's fine because this is
                 * strictly an optimization measure.
                 */
                self->have_cancel_handler = TRUE;
            }
            giorpc_peer_unref(peer);
            return id;
        }
    }
    return 0;
}

void
giorpc_request_remove_cancel_handler(
    GIoRpcRequest* self,
    gulong id)
{
    if (G_LIKELY(self) && G_LIKELY(id)) {
        GIoRpcPeer* peer = giorpc_peer_weakref_get(self->ref);

        if (peer) {
            giorpc_peer_remove_handler(peer, id);
            giorpc_peer_unref(peer);
        }
    }
}

void
giorpc_request_drop(
    GIoRpcRequest* self)
{
    /*
     * Unreference the request without sending any reply. That's the
     * service should do when (and if) it receives "giorpc-peer-cancel"
     * signal for a request that was put on hold.
     */
    if (G_LIKELY(self)) {
        g_atomic_int_set(&self->done, TRUE);
        giorpc_request_unref(self);
    }
}

gboolean
giorpc_request_complete(
    GIoRpcRequest* self,
    GBytes* data)
{
    gboolean ok = FALSE;

    if (G_LIKELY(self)) {
        if (g_atomic_int_compare_and_exchange(&self->done, FALSE, TRUE)) {
            GIoRpcPeer* peer = giorpc_peer_weakref_get(self->ref);

            if (peer) {
                ok = TRUE;
                if (!(self->flags & GIORPC_REQUEST_FLAG_ONE_WAY)) {
                    giorpc_peer_result(peer, self->id, (self->flags &
                        GIORPC_REQUEST_FLAG_NO_RESULT_DATA) ? NULL : data);
                }
                giorpc_peer_unref(peer);
            }
        }
    }
    return ok;
}

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
