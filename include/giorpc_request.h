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

#ifndef GIORPC_REQUEST_H
#define GIORPC_REQUEST_H

#include "giorpc_types.h"

G_BEGIN_DECLS

/*
 * For immediate completion, just call giorpc_request_complete() from
 * the response callback.
 *
 * For deferred completion, the response callback may grab a reference
 * with giorpc_request_ref(), then call giorpc_request_complete() later
 * and drop the reference with giorpc_request_unref().
 *
 * Each request can be completed only once. Although it's won't break
 * anything if giorpc_request_complete() is invoked several times.
 *
 * If GIORPC_REQUEST_FLAG_ONE_WAY or GIORPC_REQUEST_FLAG_NO_RESULT_DATA
 * bit is set in the flags returned by giorpc_request_flags(), the bytes
 * argument will be ignored by giorpc_request_complete() and won't be
 * actually sent back to the client even if you pass it in. That may
 * provide some room for optimisation.
 */

typedef enum giorpc_request_flags {
    GIORPC_REQUEST_FLAGS_NONE = 0,
    GIORPC_REQUEST_FLAG_ONE_WAY = 0x01,
    GIORPC_REQUEST_FLAG_NO_RESULT_DATA = 0x02
} GIORPC_REQUEST_FLAGS;

typedef
void
(*GIoRpcRequestCancelFunc)(
    GIoRpcRequest* req,
    gpointer user_data);

GIoRpcRequest*
giorpc_request_ref(
    GIoRpcRequest* req);

void
giorpc_request_unref(
    GIoRpcRequest* req);

GIORPC_REQUEST_FLAGS
giorpc_request_flags(
    GIoRpcRequest* req);

guint
giorpc_request_id(
    GIoRpcRequest* req);

gboolean
giorpc_request_is_cancelled(
    GIoRpcRequest* req);

gulong
giorpc_request_add_cancel_handler(
    GIoRpcRequest* req,
    GIoRpcRequestCancelFunc fn,
    gpointer user_data);

void
giorpc_request_remove_cancel_handler(
    GIoRpcRequest* req,
    gulong id);

void
giorpc_request_drop(
    GIoRpcRequest* req);

gboolean
giorpc_request_complete(
    GIoRpcRequest* req,
    GBytes* bytes);

#define giorpc_request_need_data(req) (!(giorpc_request_flags(req) & \
    (GIORPC_REQUEST_FLAG_ONE_WAY | GIORPC_REQUEST_FLAG_NO_RESULT_DATA)))

G_END_DECLS

#endif /* GIORPC_REQUEST_H */

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
