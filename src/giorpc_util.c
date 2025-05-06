/*
 * Copyright (C) 2023-2025 Slava Monich <slava@monich.com>
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

#include "giorpc_error.h"
#include "giorpc_log_p.h"
#include "giorpc_util_p.h"
#include "giorpc_version.h"

#include <gutil_datapack.h>
#include <gutil_macros.h>

GLOG_MODULE_DEFINE("giorpc");

guint
giorpc_version()
{
    return GIORPC_VERSION;
}

GQuark
giorpc_error_quark()
{
    return g_quark_from_static_string("giorpc-error-domain");
}

/*
 * TLV array is encoded as follows:
 *
 * +---+---+---+---+---+---+---+---+
 * |  N (MBN)
 * +---
 * |  TLV (repeated N times)
 * +---
 */
gboolean
giorpc_tlv_array_decode(
    GUtilRange* in,
    const guint* tags, /* NULL terminated */
    GUtilData* vals)
{
    GUtilRange tmp = *in;
    guint64 n = 0;
    const guint* p;

    /* Clear the output values */
    for (p = tags; *p++; n++);
    memset(vals, 0, sizeof(vals[0]) * n);

    /* Decode the size of the array */
    if (gutil_unsigned_mbn_decode(&tmp, &n)) {
        guint64 i;

        /* Decode TLVs one by one */
        for (i = 0; i < n; i++) {
            GUtilData v;
            guint tag = gutil_tlv_decode(&tmp, &v);

            if (tag) {
                for (p = tags; *p; p++) {
                    if (*p == tag) {
                        GUtilData* out = vals + (p - tags);

                        if (out->bytes) {
                            /* Duplicate tag */
                            return FALSE;
                        }

                        /* Found a known tag */
                        *out = v;
                        break;
                    }
                }
                /* Unknown tags are ignored */
                continue;
            }
            /* Unable to parse a TLV, that's a failure */
            break;
        }

        if (i == n) {
            /* All TLVs have been successfully parsed */
            in->ptr = tmp.ptr;
            return TRUE;
        }
    }
    return FALSE;
}

/*
 * giorpc_read_all_async is a replacement for g_input_stream_read_all_async
 * which requires glib 2.44, except that it doesn't report the amount of
 * partially read data before EOF (we have no use for it).
 */

typedef struct giorpc_read_all_async {
    GInputStream* in;
    GCancellable* cancel;
    guchar* buffer;
    gsize count;
    gsize nbytes;
    GIoRpcReadAllFunc callback;
    gpointer user_data;
} GIoRpcReadAllAsync;

static
void
giorpc_read_all_async_callback(
    GObject* stream,
    GAsyncResult* result,
    gpointer user_data)
{
    GIoRpcReadAllAsync* read = user_data;
    GInputStream* in = G_INPUT_STREAM(stream);
    GError* error = NULL;
    const gssize nbytes = g_input_stream_read_finish(in, result, &error);

    if (error) {
        read->callback(in, 0, error, read->user_data);
        g_error_free(error);
    } else if (nbytes > 0) {
        read->nbytes += nbytes;
        if (read->nbytes == read->count) {
            /* The requested amount of data has arrived */
            read->callback(in, read->nbytes, NULL, read->user_data);
        } else {
            /* Read the next portion of data */
            GASSERT(read->nbytes < read->count);
            g_input_stream_read_async(read->in, read->buffer + read->nbytes,
                read->count - read->nbytes, G_PRIORITY_DEFAULT, read->cancel,
                giorpc_read_all_async_callback, read);
            return;
        }
    } else {
        /* Drop whatever has been read and report EOF */
        read->callback(in, 0, NULL, read->user_data);
    }

    /* Done with the read */
    g_object_unref(read->in);
    g_object_unref(read->cancel);
    gutil_slice_free(read);
}

void
giorpc_read_all_async(
    GInputStream* in,
    void* buffer,
    gsize count,
    GCancellable* cancel,
    GIoRpcReadAllFunc callback,
    gpointer user_data)
{
    GIoRpcReadAllAsync* read = g_slice_new(GIoRpcReadAllAsync);

    g_object_ref(read->in = in);
    g_object_ref(read->cancel = cancel);
    read->buffer = buffer;
    read->count = count;
    read->nbytes = 0;
    read->callback = callback;
    read->user_data = user_data;
    g_input_stream_read_async(in, buffer, count, G_PRIORITY_DEFAULT,
        cancel, giorpc_read_all_async_callback, read);
}

/*
 * giorpc_write_all_async is a replacement for g_output_stream_write_all_async
 * which requires glib 2.44, except that it doesn't report the amount of
 * partially written data (we have no use for it).
 */

typedef struct giorpc_write_all_async {
    GOutputStream* out;
    GCancellable* cancel;
    const guchar* buffer;
    gsize count;
    gsize nbytes;
    GIoRpcWriteAllFunc callback;
    gpointer user_data;
} GIoRpcWriteAllAsync;

static
void
giorpc_write_all_async_callback(
    GObject* stream,
    GAsyncResult* result,
    gpointer user_data)
{
    GIoRpcWriteAllAsync* write = user_data;
    GOutputStream* out = G_OUTPUT_STREAM(stream);
    GError* error = NULL;
    const gssize nbytes = g_output_stream_write_finish(out, result, &error);

    if (error) {
        write->callback(out, error, write->user_data);
        g_error_free(error);
    } else {
        GASSERT(nbytes >= 0);
        write->nbytes += nbytes;
        if (write->nbytes == write->count) {
            /* The whole thing has been written */
            write->callback(out, NULL, write->user_data);
        } else {
            /* Write the next portion of data */
            GASSERT(write->nbytes < write->count);
            g_output_stream_write_async(write->out,
                write->buffer + write->nbytes,
                write->count - write->nbytes,
                G_PRIORITY_DEFAULT, write->cancel,
                giorpc_write_all_async_callback, write);
            return;
        }
    }

    /* Done with the write */
    g_object_unref(write->out);
    g_object_unref(write->cancel);
    gutil_slice_free(write);
}

void
giorpc_write_all_async(
    GOutputStream* out,
    const void* buffer,
    gsize count,
    GCancellable* cancel,
    GIoRpcWriteAllFunc callback,
    gpointer user_data)
{
    GIoRpcWriteAllAsync* write = g_slice_new(GIoRpcWriteAllAsync);

    g_object_ref(write->out = out);
    g_object_ref(write->cancel = cancel);
    write->buffer = buffer;
    write->count = count;
    write->nbytes = 0;
    write->callback = callback;
    write->user_data = user_data;
    /* coverity[resource_leak:FALSE] */
    g_output_stream_write_async(out, buffer, count, G_PRIORITY_DEFAULT,
        cancel, giorpc_write_all_async_callback, write);
}

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
