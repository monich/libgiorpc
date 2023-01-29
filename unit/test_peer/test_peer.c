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

#define _GNU_SOURCE /* memmem */

#include "test_common.h"
#include "test_io_stream.h"

#include "giorpc.h"
#include "giorpc_peer_p.h"
#include "giorpc_request.h"
#include "giorpc_types_p.h"

#include <gutil_log.h>
#include <gutil_misc.h>

#include <gio/gunixinputstream.h>
#include <gio/gunixoutputstream.h>

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>

static TestOpt test_opt;

#define TEST_(name) "/giorpc/peer/" name

#define TEST_DUMP(data,len) gutil_log_dump(GLOG_MODULE_CURRENT, \
    GLOG_LEVEL_DEBUG, "     ", data, len)

#define PACKET_START 0x01
#define PACKET_FINISH 0x02
#define PACKET_NOTIFY 0x03
#define PACKET_REQUEST 0x04
#define PACKET_RESULT 0x05
#define PACKET_ERROR 0x06
#define PACKET_CANCEL 0x07

#define GIORPC_START_VERSION_TAG 1
#define GIORPC_START_VERSION_TLV_(version) \
    GIORPC_START_VERSION_TAG, 1, version
#define GIORPC_START_VERSION_TLV() \
    GIORPC_START_VERSION_TLV_(GIORPC_PROTOCOL_VERSION)
#define START_PACKET_(version) \
    PACKET_START, 4, 1, GIORPC_START_VERSION_TLV_(version)
#define START_PACKET() \
    START_PACKET_(GIORPC_PROTOCOL_VERSION)

/* Reusable ids */
#define TEST_IID 22
#define TEST_CALL_CODE 128

/* Disable timeouts when debugging */
#define SYNC_TIMEOUT_MS \
    ((test_opt.flags & TEST_FLAG_DEBUG) ? 0 :  TEST_TIMEOUT_MS)

/*==========================================================================*
 * Test thread
 *==========================================================================*/

typedef struct test_thread {
    GThread* thread;
    GMutex mutex;
    GCond start_cond;
    GMainLoop* loop;
    GMainContext* context;
} TestThread;

static
gpointer
test_thread_proc(
    gpointer user_data)
{
    TestThread* thread = user_data;
    GMainLoop* loop = g_main_loop_ref(thread->loop);
    GMainContext* context = g_main_context_ref(thread->context);

    /* Lock */
    g_mutex_lock(&thread->mutex);
    g_main_context_push_thread_default(context);
    g_cond_broadcast(&thread->start_cond);
    GDEBUG("Test thread started");
    g_mutex_unlock(&thread->mutex);
    /* Unlock */

    g_main_loop_run(loop);
    g_main_loop_unref(loop);
    g_main_context_pop_thread_default(context);
    g_main_context_unref(context);
    GDEBUG("Test thread exiting");
    return NULL;
}

static
TestThread*
test_thread_new()
{
    TestThread* thread = g_new0(TestThread, 1);

    g_cond_init(&thread->start_cond);
    g_mutex_init(&thread->mutex);
    thread->context = g_main_context_new();
    thread->loop = g_main_loop_new(thread->context, FALSE);

    /* Lock */
    g_mutex_lock(&thread->mutex);
    thread->thread = g_thread_new("test", test_thread_proc, thread);
    g_assert(g_cond_wait_until(&thread->start_cond, &thread->mutex,
        g_get_monotonic_time() + TEST_TIMEOUT_SEC * G_TIME_SPAN_SECOND));
    g_mutex_unlock(&thread->mutex);
    /* Unlock */

    return thread;
}

static
void
test_thread_invoke_later(
    TestThread* thread,
    GSourceFunc fn,
    gpointer user_data)
{
    GSource* src = g_idle_source_new();

    g_source_set_priority(src, G_PRIORITY_DEFAULT);
    g_source_set_callback(src, fn, user_data, NULL);
    g_source_attach(src, thread->context);
    g_source_unref(src);
}

static
void
test_thread_free(
    TestThread* thread)
{
    g_main_loop_quit(thread->loop);
    g_thread_join(thread->thread);
    g_main_loop_unref(thread->loop);
    g_main_context_unref(thread->context);
    g_cond_clear(&thread->start_cond);
    g_mutex_clear(&thread->mutex);
    g_free(thread);
}

/*==========================================================================*
 * Test reader
 *==========================================================================*/

typedef struct test_reader {
    GCancellable* cancel;
    GInputStream* in;
    GByteArray* buf;
    gsize bufsize;
    void* buffer;
    guint buf_offset;
    GBytes* wait_data;
    GMainLoop* loop;
} TestReader;

static
void
test_reader_cb(
    GObject* stream,
    GAsyncResult* result,
    gpointer user_data)
{
    TestReader* reader = user_data;
    GInputStream* in = G_INPUT_STREAM(stream);
    GError* error = NULL;
    gssize nbytes = g_input_stream_read_finish(in, result, &error);

    g_assert(reader->in == in);
    if (nbytes > 0) {
        GByteArray* buf = reader->buf;

        GDEBUG("Received %d bytes:", (int) nbytes);
        TEST_DUMP(reader->buffer, nbytes);
        if (buf) {
            g_byte_array_append(buf, reader->buffer, nbytes);
            if (reader->wait_data && reader->loop) {
                gsize len;
                gconstpointer wait = g_bytes_get_data(reader->wait_data, &len);

                if (memmem(buf->data, buf->len, wait, len)) {
                    GDEBUG("Found the wait pattern!");
                    g_byte_array_remove_range(buf, 0, len);
                    test_quit_later(reader->loop);
                    g_main_loop_unref(reader->loop);
                    reader->loop = NULL;
                }
            }
        }
        g_input_stream_read_async(in, reader->buffer, reader->bufsize,
            G_PRIORITY_DEFAULT, reader->cancel, test_reader_cb, reader);
    } else {
        if (error) {
            GDEBUG("%s", GERRMSG(error));
            g_error_free(error);
        } else {
            GDEBUG("Test reader done");
        }
        g_byte_array_unref(reader->buf);
        if (reader->wait_data) {
            g_bytes_unref(reader->wait_data);
            if (reader->loop) {
                g_main_loop_unref(reader->loop);
            }
        }
        g_object_unref(reader->in);
        g_free(reader->buffer);
        g_free(reader);
    }
}

static
GCancellable*
test_reader(
    int fd,
    GByteArray* buf,
    GBytes* wait_data,
    GMainLoop* loop)
{
    TestReader* reader = g_new0(TestReader, 1);

    reader->cancel = g_cancellable_new();
    reader->in = g_unix_input_stream_new(fd, FALSE);
    reader->bufsize = 1024;
    reader->buffer = g_malloc(reader->bufsize);
    reader->buf = (buf ? g_byte_array_ref(buf) : g_byte_array_new());
    reader->buf_offset = reader->buf->len;
    if (wait_data) {
        reader->wait_data = g_bytes_ref(wait_data);
        if (loop) {
            reader->loop = g_main_loop_ref(loop);
        }
    }

    g_input_stream_read_async(reader->in, reader->buffer, reader->bufsize,
        G_PRIORITY_DEFAULT, reader->cancel, test_reader_cb, reader);
    return reader->cancel;
}

static
GCancellable*
test_null_reader(
    int fd)
{
    return test_reader(fd, NULL, NULL, NULL);
}

/*==========================================================================*
 * Single peer setup
 *==========================================================================*/

typedef struct test_setup1 {
    GIoRpcPeer* peer;
    GMainLoop* loop;
    int fd;
} TestSetup1;

static
void
test_setup1_init(
    TestSetup1* test)
{
    int fd[2];
    GIOStream* stream;

    g_assert(!socketpair(AF_UNIX, SOCK_STREAM, 0, fd));

    test->loop = g_main_loop_new(NULL, FALSE);
    test->fd = fd[1];

    stream = test_io_stream_new_fd(fd[0], TRUE);
    g_assert((test->peer = giorpc_peer_new(stream, NULL)));

    /* GIoRpcPeer object keeps references to the stream */
    g_object_unref(stream);
}

static
void
test_setup1_cleanup(
    TestSetup1* test)
{
    giorpc_peer_unref(test->peer);
    g_main_loop_unref(test->loop);
    g_assert(!close(test->fd));
}

/*==========================================================================*
 * Two-peer setup
 *==========================================================================*/

typedef struct test_setup2 {
    GIoRpcPeer* peer[2];
    GMainLoop* loop;
} TestSetup2;

static
void
test_setup2_init2(
    TestSetup2* test,
    GMainContext* ctx0,
    GMainContext* ctx1)
{
    int fd[2];
    GIOStream* stream[2];

    g_assert(!socketpair(AF_UNIX, SOCK_STREAM, 0, fd));

    test->loop = g_main_loop_new(NULL, FALSE);

    /* Input streams will close the sockets */
    stream[0] = test_io_stream_new_fd(fd[0], TRUE);
    stream[1] = test_io_stream_new_fd(fd[1], TRUE);

    g_assert((test->peer[0] = giorpc_peer_new(stream[0], ctx0)));
    g_assert((test->peer[1] = giorpc_peer_new(stream[1], ctx1)));

    /* GIoRpcPeer object keeps references to the stream */
    g_object_unref(stream[0]);
    g_object_unref(stream[1]);
}

static
void
test_setup2_init(
    TestSetup2* test,
    GMainContext* ctx)
{
    test_setup2_init2(test, ctx, ctx);
}

static
void
test_setup2_cleanup(
    TestSetup2* test)
{
    giorpc_peer_unref(test->peer[0]);
    giorpc_peer_unref(test->peer[1]);
    g_main_loop_unref(test->loop);
}

/*==========================================================================*
 * Common callbacks
 *==========================================================================*/

static
void
test_notify_unreached(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    gpointer user_data)
{
    g_assert_not_reached();
}

static
void
test_request_unreached(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    GIoRpcRequest* req,
    gpointer user_data)
{
    g_assert_not_reached();
}

static
void
test_response_unreached(
    GIoRpcPeer* peer,
    GBytes* response,
    const GError* error,
    gpointer user_data)
{
    g_assert_not_reached();
}

static
void
test_exit_loop_when_started(
    GIoRpcPeer* peer,
    gpointer loop)
{
    if (peer->state == GIORPC_PEER_STATE_STARTED) {
        GDEBUG("Started");
        test_quit_later(loop);
    }
}

static
void
test_exit_loop_when_done(
    GIoRpcPeer* peer,
    gpointer loop)
{
    if (peer->state == GIORPC_PEER_STATE_DONE) {
        GDEBUG("Done");
        test_quit_later(loop);
    }
}

static
void
test_exit_loop(
    gpointer loop)
{
    GDEBUG("Finished");
    test_quit_later(loop);
}

static
void
test_no_response(
    GIoRpcPeer* peer,
    GBytes* response,
    const GError* error,
    gpointer user_data)
{
    g_assert_not_reached();
}

static
void
test_inc_destroy(
    gpointer user_data)
{
    (*(int*)user_data)++;
}

static
void
test_no_destroy(
    gpointer user_data)
{
    g_assert_not_reached();
}

/*==========================================================================*
 * null
 *==========================================================================*/

static
void
test_null()
{
    GError* error = NULL;
    int n = 0;

    g_assert(!giorpc_peer_new(NULL, NULL));
    g_assert(!giorpc_peer_add_state_handler(NULL, NULL, NULL));
    g_assert(!giorpc_peer_add_request_handler(NULL, 0, 0, NULL, NULL));
    g_assert(!giorpc_peer_add_notify_handler(NULL, 0, 0, NULL, NULL));
    g_assert(!giorpc_peer_add_cancel_handler(NULL, NULL, NULL));
    g_assert(!giorpc_peer_ref(NULL));
    g_assert(!giorpc_peer_call(NULL, 0, 0, NULL, NULL, NULL, NULL));
    g_assert(!giorpc_peer_cancel(NULL, 0));
    g_assert(!giorpc_peer_call_sync(NULL, 0, 0, NULL, GIORPC_SYNC_FLAGS_NONE,
        GIORPC_SYNC_TIMEOUT_INFINITE, NULL, NULL));
    g_assert(!giorpc_peer_call_sync(NULL, 0, 0, NULL, GIORPC_SYNC_FLAGS_NONE,
        GIORPC_SYNC_TIMEOUT_INFINITE, NULL, &error));
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_INVALID_ARGS));
    g_clear_error(&error);

    giorpc_peer_notify(NULL, 0, 0, NULL);
    giorpc_peer_result(NULL, 0, NULL);
    giorpc_peer_error(NULL, 0, GIORPC_PEER_ERROR_UNSPECIFIED);
    giorpc_peer_remove_handler(NULL, 0);
    giorpc_peer_remove_handler(NULL, 1); /* id is checked first */
    giorpc_peer_remove_handlers(NULL, NULL, 0);
    giorpc_peer_unref(NULL);
    giorpc_peer_start(NULL);
    giorpc_peer_stop(NULL);

    g_assert(!giorpc_request_ref(NULL));
    g_assert(!giorpc_request_complete(NULL, NULL));
    g_assert(!giorpc_request_is_cancelled(NULL));
    g_assert(!giorpc_request_add_cancel_handler(NULL, NULL, NULL));
    g_assert_cmpuint(giorpc_request_id(NULL), == ,0);
    g_assert_cmpint(giorpc_request_flags(NULL), == ,GIORPC_REQUEST_FLAGS_NONE);
    giorpc_request_remove_cancel_handler(NULL, 0);
    giorpc_request_drop(NULL);
    giorpc_request_unref(NULL);

    /* giorpc_peer_call() withough an object still calls the destroy callback */
    g_assert(!giorpc_peer_call(NULL, 0, 0, NULL, NULL, &n, test_inc_destroy));
    g_assert_cmpint(n, == ,1);
}

/*==========================================================================*
 * basic/1
 *==========================================================================*/

static
void
test_basic1()
{
    int fd[2];
    GIOStream* stream;
    GInputStream* in;
    GOutputStream* out;
    GIoRpcPeer* peer;
    GMainLoop* loop;

    /* Let the peer talk to itself :) */
    g_assert(!socketpair(AF_UNIX, SOCK_STREAM, 0, fd));
    in = g_unix_input_stream_new(fd[0], FALSE);
    out = g_unix_output_stream_new(fd[1], FALSE);
    stream = test_io_stream_new(in, out);
    g_assert((peer = giorpc_peer_new(stream, g_main_context_default())));
    g_assert_cmpint(peer->state, == ,GIORPC_PEER_STATE_INIT);
    g_object_unref(in);
    g_object_unref(out);
    g_object_unref(stream);

    /* Request codes must be in the range [1..0x1fffff] */
    g_assert(!giorpc_peer_call(peer, 0, 0, NULL, NULL, NULL, NULL));
    g_assert(!giorpc_peer_call(peer, 1, 0x200000, NULL, NULL, NULL, NULL));
    g_assert(!giorpc_peer_add_request_handler(peer, GIORPC_MIN_IID, 0x200000,
        test_request_unreached, NULL));

    /* No calls => nothing to cancel */
    g_assert(!giorpc_peer_cancel(peer, 0));
    g_assert(!giorpc_peer_cancel(peer, 1));

    /* These have no effect */
    g_assert(!giorpc_peer_add_state_handler(peer, NULL, NULL));
    g_assert(!giorpc_peer_add_notify_handler(peer, GIORPC_MIN_IID, UINT_MAX,
        test_notify_unreached, NULL)); /* Code too large */
    g_assert(!giorpc_peer_add_notify_handler(peer, GIORPC_MIN_IID,
        GIORPC_CODE_ANY,  NULL, NULL)); /* Callback is required */
    g_assert(!giorpc_peer_add_request_handler(peer, GIORPC_MIN_IID, UINT_MAX,
        test_request_unreached, NULL)); /* Code too large */
    g_assert(!giorpc_peer_add_request_handler(peer, GIORPC_MIN_IID,
        GIORPC_CODE_ANY, NULL, NULL)); /* Callback is required */
    giorpc_peer_remove_handler(peer, 0);

    /* Ref and unref */
    g_assert(giorpc_peer_ref(peer) == peer);
    giorpc_peer_unref(peer);

    /* Request start and immediately unref */
    giorpc_peer_start(peer);
    giorpc_peer_unref(peer);

    /* One loop is enough for giorpc_peer_invoke_proc() to get invoked */
    loop = g_main_loop_new(NULL, FALSE);
    test_quit_later(loop);
    test_run(&test_opt, loop);

    g_main_loop_unref(loop);
    g_assert(!close(fd[0]));
    g_assert(!close(fd[1]));
}

/*==========================================================================*
 * basic/2
 *==========================================================================*/

typedef struct test_basic {
    int count;
    GMainLoop* loop;
} TestBasic;

static
void
test_basic2_start_cb(
    GIoRpcPeer* peer,
    void* user_data)
{
    TestBasic* test = user_data;

    g_assert(test->count < 2);
    if (peer->state == GIORPC_PEER_STATE_STARTED) {
        test->count++;
        GDEBUG("Peer #%d has started", test->count);
        g_assert_cmpuint(peer->version, == ,GIORPC_PROTOCOL_VERSION);
        if (test->count == 2) {
            GDEBUG("Both peers are started");
            g_main_loop_quit(test->loop);
        }
    }
}

static
void
test_basic2_done_cb(
    GIoRpcPeer* peer,
    void* user_data)
{
    TestBasic* test = user_data;

    g_assert(test->count < 2);
    if (peer->state == GIORPC_PEER_STATE_DONE) {
        test->count++;
        GDEBUG("Peer #%d is done", test->count);
        if (test->count == 2) {
            GDEBUG("Done");
            test_quit_later(test->loop);
        }
    }
}

static
void
test_basic2_done_unref_cb(
    GIoRpcPeer* peer,
    void* user_data)
{
    test_basic2_done_cb(peer, user_data);
    if (peer->state == GIORPC_PEER_STATE_DONE) {
        giorpc_peer_unref(peer);
    }
}

static
void
test_basic2()
{
    gulong id[2];
    TestSetup2 setup;
    TestBasic test;
    GIoRpcPeer* peer0;
    GIoRpcPeer* peer1;

    test_setup2_init(&setup, NULL);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    peer0 = setup.peer[0];
    peer1 = setup.peer[1];

    id[0] = giorpc_peer_add_state_handler(peer0, test_basic2_start_cb, &test);
    id[1] = giorpc_peer_add_state_handler(peer1, test_basic2_start_cb, &test);
    g_assert(id[0]);
    g_assert(id[1]);

    giorpc_peer_start(peer0);
    giorpc_peer_start(peer1);
    test_run(&test_opt, test.loop);
    g_assert_cmpint(test.count, == ,2);

    /* Stop both peers */
    giorpc_peer_remove_handler(peer0, id[0]);
    giorpc_peer_remove_handler(peer1, id[1]);
    id[0] = giorpc_peer_add_state_handler(peer0, test_basic2_done_cb, &test);
    id[1] = giorpc_peer_add_state_handler(peer1, test_basic2_done_unref_cb,
        &test);
    g_assert(id[0]);
    g_assert(id[1]);

    GDEBUG("Stopping...");
    test.count = 0;
    giorpc_peer_stop(peer0);
    giorpc_peer_stop(peer1);

    test_run(&test_opt, test.loop);
    g_assert_cmpint(test.count, == ,2);

    /* peer1 is unref'd by test_basic2_done_unref_cb */
    giorpc_peer_remove_handler(peer0, id[0]);
    setup.peer[1] = NULL;
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * basic/3
 *==========================================================================*/

static
void
test_basic3()
{
    gulong id;
    TestSetup1 test;
    GCancellable* cancel;

    test_setup1_init(&test);

    /* Setup the reader */
    cancel = test_null_reader(test.fd);

    id = giorpc_peer_add_state_handler(test.peer, test_exit_loop_when_done,
        test.loop);
    g_assert(id);

    /* Start and immediately stop */
    giorpc_peer_start(test.peer);
    giorpc_peer_stop(test.peer);
    test_run(&test_opt, test.loop);

    /* It's too late to submit calls */
    g_assert(!giorpc_peer_call(test.peer, 1, 1, NULL, test_no_response,
        test_no_destroy, NULL));

    giorpc_peer_remove_handler(test.peer, id);
    g_cancellable_cancel(cancel);
    g_object_unref(cancel);
    test_setup1_cleanup(&test);
}

/*==========================================================================*
 * stop
 *==========================================================================*/

static
void
test_stop_cb(
    GIoRpcPeer* peer,
    gpointer user_data)
{
    gboolean* stopped = user_data;

    g_assert_cmpint(peer->state, == ,GIORPC_PEER_STATE_DONE);
    g_assert(!*stopped);
    *stopped = TRUE;
}



static
void
test_stop()
{
    gulong id;
    TestSetup1 test;
    gboolean stopped = FALSE;

    test_setup1_init(&test);

    id = giorpc_peer_add_state_handler(test.peer, test_stop_cb, &stopped);
    g_assert(id);

    /* Stop before even starting */
    giorpc_peer_stop(test.peer);
    g_assert(stopped);

    /* Subsequent stops and starts do nothing */
    giorpc_peer_start(test.peer);
    giorpc_peer_stop(test.peer);

    giorpc_peer_remove_handler(test.peer, id);
    test_setup1_cleanup(&test);
}

/*==========================================================================*
 * shutdown
 *==========================================================================*/

static
void
test_shutdown()
{
    gulong id;
    TestSetup1 test;

    test_setup1_init(&test);

    id = giorpc_peer_add_state_handler(test.peer, test_exit_loop_when_done,
        test.loop);
    g_assert(id);

    /* Shutdown the socket and expect the peer to shut down too */
    giorpc_peer_start(test.peer);
    giorpc_peer_start(test.peer); /* Second start does nothing */
    g_assert(!shutdown(test.fd, SHUT_RDWR));
    test_run(&test_opt, test.loop);

    giorpc_peer_remove_handler(test.peer, id);
    test_setup1_cleanup(&test);
}

/*==========================================================================*
 * notify/main
 * notify/thread
 *==========================================================================*/

#define TEST_NOTIFY_CODE1 42
#define TEST_NOTIFY_CODE2 43

typedef struct test_notify {
    GBytes* notify_data;
    GThread* notify_thread;
    GMainContext* notify_context;
    GMainLoop* loop;
    guint cb0_code;
} TestNotify;

static
void
test_notify_cb0(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestNotify* test = user_data;

    g_assert(data);
    GDEBUG("Received notification %u (wildcard), %u bytes", code, (guint)
        g_bytes_get_size(data));
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_NOTIFY_CODE1);
    g_assert(g_bytes_equal(test->notify_data, data));
    g_assert(g_main_context_is_owner(test->notify_context));
    g_assert(test->notify_thread == g_thread_self());
    g_assert(!test->cb0_code);
    test->cb0_code = code;
}

static
void
test_notify_cb1(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestNotify* test = user_data;

    g_assert(data);
    GDEBUG("Received notification %u, %u bytes", code, (guint)
        g_bytes_get_size(data));
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_NOTIFY_CODE1);
    g_assert(g_bytes_equal(test->notify_data, data));
    g_assert(g_main_context_is_owner(test->notify_context));
    g_assert(test->notify_thread == g_thread_self());
    test_quit_later(test->loop);
}

static
void
test_notify_cb2(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestNotify* test = user_data;

    g_assert(data);
    GDEBUG("Received notification %u, %u bytes", code, (guint)
        g_bytes_get_size(data));
    g_assert_cmpuint(code, == ,TEST_NOTIFY_CODE2);
    g_assert_cmpuint(g_bytes_get_size(data), == ,0);
    g_assert(g_main_context_is_owner(test->notify_context));
    g_assert(test->notify_thread == g_thread_self());
    test_quit_later(test->loop);
}

static
void
test_notify_with_context(
    GThread* thread,
    GMainContext* context)
{
    static const guint8 notify_data[] = { 1, 2, 3 };
    TestSetup2 setup;
    TestNotify test;
    GIoRpcPeer* callee;
    GIoRpcPeer* caller;
    gulong id[3];

    test_setup2_init(&setup, context);
    callee = setup.peer[0];
    caller = setup.peer[1];

    memset(&test, 0, sizeof(test));
    test.notify_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(notify_data));
    test.notify_thread = thread;
    test.notify_context = context;
    test.loop = setup.loop;
    g_assert((id[0] = giorpc_peer_add_notify_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_notify_cb0, &test)));
    g_assert((id[1] = giorpc_peer_add_notify_handler(callee, TEST_IID,
        TEST_NOTIFY_CODE1, test_notify_cb1, &test)));
    g_assert((id[2] = giorpc_peer_add_notify_handler(callee, TEST_IID,
        TEST_NOTIFY_CODE2, test_notify_unreached, NULL)));

    /* Send notification from peer 1 to peer 0, this one has data */
    giorpc_peer_start(callee);
    giorpc_peer_notify(caller, TEST_IID, TEST_NOTIFY_CODE1, test.notify_data);
    test_run(&test_opt, setup.loop);

    GDEBUG("First notification has been received");
    g_assert_cmpuint(test.cb0_code, == ,TEST_NOTIFY_CODE1);
    giorpc_peer_remove_all_handlers(callee, id);
    g_assert((id[0] = giorpc_peer_add_notify_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_notify_cb2, &test)));

    /* Another notification, this time without data */
    giorpc_peer_notify(caller, TEST_IID, TEST_NOTIFY_CODE2, NULL);
    test_run(&test_opt, setup.loop);

    GDEBUG("Second notification has been received");
    /* Only id[0] is non-zero at this point */
    giorpc_peer_remove_all_handlers(callee, id);
    test_setup2_cleanup(&setup);
    g_bytes_unref(test.notify_data);
}

static
void
test_notify_main()
{
    test_notify_with_context(g_thread_self(), g_main_context_default());
}

static
void
test_notify_thread()
{
    TestThread* thread = test_thread_new();

    test_notify_with_context(thread->thread, thread->context);
    test_thread_free(thread);
}

/*==========================================================================*
 * unhandled/1
 *==========================================================================*/

typedef struct test_unhandled1 {
    gboolean completed;
    GMainLoop* loop;
} TestUnhandled1;

static
void
test_unhandled1_cancelled(
    gpointer user_data)
{
    gboolean* cancelled = user_data;

    g_assert(!*cancelled);
    *cancelled = TRUE;
}

static
void
test_unhandled1_done(
    GIoRpcPeer* peer,
    GBytes* response,
    const GError* error,
    gpointer user_data)
{
    TestUnhandled1* test = user_data;

    GDEBUG("Done");
    g_assert(!response);
    g_assert(!test->completed);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_UNHANDLED));
    test->completed = TRUE;
    g_main_loop_quit(test->loop);
}

static
void
test_unhandled1()
{
    TestSetup2 setup;
    TestUnhandled1 test;
    GIoRpcPeer* caller;
    gboolean cancelled = FALSE;
    guint id;

    test_setup2_init(&setup, NULL);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    caller = setup.peer[0];

    /* Start the callee (caller will implcitly get started) */
    giorpc_peer_start(setup.peer[1]);

    /* Submit and immediately cancel one call */
    id = giorpc_peer_call(caller, TEST_IID, 1, NULL, test_response_unreached,
        &cancelled, test_unhandled1_cancelled);
    g_assert(id);
    g_assert(!cancelled);
    g_assert(giorpc_peer_cancel(caller, id));
    g_assert(cancelled);
    g_assert(!giorpc_peer_cancel(caller, id)); /* Already cancelled */

    /* Submit another call and wait for it to be rejected */
    id = giorpc_peer_call(caller, TEST_IID, 1, NULL, test_unhandled1_done,
        &test, NULL);
    g_assert(id);

    test_run(&test_opt, test.loop);

    g_assert(test.completed);
    g_assert(!giorpc_peer_cancel(caller, id)); /* Already completed */

    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * unhandled/2
 * unhandled/3
 *==========================================================================*/

typedef struct test_unhandled {
    GMainLoop* loop;
    guint destroyed;
    GIoRpcRequest* req;
} TestUnhandled;

static
void
test_unhandled_request(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestUnhandled* test = user_data;

    GDEBUG("Test done");
    g_assert_cmpuint(code, == ,TEST_CALL_CODE);
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    /* Reference the request to prevent it from being completed */
    g_assert(!test->req);
    test->req = giorpc_request_ref(req);

    g_main_loop_quit(test->loop);
}

void
test_unhandled_destroy(
    gpointer user_data)
{
    TestUnhandled* test = user_data;

    test->destroyed++;
}

static
GIoRpcRequest*
test_unhandled_req()
{
    TestSetup2 setup;
    TestUnhandled test;
    GIoRpcPeer* caller;
    GIoRpcPeer* callee;
    gulong id;

    test_setup2_init(&setup, NULL);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    caller = setup.peer[0];
    callee = setup.peer[1];

    /* Submit a call and wait for the handler to be invoked */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_unhandled_request, &test)));
    giorpc_peer_start(callee);
    g_assert_cmpuint(giorpc_peer_call(caller, TEST_IID, TEST_CALL_CODE, NULL,
        test_response_unreached, &test, test_unhandled_destroy), != ,0);

    test_run(&test_opt, test.loop);

    g_assert(!test.destroyed);
    g_assert(test.req);
    giorpc_peer_remove_handler(callee, id);
    test_setup2_cleanup(&setup);

    /* The request must be cancelled by now */
    g_assert(test.destroyed);

    /* Return the ignored request */
    return test.req;
}

static
void
test_unhandled2()
{
    GIoRpcRequest* req = test_unhandled_req();

    /* Unref the ignored request after the peer is gone */
    giorpc_request_unref(req);
}

static
void
test_unhandled3()
{
    GIoRpcRequest* req = test_unhandled_req();

    /* Try to complete the ignored request after the peer is gone (and fail) */
    g_assert(!giorpc_request_complete(req, NULL));
    giorpc_request_unref(req);
}

/*==========================================================================*
 * unhandled/4
 *==========================================================================*/

void
test_unhandled4_done(
    gpointer loop)
{
    GDEBUG("Done");
    g_main_loop_quit(loop);
}

static
void
test_unhandled4()
{
    TestSetup2 setup;

    test_setup2_init(&setup, NULL);

    /* Submit a call and wait for it to be rejected */
    giorpc_peer_start(setup.peer[1]);
    g_assert_cmpuint(giorpc_peer_call(setup.peer[0], TEST_IID, TEST_CALL_CODE,
        NULL, NULL, setup.loop, test_unhandled4_done), != ,0);

    test_run(&test_opt, setup.loop);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * unhandled/5
 *==========================================================================*/

typedef struct test_unhandled5 {
    GMainLoop* loop;
    GIoRpcRequest* req;
} TestUnhandled5;

static
void
test_unhandled5_request(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestUnhandled5* test = user_data;

    GDEBUG("Test done");
    g_assert_cmpuint(code, == ,TEST_CALL_CODE);
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    /* Reference the request to prevent it from being completed */
    g_assert(!test->req);
    test->req = giorpc_request_ref(req);

    test_quit_later(test->loop);
}

static
void
test_unhandled5()
{
    TestSetup2 setup;
    TestUnhandled5 test;
    GIoRpcPeer* caller;
    GIoRpcPeer* callee;
    gulong id;

    test_setup2_init(&setup, NULL);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    caller = setup.peer[0];
    callee = setup.peer[1];

    /* Submit a call and wait for the handler to be invoked */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_unhandled5_request, &test)));
    giorpc_peer_start(callee);
    g_assert_cmpuint(giorpc_peer_call(caller, TEST_IID, TEST_CALL_CODE, NULL,
        test_response_unreached, &test, NULL), != ,0);

    test_run(&test_opt, test.loop);

    g_assert(test.req);
    giorpc_peer_remove_handler(callee, id);

    /* Prevent callee from being deallocated by test_setup2_cleanup */
    giorpc_peer_ref(callee);

    /* Stop the callee (the request is still pending) */
    giorpc_peer_stop(callee);
    test_setup2_cleanup(&setup);

    /* Try to complete the ignored request (and fail) */
    giorpc_request_unref(test.req);
    giorpc_peer_unref(callee);
}

/*==========================================================================*
 * response/main
 * response/thread
 * completion/main
 * completion/thread
 *==========================================================================*/

#define TEST_RESPONSE_CODE 42

typedef struct test_response {
    GIoRpcPeer* caller;
    GBytes* request_data;
    GBytes* response_data;
    GThread* response_thread;
    GMainContext* response_context;
    GMainLoop* loop;
    GIoRpcRequest* req;
} TestResponse;

static
void
test_response_request_cb(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestResponse* test = user_data;

    GDEBUG("Handling request");
    g_assert_cmpuint(code, == ,TEST_RESPONSE_CODE);
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);
    g_assert(g_bytes_equal(test->request_data, data));
    g_assert(!test->req);
    test->req = giorpc_request_ref(req);

    g_assert(giorpc_request_complete(req, test->response_data));
}

static
void
test_response_cb(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestResponse* test = user_data;

    GDEBUG("Done");
    g_assert(!error);
    g_assert(data);
    g_assert(g_bytes_equal(test->response_data, data));
    g_assert(g_main_context_is_owner(test->response_context));
    g_assert(test->response_thread == g_thread_self());
    test_quit_later(test->loop);
}

static
void
test_response_with_context(
    GThread* thread,
    GMainContext* context)
{
    TestSetup2 setup;
    TestResponse test;
    GIoRpcPeer* caller;
    GIoRpcPeer* callee;
    gulong id[2];
    static const guint8 in[] = { 0x01, 0x02 };
    static const guint8 out[] = { 0x03, 0x04, 0x05 };

    test_setup2_init(&setup, context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.response_thread = thread;
    test.response_context = context;
    test.request_data =  g_bytes_new_static(TEST_ARRAY_AND_SIZE(in));
    test.response_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(out));
    caller = test.caller = setup.peer[0];
    callee = setup.peer[1];

    /* This one will handle the call */
    g_assert((id[0] = giorpc_peer_add_request_handler(callee, TEST_IID,
        TEST_RESPONSE_CODE, test_response_request_cb, &test)));

    /* And this one is not going to be invoked */
    g_assert((id[1] = giorpc_peer_add_request_handler(callee, TEST_IID,
        TEST_RESPONSE_CODE+1, test_request_unreached, &test)));

    /* Submit the call */
    giorpc_peer_start(callee);
    g_assert_cmpuint(giorpc_peer_call(caller, TEST_IID,
        TEST_RESPONSE_CODE, test.request_data, test_response_cb,
        &test, NULL), != ,0);

    /* Wait for completion */
    test_run(&test_opt, setup.loop);

    /* The request has already been responded to */
    g_assert(test.req);
    g_assert(!giorpc_request_complete(test.req, NULL));

    giorpc_peer_remove_all_handlers(callee, id);
    giorpc_request_unref(test.req);
    g_bytes_unref(test.request_data);
    g_bytes_unref(test.response_data);

    test_setup2_cleanup(&setup);
}

static
void
test_completion_request_cb(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestResponse* test = user_data;

    GDEBUG("Handling request");
    g_assert_cmpuint(code, == ,TEST_RESPONSE_CODE);
    /* No response callback => NO_RESULT_DATA */
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == ,
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA);
    g_assert(g_bytes_equal(test->request_data, data));
    g_assert(!test->req);
    test->req = giorpc_request_ref(req);

    g_assert(giorpc_request_complete(req, test->response_data));
}

static
void
test_completion_cb(
    gpointer user_data)
{
    TestResponse* test = user_data;

    GDEBUG("Done");
    g_assert(g_main_context_is_owner(test->response_context));
    g_assert(test->response_thread == g_thread_self());
    test_quit_later(test->loop);
}

static
void
test_completion_late_cb(
    GIoRpcPeer* peer,
    GBytes* response,
    const GError* error,
    gpointer user_data)
{
    g_assert(!response);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_ABANDONED));
    GDEBUG("%s", error->message);
}

static
gboolean
test_completion_submit_late_call(
    gpointer user_data)
{
    TestResponse* test = user_data;

    /*
     * If the peer runs on the main thread, giorpc_peer_call will fail
     * right away.
     */
    if (giorpc_peer_call(test->caller, TEST_IID, TEST_RESPONSE_CODE, NULL,
        test_completion_late_cb, test->loop, test_exit_loop)) {
        GDEBUG("Waiting for late request to get dismissed");
    } else {
        test_quit_later(test->loop);
    }
    return G_SOURCE_REMOVE;
}

static
void
test_completion_with_context(
    GThread* thread,
    GMainContext* context)
{
    TestSetup2 setup;
    TestResponse test; /* Reuse the text context */
    GIoRpcPeer* caller;
    GIoRpcPeer* callee;
    gulong id;
    static const guint8 in[] = { 0x01, 0x02 };
    static const guint8 out[] = { 0x03, 0x04, 0x05 };

    test_setup2_init(&setup, context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.response_thread = thread;
    test.response_context = context;
    test.request_data =  g_bytes_new_static(TEST_ARRAY_AND_SIZE(in));
    test.response_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(out));
    caller = test.caller = setup.peer[0];
    callee = setup.peer[1];

    /* Submit the call */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        TEST_RESPONSE_CODE, test_completion_request_cb, &test)));
    giorpc_peer_start(callee);
    g_assert_cmpuint(giorpc_peer_call(caller, TEST_IID, TEST_RESPONSE_CODE,
        test.request_data, NULL, &test, test_completion_cb), != ,0);

    /* Wait for completion */
    test_run(&test_opt, setup.loop);

    /* Make sure no requests get sent after we stop the caller */
    giorpc_peer_remove_handler(callee, id);
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        TEST_RESPONSE_CODE, test_request_unreached, NULL)));
    giorpc_peer_stop(caller);
    g_idle_add(test_completion_submit_late_call, &test);
    test_run(&test_opt, test.loop);

    giorpc_request_unref(test.req);
    g_bytes_unref(test.request_data);
    g_bytes_unref(test.response_data);

    test_setup2_cleanup(&setup);
}

static
void
test_response_main()
{
    test_response_with_context(g_thread_self(), g_main_context_default());
}

static
void
test_response_thread()
{
    TestThread* thread = test_thread_new();

    test_response_with_context(thread->thread, thread->context);
    test_thread_free(thread);
}

static
void
test_completion_main()
{
    test_completion_with_context(g_thread_self(), g_main_context_default());
}

static
void
test_completion_thread()
{
    TestThread* thread = test_thread_new();

    test_completion_with_context(thread->thread, thread->context);
    test_thread_free(thread);
}

/*==========================================================================*
 * calls
 *==========================================================================*/

#define TEST_CALLS_CODE_0 24
#define TEST_CALLS_CODE_1 42

typedef struct test_calls {
    gint handled;
    gint completed;
    GIoRpcPeer* caller;
    GMainLoop* loop;
} TestCalls;

static
void
test_calls_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestCalls* test = user_data;

    GDEBUG("Request %u handled", code);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);
    g_atomic_int_inc(&test->handled);

    g_assert(giorpc_request_complete(req, NULL));
}

static
void
test_calls_complete(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestCalls* test = user_data;
    gint old_val;

    g_assert(!error);
    g_assert(data);
    old_val = g_atomic_int_add(&test->completed, 1);
    g_assert_cmpint(old_val, <= ,1);
    GDEBUG("Completion %d", old_val + 1);
    if (old_val == 1) {
        test_quit_later(test->loop);
    }
}

static
gboolean
test_calls_submit(
    gpointer user_data)
{
    TestCalls* test = user_data;

    g_assert_cmpuint(giorpc_peer_call(test->caller, TEST_IID,
        TEST_CALLS_CODE_0, NULL, test_calls_complete, test, NULL), != ,0);
    g_assert_cmpuint(giorpc_peer_call(test->caller, TEST_IID,
        TEST_CALLS_CODE_1, NULL, test_calls_complete, test, NULL), != ,0);
    return G_SOURCE_REMOVE;
}

static
void
test_calls()
{
    TestThread* t0 = test_thread_new();
    TestThread* t1 = test_thread_new();
    TestSetup2 setup;
    TestCalls test;
    GIoRpcPeer* callee;
    gulong id;

    test_setup2_init2(&setup, t0->context, t1->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.caller = setup.peer[0];
    callee = setup.peer[1];

    /*
     * Submit two calls and wait for them to complete. Note that they
     * are handled and completed on different threads.
     */
    giorpc_peer_start(callee); /* Caller gets started implicitly */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_calls_handler, &test)));

    test_thread_invoke_later(t0, test_calls_submit, &test);
    test_run(&test_opt, setup.loop);
    g_assert_cmpint(test.handled, == ,2);
    g_assert_cmpint(test.completed, == ,2);

    test_setup2_cleanup(&setup);
    test_thread_free(t0);
    test_thread_free(t1);
}

/*==========================================================================*
 * sync/basic
 *==========================================================================*/

#define TEST_SYNC_CALL (11)

typedef struct test_sync_basic {
    GIoRpcPeer* caller;
    gint handled;
    gint completed;
    GMainLoop* loop;
    GBytes* request_data;
    GBytes* response_data;
} TestSyncBasic;

static
void
test_sync_basic_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBasic* test = user_data;

    GDEBUG("Request %u handled", code);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_SYNC_CALL);
    g_assert(g_bytes_equal(test->request_data, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);
    g_assert(!test->handled);
    g_atomic_int_inc(&test->handled);

    g_assert(giorpc_request_complete(req, test->response_data));
}

static
gboolean
test_sync_basic_call(
    gpointer user_data)
{
    TestSyncBasic* test = user_data;
    GError* error = NULL;
    GBytes* response;

    /* Invalid IID and/or request code get rejected */
    g_assert(!giorpc_peer_call_sync(test->caller, 0, 0, NULL,
        GIORPC_SYNC_FLAGS_NONE, GIORPC_SYNC_TIMEOUT_INFINITE, NULL, NULL));
    g_assert(!giorpc_peer_call_sync(test->caller, 0, 0, NULL,
        GIORPC_SYNC_FLAGS_NONE, GIORPC_SYNC_TIMEOUT_INFINITE, NULL, &error));
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_INVALID_ARGS));
    g_clear_error(&error);

    /* Really submit the call */
    GDEBUG("Submitting sync call %u:%u", TEST_IID, TEST_SYNC_CALL);
    response = giorpc_peer_call_sync(test->caller, TEST_IID,
        TEST_SYNC_CALL, test->request_data, GIORPC_SYNC_FLAGS_NONE,
        GIORPC_SYNC_TIMEOUT_INFINITE, NULL, &error);
    GDEBUG("Sync call done");

    g_assert(!error);
    g_assert(response);
    g_assert(g_bytes_equal(response, test->response_data));
    g_bytes_unref(response);

    g_atomic_int_inc(&test->completed);
    test_quit_later(test->loop);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_basic()
{
    static const guint8 in[] = { 2, 3, 4 };
    static const guint8 out[] = { 5, 6 };
    TestThread* t0 = test_thread_new();
    TestThread* t1 = test_thread_new();
    TestSetup2 setup;
    TestSyncBasic test;
    GIoRpcPeer* callee;
    GError* error = NULL;
    gulong id;

    test_setup2_init2(&setup, t0->context, t1->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.caller = setup.peer[0];
    test.request_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(in));
    test.response_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(out));
    callee = setup.peer[1];

    /* Context is acquired by TestThread */
    g_assert(!giorpc_peer_call_sync(test.caller, TEST_IID, TEST_SYNC_CALL,
        NULL, GIORPC_SYNC_FLAGS_NONE, GIORPC_SYNC_TIMEOUT_INFINITE,
        NULL, NULL));
    g_assert(!giorpc_peer_call_sync(test.caller, TEST_IID, TEST_SYNC_CALL,
        NULL, GIORPC_SYNC_FLAGS_NONE, GIORPC_SYNC_TIMEOUT_INFINITE,
        NULL, &error));
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_NOT_OWNER));
    g_clear_error(&error);

    /* Make an RPC call from one thread to another */
    giorpc_peer_start(callee); /* Caller gets started implicitly */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_sync_basic_handler, &test)));
    test_thread_invoke_later(t0, test_sync_basic_call, &test);
    test_run(&test_opt, setup.loop);

    g_assert_cmpint(test.handled, == ,1);
    g_assert_cmpint(test.completed, == ,1);

    test_thread_free(t0);
    test_thread_free(t1);

    g_bytes_unref(test.request_data);
    g_bytes_unref(test.response_data);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/cancel
 *==========================================================================*/

typedef struct test_sync_cancel {
    GIoRpcPeer* caller;
    GMainLoop* loop;
    GBytes* request_data;
    GIoRpcRequest* req;
    GCancellable* cancel;
} TestSyncCancel;

static
gboolean
test_sync_cancel_request(
    gpointer cancel)
{
    GDEBUG("Cancelling the request");
    g_cancellable_cancel(cancel);
    g_object_unref(cancel);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_cancel_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncCancel* test = user_data;
    GCancellable* cancel = test->cancel;

    GDEBUG("Request %u received (but not handled)", code);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_SYNC_CALL);
    g_assert(g_bytes_equal(test->request_data, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    /* This handler is invoked twice, second time req isn't NULL */
    giorpc_request_unref(test->req);
    test->req = giorpc_request_ref(req);

    /* test_sync_cancel_request() will unref this cancellable */
    test->cancel = NULL;
    g_idle_add(test_sync_cancel_request, cancel);
}

static
gboolean
test_sync_cancel_call(
    gpointer user_data)
{
    TestSyncCancel* test = user_data;
    GError* error = NULL;
    GCancellable* cancel;

    /*
     * Must bump cancellable refcount to keep it alive until completion
     * of the sync call. test_sync_cancel_request() will consume the ref
     * returned by g_cancellable_new().
     */
    g_object_ref(cancel = test->cancel = g_cancellable_new());

    /* Submit the call */
    GDEBUG("Submitting sync call %u:%u", TEST_IID, TEST_SYNC_CALL);
    g_assert(!giorpc_peer_call_sync(test->caller, TEST_IID,
        TEST_SYNC_CALL, test->request_data, GIORPC_SYNC_FLAGS_NONE,
        GIORPC_SYNC_TIMEOUT_INFINITE, cancel, NULL));

    GDEBUG("Sync call finished");
    g_assert(!test->cancel); /* Zeroed by test_sync_cancel_handler() */
    g_assert(g_cancellable_is_cancelled(cancel));
    g_object_unref(cancel);

    /* Allocate another cancellable for the next request */
    g_object_ref(cancel = test->cancel = g_cancellable_new());

    /* Submit the call again, this time passing GError pointer */
    GDEBUG("Submitting sync call %u:%u again", TEST_IID, TEST_SYNC_CALL);
    g_assert(!giorpc_peer_call_sync(test->caller, TEST_IID,
        TEST_SYNC_CALL, test->request_data, GIORPC_SYNC_FLAGS_NONE,
        GIORPC_SYNC_TIMEOUT_INFINITE, cancel, &error));

    GDEBUG("Sync call finished");
    g_assert(!test->cancel); /* Zeroed by test_sync_cancel_handler() */
    g_assert(g_cancellable_is_cancelled(cancel));
    g_object_unref(cancel);

    /* Check the error */
    g_assert(error);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_CANCELLED));
    g_error_free(error);

    test_quit_later(test->loop);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_cancel()
{
    static const guint8 in[] = { 2, 3, 4 };
    TestThread* t0 = test_thread_new();
    TestThread* t1 = test_thread_new();
    TestSetup2 setup;
    TestSyncCancel test;
    GIoRpcPeer* callee;
    gulong id;

    test_setup2_init2(&setup, t0->context, t1->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.caller = setup.peer[0];
    test.request_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(in));
    callee = setup.peer[1];

    /* Make an RPC call from one thread to another */
    giorpc_peer_start(callee); /* Caller gets started implicitly */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_sync_cancel_handler, &test)));
    test_thread_invoke_later(t0, test_sync_cancel_call, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    g_assert(test.req);
    test_thread_free(t0);
    test_thread_free(t1);

    g_bytes_unref(test.request_data);
    giorpc_request_unref(test.req);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/timeout
 *==========================================================================*/

typedef struct test_sync_timeout {
    GIoRpcPeer* caller;
    GMainLoop* loop;
    GBytes* request_data;
    GIoRpcRequest* req;
} TestSyncTimeout;

static
void
test_sync_timeout_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncTimeout* test = user_data;

    GDEBUG("Request %u received (but not handled)", code);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_SYNC_CALL);
    g_assert(g_bytes_equal(test->request_data, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    /* This handler is invoked twice, second time req isn't NULL */
    giorpc_request_unref(test->req);
    test->req = giorpc_request_ref(req);
}

static
gboolean
test_sync_timeout_call(
    gpointer user_data)
{
    TestSyncTimeout* test = user_data;
    GError* error = NULL;
    GCancellable* cancel = g_cancellable_new();
    guint timeout_ms = 1; /* Anything > 0 */

    /* Submit the call */
    GDEBUG("Submitting sync call %u:%u", TEST_IID, TEST_SYNC_CALL);
    g_assert(!giorpc_peer_call_sync(test->caller, TEST_IID,
        TEST_SYNC_CALL, test->request_data, GIORPC_SYNC_FLAGS_NONE,
        timeout_ms, cancel, NULL));
    GDEBUG("Sync call finished");

    /* Submit the call again, this time passing GError pointer */
    GDEBUG("Submitting sync call %u:%u again", TEST_IID, TEST_SYNC_CALL);
    g_assert(!giorpc_peer_call_sync(test->caller, TEST_IID,
        TEST_SYNC_CALL, test->request_data, GIORPC_SYNC_FLAGS_NONE,
        timeout_ms, cancel, &error));
    GDEBUG("Sync call finished");

    /* Check the error */
    g_assert(error);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_TIMEOUT));
    g_error_free(error);

    g_object_unref(cancel);
    test_quit_later(test->loop);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_timeout()
{
    static const guint8 in[] = { 2, 3, 4 };
    TestThread* t0 = test_thread_new();
    TestThread* t1 = test_thread_new();
    TestSetup2 setup;
    TestSyncTimeout test;
    GIoRpcPeer* callee;
    gulong id;

    test_setup2_init2(&setup, t0->context, t1->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.caller = setup.peer[0];
    test.request_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(in));
    callee = setup.peer[1];

    /* Make an RPC call from one thread to another */
    giorpc_peer_start(callee); /* Caller gets started implicitly */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_sync_timeout_handler, &test)));
    test_thread_invoke_later(t0, test_sync_timeout_call, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    test_thread_free(t0);
    test_thread_free(t1);

    /*
     * The call may actually timeout before its handler is invoked
     * in which case test.req will be NULL.
     */
    giorpc_request_unref(test.req);
    g_bytes_unref(test.request_data);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/unhandled
 *==========================================================================*/

typedef struct test_sync_unhandled {
    GIoRpcPeer* caller;
    GMainLoop* loop;
    GBytes* request_data;
} TestSyncUnhandled;

static
void
test_sync_unhandled_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncUnhandled* test = user_data;

    GDEBUG("Request %u received (but not handled)", code);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_SYNC_CALL);
    g_assert(g_bytes_equal(test->request_data, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);
}

static
gboolean
test_sync_unhandled_call(
    gpointer user_data)
{
    TestSyncUnhandled* test = user_data;
    GCancellable* cancel = g_cancellable_new();
    GError* error = NULL;

    /* Submit the call */
    GDEBUG("Submitting sync call %u:%u", TEST_IID, TEST_SYNC_CALL);
    g_assert(!giorpc_peer_call_sync(test->caller, TEST_IID,
        TEST_SYNC_CALL, test->request_data, GIORPC_SYNC_FLAGS_NONE,
        SYNC_TIMEOUT_MS, cancel, NULL));
    GDEBUG("Sync call finished");

    /* Submit the call again, this time passing GError pointer */
    GDEBUG("Submitting sync call %u:%u again", TEST_IID, TEST_SYNC_CALL);
    g_assert(!giorpc_peer_call_sync(test->caller, TEST_IID,
        TEST_SYNC_CALL, test->request_data, GIORPC_SYNC_FLAGS_NONE,
        SYNC_TIMEOUT_MS, cancel, &error));
    GDEBUG("Sync call finished");

    /* Check the error */
    g_assert(error);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_UNHANDLED));
    g_error_free(error);

    test_quit_later(test->loop);
    g_object_unref(cancel);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_unhandled()
{
    static const guint8 in[] = { 2, 3, 4 };
    TestThread* t0 = test_thread_new();
    TestThread* t1 = test_thread_new();
    TestSetup2 setup;
    TestSyncUnhandled test;
    GIoRpcPeer* callee;
    gulong id;

    test_setup2_init2(&setup, t0->context, t1->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.caller = setup.peer[0];
    test.request_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(in));
    callee = setup.peer[1];

    /* Make an RPC call from one thread to another */
    giorpc_peer_start(callee); /* Caller gets started implicitly */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        GIORPC_CODE_ANY, test_sync_unhandled_handler, &test)));
    test_thread_invoke_later(t0, test_sync_unhandled_call, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    test_thread_free(t0);
    test_thread_free(t1);

    g_bytes_unref(test.request_data);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/block/none
 *==========================================================================*/

typedef struct test_sync_block_none {
    GIoRpcPeer* pa;
    GIoRpcPeer* pb;
    GMainLoop* loop;
    GBytes* pa_request_data4;
    GBytes* pa_request_data5;
    GBytes* pa_request_result5;
    GBytes* pa_request_data6;
    GBytes* pa_request_result6;
    GBytes* pb_notify_data1;
    GBytes* pb_notify_data3;
    GBytes* pb_request_data2;
    GBytes* pb_request_result2;
    gint last_code;
} TestSyncBlockNone;

static
void
test_sync_block_none_pb_call_complete2(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;

    GDEBUG("Call 2 complete");
    g_assert(peer == test->pb);
    g_assert(!error);
    g_assert(data);
    g_assert(g_bytes_equal(data, test->pb_request_result2));
}

static
void
test_sync_block_none_pa_call_handler2(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;

    GDEBUG("Peer A received call %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,2);
    g_assert(g_bytes_equal(data, test->pb_request_data2));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    /* Update last_code 1 => 2 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 1, 2));
    g_assert(giorpc_request_complete(req, test->pb_request_result2));
}

static
void
test_sync_block_none_pa_notify3(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,3);
    g_assert(g_bytes_equal(data, test->pb_notify_data3));

    /* Update last_code 2 => 3 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 2, 3));
}

static
void
test_sync_block_none_pb_call_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;

    GDEBUG("Peer B received call %u", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    switch(code) {
    case 4:
        g_assert(g_bytes_equal(data, test->pa_request_data4));
        /*  Ignore this one */
        break;
    case 5:
        g_assert(g_bytes_equal(data, test->pa_request_data5));
        g_assert(giorpc_request_complete(req, test->pa_request_result5));
        break;
    case 6:
        g_assert(g_bytes_equal(data, test->pa_request_data6));
        g_assert(giorpc_request_complete(req, test->pa_request_result6));
        break;
    default:
        g_assert_not_reached();
        break;
    }
}

static
void
test_sync_block_none_pa_call_complete4(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;

    GDEBUG("Call 4 complete");
    g_assert(peer == test->pa);
    g_assert(!data);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_UNHANDLED));

    /* Update last_code 3 => 4 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 3, 4));
}

static
void
test_sync_block_none_pa_call_complete5(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;

    GDEBUG("Call 5 complete");
    g_assert(peer == test->pa);
    g_assert(!error);
    g_assert(data);
    g_assert(g_bytes_equal(data, test->pa_request_result5));

    /* Update last_code 4 => 5 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 4, 5));
}

static
void
test_sync_block_none_pa_notify1(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;
    GBytes* result;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,1);
    g_assert(g_bytes_equal(data, test->pb_notify_data1));

    /* Update last_code 0 => 1 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 0, 1));

    GDEBUG("Peer A submitting call 4");
    g_assert(giorpc_peer_call(test->pa, TEST_IID, 4, test->pa_request_data4,
        test_sync_block_none_pa_call_complete4, test, NULL));

    GDEBUG("Peer A submitting call 5");
    g_assert(giorpc_peer_call(test->pa, TEST_IID, 5, test->pa_request_data5,
        test_sync_block_none_pa_call_complete5, test, NULL));

    /* Make the sync call */
    GDEBUG("Peer A making sync call 6");
    result = giorpc_peer_call_sync(test->pa, TEST_IID, 6,
        test->pa_request_data6, GIORPC_SYNC_FLAGS_NONE,
        SYNC_TIMEOUT_MS, NULL, NULL);
    GDEBUG("Sync call 6 done");
    g_assert(result);
    g_assert(g_bytes_equal(result, test->pa_request_result6));
    g_bytes_unref(result);

    g_assert_cmpint(test->last_code, == ,5);

    /* Done with the test */
    test_quit_later(test->loop);
}

static
gboolean
test_sync_block_none_start(
    gpointer user_data)
{
    TestSyncBlockNone* test = user_data;

    GDEBUG("Peer B submitting notify 1");
    giorpc_peer_notify(test->pb, TEST_IID, 1, test->pb_notify_data1);

    GDEBUG("Peer B submitting call 2");
    g_assert(giorpc_peer_call(test->pb, TEST_IID, 2, test->pb_request_data2,
        test_sync_block_none_pb_call_complete2, test, NULL));

    GDEBUG("Peer B submitting notify 3");
    giorpc_peer_notify(test->pb, TEST_IID, 3, test->pb_notify_data3);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_block_none()
{
    static const guint8 data1[] = { 1 };
    static const guint8 data2[] = { 2, 3 };
    static const guint8 data3[] = { 4, 5, 6 };
    static const guint8 data4[] = { 7, 8, 9, 10 };
    static const guint8 data5[] = { 11, 12, 13, 14, 15 };
    static const guint8 data6[] = { 16, 17, 18, 19, 20, 21 };
    static const guint8 data7[] = { 22, 23, 24, 25, 26, 27, 28};
    static const guint8 data8[] = { 29, 30, 31, 32, 33, 34, 35, 36};
    static const guint8 data9[] = { 37, 38, 39, 40, 41, 42, 43, 44, 45};
    TestThread* ta = test_thread_new();
    TestThread* tb = test_thread_new();
    TestSetup2 setup;
    TestSyncBlockNone test;
    gulong ida[3];
    gulong idb;

    test_setup2_init2(&setup, ta->context, tb->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.pa = setup.peer[0];
    test.pb = setup.peer[1];
    test.pa_request_data4 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data1));
    test.pa_request_data5 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data2));
    test.pa_request_result5 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data3));
    test.pa_request_data6 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data4));
    test.pa_request_result6 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data5));
    test.pb_notify_data1 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data6));
    test.pb_notify_data3 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data7));
    test.pb_request_data2 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data8));
    test.pb_request_result2 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data9));

    /*
     * Peer A and peer B operate on threads A and B, respectively
     *
     * Test scenario:
     *
     * 1. Peer B submits notification 1, async call 2 and notification 3
     * 2. Peer A receives notification 1 and makes async calls 4 (fails)
     *    and 5 (succeeds) followed by sync call 6 with no flags.
     * 3. Peer A receives incoming call 2, notification 3 and completion
     *    callbacks for its calls 4 and 5 (nothing is blocked)
     * 4. Call 6 completes.
     */
    g_assert((ida[0] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        1, test_sync_block_none_pa_notify1, &test)));
    g_assert((ida[1] = giorpc_peer_add_request_handler(test.pa, TEST_IID,
        2, test_sync_block_none_pa_call_handler2, &test)));
    g_assert((ida[2] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        3, test_sync_block_none_pa_notify3, &test)));
    g_assert((idb = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        GIORPC_CODE_ANY, test_sync_block_none_pb_call_handler, &test)));
    giorpc_peer_start(test.pa); /* Peer B gets started implicitly */
    test_thread_invoke_later(tb, test_sync_block_none_start, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    g_assert_cmpint(test.last_code, == ,5);
    test_thread_free(ta);
    test_thread_free(tb);

    giorpc_peer_remove_all_handlers(test.pa, ida);
    giorpc_peer_remove_handler(test.pb, idb);
    g_bytes_unref(test.pa_request_data4);
    g_bytes_unref(test.pa_request_data5);
    g_bytes_unref(test.pa_request_result5);
    g_bytes_unref(test.pa_request_data6);
    g_bytes_unref(test.pa_request_result6);
    g_bytes_unref(test.pb_notify_data1);
    g_bytes_unref(test.pb_notify_data3);
    g_bytes_unref(test.pb_request_data2);
    g_bytes_unref(test.pb_request_result2);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/block/request
 *==========================================================================*/

typedef struct test_sync_block_request {
    GIoRpcPeer* pa;
    GIoRpcPeer* pb;
    GMainLoop* loop;
    GBytes* pa_request_data22;
    GBytes* pa_request_result22;
    GBytes* pb_notify_data1;
    GBytes* pb_notify_data2;
    GBytes* pb_request_data3;
    GBytes* pb_request_data4;
    GBytes* pb_request_result4;
    gint last_code;
} TestSyncBlockRequest;

static
void
test_sync_block_request_pb_call_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockRequest* test = user_data;

    GDEBUG("Peer B received call %u", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,22);
    g_assert(g_bytes_equal(test->pa_request_data22, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    g_assert(giorpc_request_complete(req, test->pa_request_result22));
}

static
void
test_sync_block_request_pa_call_handler3(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockRequest* test = user_data;

    GDEBUG("Peer A received call %u (and ignored it)", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,3);
    g_assert(g_bytes_equal(data, test->pb_request_data3));
    g_assert(giorpc_request_flags(req) & GIORPC_REQUEST_FLAG_ONE_WAY);
    /* Ignore the request */

    /* Update last_code 2 => 3 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 2, 3));
}

static
void
test_sync_block_request_pa_call_handler4(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockRequest* test = user_data;

    GDEBUG("Peer A received call %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,4);
    g_assert(g_bytes_equal(data, test->pb_request_data4));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    /* Update last_code 3 => 4 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 3, 4));
    g_assert(giorpc_request_complete(req, test->pb_request_result4));
}

static
void
test_sync_block_request_pb_call_complete4(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockRequest* test = user_data;

    GDEBUG("Call 4 complete");
    g_assert(peer == test->pb);
    g_assert(!error);
    g_assert(data);
    g_assert(g_bytes_equal(data, test->pb_request_result4));

    /* Done with the test */
    test_quit_later(test->loop);
}

static
void
test_sync_block_request_pa_notify2(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockRequest* test = user_data;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,2);
    g_assert(g_bytes_equal(test->pb_notify_data2, data));

    /* Update last_code 1 => 2 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 1, 2));
}

static
void
test_sync_block_request_pa_notify1(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    gpointer user_data)
{
    TestSyncBlockRequest* test = user_data;
    GBytes* result;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,1);

    /* Update last_code 0 => 1 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 0, 1));

    /* Make the sync call */
    GDEBUG("Making sync call 22");
    result = giorpc_peer_call_sync(test->pa, TEST_IID, 22,
        test->pa_request_data22, GIORPC_SYNC_FLAG_BLOCK_REQUEST,
        SYNC_TIMEOUT_MS, NULL, NULL);
    GDEBUG("Sync call done");
    g_assert(result);
    g_assert(g_bytes_equal(result, test->pa_request_result22));
    g_bytes_unref(result);

    /* Incoming notification 2 must have arrived */
    g_assert_cmpint(test->last_code, == ,2);
}

static
gboolean
test_sync_block_request_start(
    gpointer user_data)
{
    TestSyncBlockRequest* test = user_data;

    /*
     * This triggers test_sync_block_request_pa_notify1 for peer A,
     * which makes a sync call.
     */
    GDEBUG("Peer B submitting notify 1");
    giorpc_peer_notify(test->pb, TEST_IID, 1, test->pb_notify_data1);

    /* These get blocked until completion of the sync call 22 */
    GDEBUG("Peer B submitting call 3");
    g_assert(giorpc_peer_call(test->pb, TEST_IID, 3, test->pb_request_data3,
        NULL, test, NULL)); /* One-way call */
    GDEBUG("Peer B submitting call 4");
    g_assert(giorpc_peer_call(test->pb, TEST_IID, 4, test->pb_request_data4,
        test_sync_block_request_pb_call_complete4, test, NULL));

    /* But this one must go through, it's not blocked */
    GDEBUG("Peer B submitting notify 2");
    giorpc_peer_notify(test->pb, TEST_IID, 2, test->pb_notify_data2);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_block_request()
{
    static const guint8 data1[] = { 1 };
    static const guint8 data2[] = { 2, 3 };
    static const guint8 data3[] = { 4, 5, 6 };
    static const guint8 data4[] = { 7, 8, 9, 10 };
    static const guint8 data5[] = { 11, 12, 13, 14, 15 };
    static const guint8 data6[] = { 16, 17, 18, 19, 20, 21 };
    static const guint8 data7[] = { 22, 23, 24, 25, 26, 27, 28};
    TestThread* ta = test_thread_new();
    TestThread* tb = test_thread_new();
    TestSetup2 setup;
    TestSyncBlockRequest test;
    gulong ida[4];
    gulong idb;

    test_setup2_init2(&setup, ta->context, tb->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.pa = setup.peer[0];
    test.pb = setup.peer[1];
    test.pa_request_data22 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data1));
    test.pa_request_result22 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data2));
    test.pb_notify_data1 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data3));
    test.pb_notify_data2 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data4));
    test.pb_request_data3 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data5));
    test.pb_request_data4 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data6));
    test.pb_request_result4 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data7));

    /*
     * Peer A and peer B operate on threads A and B, respectively
     *
     * Test scenario:
     *
     * 1. Peer B submits notification 1, async calls 3, 4 and notification 2
     * 2. Peer A receives notification 1 and makes sync call 22 with
     *    GIORPC_SYNC_FLAG_BLOCK_REQUEST flag. Calls 3 and 4 get queued until
     *    completion of call 22.
     * 3. Meanwhile peer A receives notification 2, it's not blocked
     * 4. Peer B handles call 22 and responds to it. That unblocks call 3
     *    and 4, which must arrive in the right order.
     * 5. Call 4 handler quits the event loop and terminates the test.
     */
    g_assert((ida[0] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        1, test_sync_block_request_pa_notify1, &test)));
    g_assert((ida[1] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        2, test_sync_block_request_pa_notify2, &test)));
    g_assert((ida[2] = giorpc_peer_add_request_handler(test.pa, TEST_IID,
        3, test_sync_block_request_pa_call_handler3, &test)));
    g_assert((ida[3] = giorpc_peer_add_request_handler(test.pa, TEST_IID,
        4, test_sync_block_request_pa_call_handler4, &test)));
    g_assert((idb = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        GIORPC_CODE_ANY, test_sync_block_request_pb_call_handler, &test)));
    giorpc_peer_start(test.pa); /* Peer B gets started implicitly */
    test_thread_invoke_later(tb, test_sync_block_request_start, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    g_assert_cmpint(test.last_code, == ,4);
    test_thread_free(ta);
    test_thread_free(tb);

    giorpc_peer_remove_all_handlers(test.pa, ida);
    giorpc_peer_remove_handler(test.pb, idb);
    g_bytes_unref(test.pa_request_data22);
    g_bytes_unref(test.pa_request_result22);
    g_bytes_unref(test.pb_notify_data1);
    g_bytes_unref(test.pb_notify_data2);
    g_bytes_unref(test.pb_request_data3);
    g_bytes_unref(test.pb_request_data4);
    g_bytes_unref(test.pb_request_result4);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/block/response
 *==========================================================================*/

typedef struct test_sync_block_response {
    GIoRpcPeer* pa;
    GIoRpcPeer* pb;
    GMainLoop* loop;
    GBytes* pb_notify_data1;
    GBytes* pb_notify_data2;
    GBytes* pa_request_data3;
    GBytes* pa_request_result3;
    GBytes* pa_request_data4;
    GBytes* pa_request_data5;
    GBytes* pa_request_result5;
    gint last_code;
} TestSyncBlockResponse;

static
void
test_sync_block_response_pa_call_complete4(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;

    GDEBUG("Call 4 complete");
    g_assert(peer == test->pa);
    g_assert(!data);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_UNHANDLED));

    /* Update last_code 3 => 4 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 3, 4));
}

static
void
test_sync_block_response_pa_call_complete5(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;

    GDEBUG("Call 5 complete");
    g_assert(peer == test->pa);
    g_assert(!error);
    g_assert(data);
    g_assert(g_bytes_equal(data, test->pa_request_result5));

    /* Update last_code 4 => 5 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 4, 5));

    /* Done with the test */
    test_quit_later(test->loop);
}

static
void
test_sync_block_response_pb_call_handler3(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;

    GDEBUG("Peer B received call %u", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,3);
    g_assert(g_bytes_equal(test->pa_request_data3, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    g_assert(giorpc_request_complete(req, test->pa_request_result3));
}

static
void
test_sync_block_response_pb_call_handler4(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;

    GDEBUG("Peer B received call %u (and ignored it)", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,4);
    g_assert(g_bytes_equal(data, test->pa_request_data4));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);
    /* Ignore the request */
}

static
void
test_sync_block_response_pb_call_handler5(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;

    GDEBUG("Peer B received call %u", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,5);
    g_assert(g_bytes_equal(data, test->pa_request_data5));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    g_assert(giorpc_request_complete(req, test->pa_request_result5));
}


static
void
test_sync_block_response_pa_notify2(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,2);
    g_assert(data);
    g_assert(g_bytes_equal(data, test->pb_notify_data2));

    /* Update last_code 1 => 2 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 1, 2));

}

static
void
test_sync_block_response_pa_notify1(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;
    GBytes* result;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,1);
    g_assert(data);
    g_assert(g_bytes_equal(data, test->pb_notify_data1));

    /* Update last_code 0 => 1 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 0, 1));

    /* Make async calls */
    GDEBUG("Peer A making async calls 4 and 5");
    g_assert(giorpc_peer_call(test->pa, TEST_IID, 4, test->pa_request_data4,
        test_sync_block_response_pa_call_complete4, test, NULL));
    g_assert(giorpc_peer_call(test->pa, TEST_IID, 5, test->pa_request_data5,
        test_sync_block_response_pa_call_complete5, test, NULL));

    /* Make async call */
    GDEBUG("Peer A making sync call 3");
    result = giorpc_peer_call_sync(test->pa, TEST_IID, 3,
        test->pa_request_data3, GIORPC_SYNC_FLAG_BLOCK_RESPONSE,
        SYNC_TIMEOUT_MS, NULL, NULL);
    GDEBUG("Sync call done");
    g_assert(result);
    g_assert(g_bytes_equal(result, test->pa_request_result3));
    g_bytes_unref(result);

    /* Incoming notification 2 must have arrived */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 2, 3));
}

static
gboolean
test_sync_block_response_start(
    gpointer user_data)
{
    TestSyncBlockResponse* test = user_data;

    GDEBUG("Peer B submitting notify 1");
    giorpc_peer_notify(test->pb, TEST_IID, 1, test->pb_notify_data1);

    GDEBUG("Peer B submitting notify 2");
    giorpc_peer_notify(test->pb, TEST_IID, 2, test->pb_notify_data2);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_block_response()
{
    static const guint8 data1[] = { 1 };
    static const guint8 data2[] = { 2, 3 };
    static const guint8 data3[] = { 4, 5, 6 };
    static const guint8 data4[] = { 7, 8, 9, 10 };
    static const guint8 data5[] = { 11, 12, 13, 14, 15 };
    static const guint8 data6[] = { 16, 17, 18, 19, 20, 21 };
    static const guint8 data7[] = { 22, 23, 24, 25, 26, 27, 28};
    TestThread* ta = test_thread_new();
    TestThread* tb = test_thread_new();
    TestSetup2 setup;
    TestSyncBlockResponse test;
    gulong ida[2];
    gulong idb[3];

    test_setup2_init2(&setup, ta->context, tb->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.pa = setup.peer[0];
    test.pb = setup.peer[1];
    test.pb_notify_data1 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data1));
    test.pb_notify_data2 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data2));
    test.pa_request_data3 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data3));
    test.pa_request_result3 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data4));
    test.pa_request_data4 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data5));
    test.pa_request_data5 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data6));
    test.pa_request_result5 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data7));

    /*
     * Peer A and peer B operate on threads A and B, respectively
     *
     * Test scenario:
     *
     * 1. Peer B submits notifications 1 and 2
     * 2. Peer A receives notification 1 and makes async call 4 and 5
     *    and sync call 3
     *    with GIORPC_SYNC_FLAG_BLOCK_RESPONSE flag.
     * 3. Peer A receives notification 2, it's not blocked
     * 4. Peer B handles calls 4 (with error), 5 and 3. Completiuons of
     *    calls 4 and 5 are blocked until completion of call 3
     * 5. Call 3 gets completed
     * 6. Calls 4 and 5 are completed asynchronously
     * 7. Call 5 completion callback quits the event loop and terminates
     *    the test.
     */
    g_assert((ida[0] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        1, test_sync_block_response_pa_notify1, &test)));
    g_assert((ida[1] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        2, test_sync_block_response_pa_notify2, &test)));
    g_assert((idb[0] = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        3, test_sync_block_response_pb_call_handler3, &test)));
    g_assert((idb[1] = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        4, test_sync_block_response_pb_call_handler4, &test)));
    g_assert((idb[2] = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        5, test_sync_block_response_pb_call_handler5, &test)));
    giorpc_peer_start(test.pa); /* Peer B gets started implicitly */
    test_thread_invoke_later(tb, test_sync_block_response_start, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    g_assert_cmpint(test.last_code, == ,5);
    test_thread_free(ta);
    test_thread_free(tb);

    giorpc_peer_remove_all_handlers(test.pa, ida);
    giorpc_peer_remove_all_handlers(test.pb, idb);
    g_bytes_unref(test.pb_notify_data1);
    g_bytes_unref(test.pb_notify_data2);
    g_bytes_unref(test.pa_request_data3);
    g_bytes_unref(test.pa_request_result3);
    g_bytes_unref(test.pa_request_data4);
    g_bytes_unref(test.pa_request_data5);
    g_bytes_unref(test.pa_request_result5);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/block/notify
 *==========================================================================*/

typedef struct test_sync_block_notify {
    GIoRpcPeer* pa;
    GIoRpcPeer* pb;
    GMainLoop* loop;
    GBytes* pa_request_data;
    GBytes* pa_request_result;
    GBytes* pb_request_data;
    GBytes* pb_request_result;
    GBytes* pb_notify_data1;
    GBytes* pb_notify_data3;
    GBytes* pb_notify_data4;
    gint last_code;
} TestSyncBlockNotify;

static
void
test_sync_block_notify_pa_call_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNotify* test = user_data;

    GDEBUG("Peer A received call %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,22);
    g_assert(g_bytes_equal(test->pb_request_data, data));
    g_assert(giorpc_request_flags(req) & GIORPC_REQUEST_FLAG_ONE_WAY);

    /* Update last_code 1 => 2 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 1, 2));
    g_assert(giorpc_request_complete(req, test->pb_request_result));
}

static
void
test_sync_block_notify_pb_call_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNotify* test = user_data;

    GDEBUG("Peer B received call %u", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,2);
    g_assert(g_bytes_equal(test->pa_request_data, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    g_assert(giorpc_request_complete(req, test->pa_request_result));
}

static
void
test_sync_block_notify_pa_notify1(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    gpointer user_data)
{
    TestSyncBlockNotify* test = user_data;
    GBytes* result;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,1);

    /* Update last_code 0 => 1 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 0, 1));

    /* Make the sync call */
    GDEBUG("Making sync call 2");
    result = giorpc_peer_call_sync(test->pa, TEST_IID, 2,
        test->pa_request_data, GIORPC_SYNC_FLAG_BLOCK_NOTIFY,
        SYNC_TIMEOUT_MS, NULL, NULL);
    GDEBUG("Sync call done");
    g_assert(result);
    g_assert(g_bytes_equal(result, test->pa_request_result));
    g_bytes_unref(result);

    /* Incoming call 2 must have arrived */
    g_assert_cmpint(test->last_code, == ,2);
}

static
void
test_sync_block_notify_pa_notify3(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockNotify* test = user_data;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,3);
    g_assert(g_bytes_equal(test->pb_notify_data3, data));

    /* Update last_code 2 => 3 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 2, 3));
}

static
void
test_sync_block_notify_pa_notify4(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockNotify* test = user_data;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,4);
    g_assert(g_bytes_equal(test->pb_notify_data4, data));

    /* Update last_code 3 => 4 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 3, 4));

    /* Done with the test */
    test_quit_later(test->loop);
}

static
gboolean
test_sync_block_notify_start(
    gpointer user_data)
{
    TestSyncBlockNotify* test = user_data;

    /*
     * This triggers test_sync_block_notify_2 for peer A, which
     * makes a sync call.
     */
    GDEBUG("Peer B submitting notify 1");
    giorpc_peer_notify(test->pb, TEST_IID, 1, test->pb_notify_data1);

    /*
     * By the time peer A receives this one, it's going to be waiting
     * for the sync call to complete. This notification has to be blocked.
     */
    GDEBUG("Peer B submitting notify 3");
    giorpc_peer_notify(test->pb, TEST_IID, 3, test->pb_notify_data3);
    GDEBUG("Peer B submitting notify 4");
    giorpc_peer_notify(test->pb, TEST_IID, 4, test->pb_notify_data4);

    /* But this call must go through, it's not blocked */
    GDEBUG("Peer B submitting call 2");
    g_assert(giorpc_peer_call(test->pb, TEST_IID, 22,
        test->pb_request_data, NULL, test, NULL));
    return G_SOURCE_REMOVE;
}

static
void
test_sync_block_notify()
{
    static const guint8 data1[] = { 1 };
    static const guint8 data2[] = { 2, 3 };
    static const guint8 data3[] = { 4, 5, 6 };
    static const guint8 data4[] = { 7, 8, 9, 10 };
    static const guint8 data5[] = { 11, 12, 13, 14, 15 };
    static const guint8 data6[] = { 16, 17, 18, 19, 20, 21 };
    static const guint8 data7[] = { 22, 23, 24, 25, 26, 27, 28};
    TestThread* ta = test_thread_new();
    TestThread* tb = test_thread_new();
    TestSetup2 setup;
    TestSyncBlockNotify test;
    gulong ida[4];
    gulong idb;

    test_setup2_init2(&setup, ta->context, tb->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.pa = setup.peer[0];
    test.pb = setup.peer[1];
    test.pa_request_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data1));
    test.pa_request_result = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data2));
    test.pb_request_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data3));
    test.pb_request_result = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data4));
    test.pb_notify_data1 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data5));
    test.pb_notify_data3 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data6));
    test.pb_notify_data4 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data7));

    /*
     * Peer A and peer B operate on threads A and B, respectively
     *
     * Test scenario:
     *
     * 1. Peer B submits notifications 1, 3 and 4 and async call 22
     * 2. Peer A receives notification 1 and makes sync call 2 with
     *    GIORPC_SYNC_FLAG_BLOCK_NOTIFY flag. Notifications 3 and 4
     *    are sitting in the queue waiting for completion of call 2.
     * 3. Meanwhile peer A receives call 22, it's not blocked
     * 4. Peer B handles call 2 and responds to it. That unblocks
     *    notifications 3 and 4 which must arrive in the right order.
     * 5. Notification 4 quits the event loop and terminates the test.
     */
    g_assert((ida[0] = giorpc_peer_add_request_handler(test.pa, TEST_IID,
        GIORPC_CODE_ANY, test_sync_block_notify_pa_call_handler, &test)));
    g_assert((ida[1] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        1, test_sync_block_notify_pa_notify1, &test)));
    g_assert((ida[2] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        3, test_sync_block_notify_pa_notify3, &test)));
    g_assert((ida[3] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        4, test_sync_block_notify_pa_notify4, &test)));
    g_assert((idb = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        2, test_sync_block_notify_pb_call_handler, &test)));
    giorpc_peer_start(test.pa); /* Peer B gets started implicitly */
    test_thread_invoke_later(tb, test_sync_block_notify_start, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    g_assert_cmpint(test.last_code, == ,4);
    test_thread_free(ta);
    test_thread_free(tb);

    giorpc_peer_remove_all_handlers(test.pa, ida);
    giorpc_peer_remove_handler(test.pb, idb);
    g_bytes_unref(test.pa_request_data);
    g_bytes_unref(test.pa_request_result);
    g_bytes_unref(test.pb_request_data);
    g_bytes_unref(test.pb_request_result);
    g_bytes_unref(test.pb_notify_data1);
    g_bytes_unref(test.pb_notify_data3);
    g_bytes_unref(test.pb_notify_data4);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * sync/block/nested
 *==========================================================================*/

typedef struct test_sync_block_nested {
    GIoRpcPeer* pa;
    GIoRpcPeer* pb;
    GMainLoop* loop;
    GBytes* pa_request_data3;
    GBytes* pa_request_result3;
    GBytes* pa_request_data5;
    GBytes* pa_request_result5;
    GBytes* pb_notify_data1;
    GBytes* pb_notify_data2;
    GBytes* pb_notify_data4;
    GBytes* pb_request_data6;
    GBytes* pb_request_data7;
    GBytes* pb_request_result7;
    gint last_code;
} TestSyncBlockNested;


static
void
test_sync_block_nested_pb_call_complete6(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    GDEBUG("Call 6 complete");
    g_assert(peer == test->pb);
    g_assert(!data);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_UNHANDLED));

    /* Update last_code 5 => 6 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 5, 6));
}

static
void
test_sync_block_nested_pb_call_complete7(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    GDEBUG("Call 7 complete");
    g_assert(peer == test->pb);
    g_assert(!error);
    g_assert(data);
    g_assert(g_bytes_equal(data, test->pb_request_result7));

    /* Update last_code 6 => 7 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 6, 7));

    /* Done with the test */
    test_quit_later(test->loop);
}

static
void
test_sync_block_nested_pa_call_handler6(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    GDEBUG("Peer A received call %u (and ignored it)", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,6);
    g_assert(g_bytes_equal(data, test->pb_request_data6));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);
    /* Ignore the request */
}

static
void
test_sync_block_nested_pa_call_handler7(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    GDEBUG("Peer A received call %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,7);
    g_assert(g_bytes_equal(data, test->pb_request_data7));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    g_assert(giorpc_request_complete(req, test->pb_request_result7));
}

static
void
test_sync_block_request_pb_call_handler3(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    GDEBUG("Peer B received call %u", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,3);
    g_assert(g_bytes_equal(data, test->pa_request_data3));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    g_assert(giorpc_request_complete(req, test->pa_request_result3));
}

static
void
test_sync_block_request_pb_call_handler5(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    GDEBUG("Peer B received call %u", code);
    g_assert(peer == test->pb);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,5);
    g_assert(g_bytes_equal(data, test->pa_request_data5));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    GDEBUG("Peer B submitting notify 4");
    giorpc_peer_notify(peer, TEST_IID, 4, test->pb_notify_data4);
    g_assert(giorpc_request_complete(req, test->pa_request_result5));
}

static
void
test_sync_block_nested_pa_notify4(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,4);
    g_assert(g_bytes_equal(test->pb_notify_data4, data));

    /* Update last_code 3 => 4 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 3, 4));
}


static
void
test_sync_block_nested_pa_notify2(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;
    GBytes* result;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,2);
    g_assert(g_bytes_equal(test->pb_notify_data2, data));

    /* Update last_code 1 => 2 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 1, 2));

    /* Make a sync call */
    GDEBUG("Making nested sync call 3");
    result = giorpc_peer_call_sync(test->pa, TEST_IID, 3,
        test->pa_request_data3, GIORPC_SYNC_FLAG_BLOCK_ALL,
        SYNC_TIMEOUT_MS, NULL, NULL);
    GDEBUG("Sync call 3 done");
    g_assert(result);
    g_assert(g_bytes_equal(result, test->pa_request_result3));
    g_bytes_unref(result);

    /* Update last_code 2 => 3 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 2, 3));
}

static
void
test_sync_block_nested_pa_notify1(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;
    GBytes* result;

    GDEBUG("Peer A received notification %u", code);
    g_assert(peer == test->pa);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,1);

    /* Update last_code 0 => 1 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 0, 1));

    /* Nested sync call */
    GDEBUG("Making sync call 5");
    result = giorpc_peer_call_sync(test->pa, TEST_IID, 5,
        test->pa_request_data5, GIORPC_SYNC_FLAG_BLOCK_REQUEST,
        SYNC_TIMEOUT_MS, NULL, NULL);
    GDEBUG("Sync call 5 done");
    g_assert(result);
    g_assert(g_bytes_equal(result, test->pa_request_result5));
    g_bytes_unref(result);

    /* Update last_code 4 => 5 */
    g_assert(g_atomic_int_compare_and_exchange(&test->last_code, 4, 5));
}

static
gboolean
test_sync_block_nested_start(
    gpointer user_data)
{
    TestSyncBlockNested* test = user_data;

    /*
     * This triggers test_sync_block_nested_pa_notify1 for peer A,
     * which makes a sync call.
     */
    GDEBUG("Peer B submitting notify 1");
    giorpc_peer_notify(test->pb, TEST_IID, 1, test->pb_notify_data1);

    /* These get blocked until completion of the sync call 22 */
    GDEBUG("Peer B submitting call 6");
    g_assert(giorpc_peer_call(test->pb, TEST_IID, 6, test->pb_request_data6,
        test_sync_block_nested_pb_call_complete6, test, NULL));
    GDEBUG("Peer B submitting call 7");
    g_assert(giorpc_peer_call(test->pb, TEST_IID, 7, test->pb_request_data7,
        test_sync_block_nested_pb_call_complete7, test, NULL));

    /* But this one must go through, it's not blocked */
    GDEBUG("Peer B submitting notify 2");
    giorpc_peer_notify(test->pb, TEST_IID, 2, test->pb_notify_data2);
    return G_SOURCE_REMOVE;
}

static
void
test_sync_block_nested()
{
    static const guint8 data1[] = { 1 };
    static const guint8 data2[] = { 2, 3 };
    static const guint8 data3[] = { 4, 5, 6 };
    static const guint8 data4[] = { 7, 8, 9, 10 };
    static const guint8 data5[] = { 11, 12, 13, 14, 15 };
    static const guint8 data6[] = { 16, 17, 18, 19, 20, 21 };
    static const guint8 data7[] = { 22, 23, 24, 25, 26, 27, 28 };
    static const guint8 data8[] = { 29, 30, 31, 32, 33, 34, 35, 36 };
    static const guint8 data9[] = { 37, 38, 39, 40, 41, 42, 43, 44, 45 };
    static const guint8 data10[] = { 46, 47, 48, 49, 50, 51, 52, 53, 54, 55 };
    TestThread* ta = test_thread_new();
    TestThread* tb = test_thread_new();
    TestSetup2 setup;
    TestSyncBlockNested test;
    gulong ida[5];
    gulong idb[2];

    test_setup2_init2(&setup, ta->context, tb->context);
    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;
    test.pa = setup.peer[0];
    test.pb = setup.peer[1];
    test.pa_request_data3 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data1));
    test.pa_request_result3 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data2));
    test.pa_request_data5 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data3));
    test.pa_request_result5 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data4));
    test.pb_notify_data1 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data5));
    test.pb_notify_data2 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data6));
    test.pb_notify_data4 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data7));
    test.pb_request_data6 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data8));
    test.pb_request_data7 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data9));
    test.pb_request_result7 = g_bytes_new_static(TEST_ARRAY_AND_SIZE(data10));

    /*
     * Peer A and peer B operate on threads A and B, respectively
     *
     * Test scenario:
     *
     * 1. Peer B submits notification 1, async calls 6, 7 and notification 2
     * 2. Peer A receives notification 1 and makes sync call 5 with
     *    GIORPC_SYNC_FLAG_BLOCK_REQUEST flag. Calls 6 and 7 get queued until
     *    completion of call 5.
     * 3. Peer A receives notification 2 (it's not blocked) and makes
     *    sync call 3 with flags GIORPC_SYNC_FLAG_BLOCK_ALL. Calls 6 and 7
     *    are still blocked.
     * 4. Peer B handles call 5 and submits notification 4 which gets blocked
     *    until completion of call 3
     * 5. Peer B handles call 3 and responds to it.
     * 6. Peer A receives notification 4
     * 7. Sync call 5 gets completed
     * 8. Peer A handles call 6
     * 9. Peer A handles call 7. That quits the event loop and terminates
     *    the test.
     */
    g_assert((ida[0] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        1, test_sync_block_nested_pa_notify1, &test)));
    g_assert((ida[1] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        2, test_sync_block_nested_pa_notify2, &test)));
    g_assert((ida[2] = giorpc_peer_add_notify_handler(test.pa, TEST_IID,
        4, test_sync_block_nested_pa_notify4, &test)));
    g_assert((ida[3] = giorpc_peer_add_request_handler(test.pa, TEST_IID,
        6, test_sync_block_nested_pa_call_handler6, &test)));
    g_assert((ida[4] = giorpc_peer_add_request_handler(test.pa, TEST_IID,
        7, test_sync_block_nested_pa_call_handler7, &test)));
    g_assert((idb[0] = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        3, test_sync_block_request_pb_call_handler3, &test)));
    g_assert((idb[1] = giorpc_peer_add_request_handler(test.pb, TEST_IID,
        5, test_sync_block_request_pb_call_handler5, &test)));
    giorpc_peer_start(test.pa); /* Peer B gets started implicitly */
    test_thread_invoke_later(tb, test_sync_block_nested_start, &test);
    test_run(&test_opt, setup.loop);

    GDEBUG("Done");
    g_assert_cmpint(test.last_code, == ,7);

    test_thread_free(ta);
    test_thread_free(tb);

    giorpc_peer_remove_all_handlers(test.pa, ida);
    giorpc_peer_remove_all_handlers(test.pb, idb);
    g_bytes_unref(test.pa_request_data3);
    g_bytes_unref(test.pa_request_result3);
    g_bytes_unref(test.pa_request_data5);
    g_bytes_unref(test.pa_request_result5);
    g_bytes_unref(test.pb_notify_data1);
    g_bytes_unref(test.pb_notify_data2);
    g_bytes_unref(test.pb_notify_data4);
    g_bytes_unref(test.pb_request_data6);
    g_bytes_unref(test.pb_request_data7);
    g_bytes_unref(test.pb_request_result7);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * defer/result/1
 * defer/result/2
 *==========================================================================*/

typedef struct test_defer_result {
    GThread* handler_thread;
    GMainContext* handler_context;
    GBytes* request_data;
    GBytes* response_data;
    GMainLoop* loop;
    GIoRpcRequest* req;
} TestDeferResult;

static
gboolean
test_defer_result_submit(
    gpointer user_data)
{
    TestDeferResult* test = user_data;

    GDEBUG("Submitting the response");
    g_assert(test->handler_thread != g_thread_self());
    g_assert(giorpc_request_complete(test->req, test->response_data));
    return G_SOURCE_REMOVE;
}

static
void
test_defer_result_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestDeferResult* test = user_data;

    GDEBUG("Request received");
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_CALL_CODE);
    g_assert(g_main_context_is_owner(test->handler_context));
    g_assert(test->handler_thread == g_thread_self());
    g_assert(g_bytes_equal(test->request_data, data));
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    /* Stash the request */
    g_assert(!test->req);
    test->req = giorpc_request_ref(req);

    /* Complete the request on the main thread */
    g_idle_add(test_defer_result_submit, test);
}

static
void
test_defer_result_done(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestDeferResult* test = user_data;

    GDEBUG("Done");
    g_assert(!error);
    g_assert(data);
    if (test->response_data) {
        g_assert(g_bytes_equal(test->response_data, data));
    } else {
        g_assert_cmpuint(g_bytes_get_size(data), == ,0);
    }
    test_quit_later(test->loop);
}

static
void
test_defer_result(
    GBytes* response_data)
{
    TestThread* thread = test_thread_new();
    TestDeferResult test;
    TestSetup2 setup;
    GIoRpcPeer* caller;
    GIoRpcPeer* callee;
    gulong id;
    static const guint8 in[] = { 0x01, 0x02 };

    test_setup2_init(&setup, thread->context);
    caller = setup.peer[0];
    callee = setup.peer[1];

    /*
     * test_defer_result_handler is invoked on the test thread
     * test_defer_result_submit is invoked on the main thread
     * test_defer_result_done is invoked again on the test thread
     */
    memset(&test, 0, sizeof(test));
    test.handler_thread = thread->thread;
    test.handler_context = thread->context;
    test.loop = setup.loop;
    test.request_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(in));
    test.response_data = response_data;

    /* Register the handler and submit the call */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        TEST_CALL_CODE, test_defer_result_handler, &test)));
    giorpc_peer_start(callee); /* Caller gets started automatically */
    g_assert_cmpuint(giorpc_peer_call(caller, TEST_IID, TEST_CALL_CODE,
        test.request_data, test_defer_result_done, &test, NULL), != ,0);

    /* Wait for the completion */
    test_run(&test_opt, setup.loop);

    giorpc_peer_remove_handler(callee, id);
    g_assert(test.req);

    /* The request wasn't cancelled */
    g_assert(!giorpc_request_is_cancelled(test.req));

    /* The request must have been responded to */
    g_assert(!giorpc_request_complete(test.req, NULL));

    test_setup2_cleanup(&setup);

    g_bytes_unref(test.request_data);
    giorpc_request_unref(test.req);
}

static
void
test_defer_result1()
{
    test_defer_result(NULL);
}

static
void
test_defer_result2()
{
    static const guint8 out[] = { 0x03, 0x04, 0x05 };
    GBytes* response_data = g_bytes_new_static(TEST_ARRAY_AND_SIZE(out));

    test_defer_result(response_data);
    g_bytes_unref(response_data);
}

/*==========================================================================*
 * defer/error
 *==========================================================================*/

typedef struct test_defer_error {
    GThread* handler_thread;
    GMainContext* handler_context;
    GMainLoop* loop;
    GIoRpcRequest* req;
    gboolean handled;
} TestDeferError;

static
gboolean
test_defer_error_drop_req(
    gpointer user_data)
{
    TestDeferError* test = user_data;

    GDEBUG("Dropping the response");
    g_assert(test->handler_thread != g_thread_self());
    giorpc_request_unref(test->req);
    test->req = NULL;
    return G_SOURCE_REMOVE;
}

void
test_defer_error_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestDeferError* test = user_data;

    GDEBUG("Request received");
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, == ,TEST_CALL_CODE);
    g_assert_cmpint(giorpc_request_flags(req) & (GIORPC_REQUEST_FLAG_ONE_WAY |
        GIORPC_REQUEST_FLAG_NO_RESULT_DATA), == , GIORPC_REQUEST_FLAGS_NONE);

    g_assert(g_main_context_is_owner(test->handler_context));
    g_assert(test->handler_thread == g_thread_self());

    /* Stash the request */
    g_assert(!test->req);
    g_assert(!test->handled);
    test->handled = TRUE;
    test->req = giorpc_request_ref(req);

    /* Complete the request on the main thread */
    g_idle_add(test_defer_error_drop_req, test);
}

static
void
test_defer_error_done(
    GIoRpcPeer* peer,
    GBytes* data,
    const GError* error,
    gpointer user_data)
{
    TestDeferError* test = user_data;

    GDEBUG("Done");
    g_assert(!data);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_UNHANDLED));
    test_quit_later(test->loop);
}

static
void
test_defer_error()
{
    TestThread* thread = test_thread_new();
    TestDeferError test;
    TestSetup2 setup;
    GIoRpcPeer* caller;
    GIoRpcPeer* callee;
    gulong id;

    test_setup2_init(&setup, thread->context);
    caller = setup.peer[0];
    callee = setup.peer[1];

    /*
     * test_defer_error_handler is invoked on the test thread
     * test_defer_error_drop_req is invoked on the main thread
     * test_defer_error_done is invoked again on the test thread
     */
    memset(&test, 0, sizeof(test));
    test.handler_thread = thread->thread;
    test.handler_context = thread->context;
    test.loop = setup.loop;

    /* Register the handler and submit the call */
    g_assert((id = giorpc_peer_add_request_handler(callee, GIORPC_IID_ANY,
        TEST_CALL_CODE, test_defer_error_handler, &test)));
    giorpc_peer_start(callee); /* Caller gets started automatically */
    g_assert_cmpuint(giorpc_peer_call(caller, TEST_IID, TEST_CALL_CODE, NULL,
        test_defer_error_done, &test, NULL), != ,0);

    /* Wait for the completion */
    test_run(&test_opt, setup.loop);

    giorpc_peer_remove_handler(callee, id);
    g_assert(test.handled);

    /* The request has been dropped */
    g_assert(!test.req);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * error
 *==========================================================================*/

static
void
test_error_cb(
    GIoRpcPeer* peer,
    gpointer fd)
{
    if (peer->state == GIORPC_PEER_STATE_STARTING) {
        GDEBUG("Closing fd");
        g_assert(!close(GPOINTER_TO_INT(fd)));
    }
}

static
void
test_error_completion(
    GIoRpcPeer* peer,
    GBytes* response,
    const GError* error,
    gpointer loop)
{
    g_assert(!response);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_ABANDONED));
    GDEBUG("%s", error->message);
    test_quit_later(loop);
}

static
void
test_error()
{
    int fd[2];
    GIOStream* stream;
    GIoRpcPeer* peer;
    GMainLoop* loop = g_main_loop_new(NULL, FALSE);
    guint call_id;
    gulong state_change_id;

    g_assert(!socketpair(AF_UNIX, SOCK_STREAM, 0, fd));
    stream = test_io_stream_new_fd(fd[0], FALSE);
    g_assert((peer = giorpc_peer_new(stream, NULL)));
    g_object_unref(stream);

    /* Close the other side of the pipe once we reach the STARTING state */
    state_change_id = giorpc_peer_add_state_handler(peer, test_error_cb,
        GINT_TO_POINTER(fd[1]));
    g_assert(state_change_id);

    /* Once START fails, the call will get completed with an error */
    call_id = giorpc_peer_call(peer, TEST_IID, 1, NULL,
        test_error_completion, loop, NULL);
    g_assert(call_id);

    test_run(&test_opt, loop);

    g_assert(!giorpc_peer_cancel(peer, call_id)); /* Abandoned */
    giorpc_peer_remove_handler(peer, state_change_id);
    giorpc_peer_unref(peer);
    g_main_loop_unref(loop);
    g_assert(!close(fd[0]));
    /* The other fd has already been closed by test_error_cb */
}

/*==========================================================================*
 * unknown_packet
 *==========================================================================*/

static
void
test_unknown_packet()
{
    TestSetup1 test;
    GCancellable* cancel;
    static const guint8 start[] = { START_PACKET() };
    static const guint8 data[] = {
        0x7f, 0x81, 0x00,
        0x11, 0x12, 0x13, 0x13, 0x15, 0x16, 0x17, 0x18,
        0x21, 0x22, 0x23, 0x23, 0x25, 0x26, 0x27, 0x28,
        0x31, 0x32, 0x33, 0x33, 0x35, 0x36, 0x37, 0x38,
        0x41, 0x42, 0x43, 0x43, 0x45, 0x46, 0x47, 0x48,
        0x51, 0x52, 0x53, 0x53, 0x55, 0x56, 0x57, 0x58,
        0x61, 0x62, 0x63, 0x63, 0x65, 0x66, 0x67, 0x68,
        0x71, 0x72, 0x73, 0x73, 0x75, 0x76, 0x77, 0x78,
        0x81, 0x82, 0x83, 0x83, 0x85, 0x86, 0x87, 0x88,
        0x11, 0x12, 0x13, 0x13, 0x15, 0x16, 0x17, 0x18,
        0x21, 0x22, 0x23, 0x23, 0x25, 0x26, 0x27, 0x28,
        0x31, 0x32, 0x33, 0x33, 0x35, 0x36, 0x37, 0x38,
        0x41, 0x42, 0x43, 0x43, 0x45, 0x46, 0x47, 0x48,
        0x51, 0x52, 0x53, 0x53, 0x55, 0x56, 0x57, 0x58,
        0x61, 0x62, 0x63, 0x63, 0x65, 0x66, 0x67, 0x68,
        0x71, 0x72, 0x73, 0x73, 0x75, 0x76, 0x77, 0x78,
        0x81, 0x82, 0x83, 0x83, 0x85, 0x86, 0x87, 0x88,
        START_PACKET_(GIORPC_PROTOCOL_VERSION + 1)
    };
    GBytes* start_packet = g_bytes_new_static(start, sizeof(start));

    test_setup1_init(&test);

    /* Setup the reader */
    cancel = test_reader(test.fd, NULL, start_packet, test.loop);
    g_bytes_unref(start_packet);

    /* Send the data to the peer, an unknown packet followed by START */
    g_assert_cmpint(write(test.fd, data, sizeof(data)), == ,sizeof(data));

    /* Start the peer */
    giorpc_peer_start(test.peer);
    test_run(&test_opt, test.loop);

    /* We may still have to wait until the peer starts */
    if (test.peer->state < GIORPC_PEER_STATE_STARTED) {
        gulong id = giorpc_peer_add_state_handler(test.peer,
            test_exit_loop_when_started, test.loop);

        GDEBUG("Waiting for peer to start");
        test_run(&test_opt, test.loop);
        giorpc_peer_remove_handler(test.peer, id);
    }

    g_assert_cmpint(test.peer->state, == ,GIORPC_PEER_STATE_STARTED);
    g_assert_cmpint(test.peer->version, == ,GIORPC_PROTOCOL_VERSION + 1);

    g_cancellable_cancel(cancel);
    g_object_unref(cancel);
    test_setup1_cleanup(&test);
}

/*==========================================================================*
 * cancel/1
 *==========================================================================*/

typedef struct test_cancel1 {
    GMainLoop* loop;
    gboolean completed;
    int destroyed;
} TestCancel1;

static
void
test_cancel1_completion(
    GIoRpcPeer* peer,
    GBytes* response,
    const GError* error,
    gpointer user_data)
{
    TestCancel1* test = user_data;

    g_assert(!response);
    g_assert(!test->completed);
    g_assert(g_error_matches(error, GIORPC_ERROR, GIORPC_ERROR_ABANDONED));
    GDEBUG("%s", error->message);
    test->completed = TRUE;
    test_quit_later(test->loop);
}

static
void
test_cancel1_done(
    gpointer user_data)
{
    TestCancel1* test = user_data;

    g_assert_cmpint(test->destroyed, < ,2);
    test->destroyed++;
    GDEBUG("Request #%d fone", test->destroyed);
    if (test->destroyed == 2) {
        test_quit_later(test->loop);
    }
}

static
void
test_cancel1()
{
    TestSetup1 setup;
    TestCancel1 test;
    GIoRpcPeer* peer;
    GCancellable* cancel;
    static const guint8 start[] = { START_PACKET() };
    static const guint8 finish[] = { PACKET_FINISH, 0x00 };
    gulong handler_id;
    guint call_id;

    test_setup1_init(&setup);
    peer = setup.peer;

    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;

    /* Setup the reader */
    cancel = test_null_reader(setup.fd);

    /* Send the START packet to the peer */
    g_assert_cmpint(write(setup.fd, start, sizeof(start)), == ,sizeof(start));

    /* Wait for the peer to start */
    handler_id = giorpc_peer_add_state_handler(peer,
        test_exit_loop_when_started, setup.loop);
    giorpc_peer_start(peer);
    GDEBUG("Waiting for peer to start");
    test_run(&test_opt, setup.loop);
    g_assert_cmpint(peer->state, == ,GIORPC_PEER_STATE_STARTED);
    g_assert_cmpint(peer->version, == ,GIORPC_PROTOCOL_VERSION);
    giorpc_peer_remove_handler(peer, handler_id);

    /* Now submit the requests */
    call_id = giorpc_peer_call(peer, TEST_IID, 1, NULL,
        test_cancel1_completion, &test, test_cancel1_done);
    g_assert(call_id);
    call_id = giorpc_peer_call(peer, TEST_IID, 1, NULL, NULL,
        &test, test_cancel1_done);
    g_assert(call_id);

    /* FINISH terminates pending requests */
    g_assert_cmpint(write(setup.fd, finish, sizeof(finish)),==,sizeof(finish));
    GDEBUG("Waiting for requests to be cancelled");
    test_run(&test_opt, setup.loop);

    g_assert_cmpint(peer->state, == ,GIORPC_PEER_STATE_DONE);
    g_assert(test.completed);

    test_setup1_cleanup(&setup);
    g_cancellable_cancel(cancel);
    g_object_unref(cancel);
}

/*==========================================================================*
 * cancel/2
 *==========================================================================*/

typedef struct test_cancel2 {
    GIoRpcPeer* peer;
    guint id;
} TestCancel2;

static
TestCancel2*
test_cancel2_data_new(
    GIoRpcPeer* peer,
    guint id)
{
    TestCancel2* cancel = g_new(TestCancel2, 1);

    cancel->peer = peer;
    cancel->id = id;
    return cancel;
}

static
gboolean
test_cancel2_cb(
    gpointer data)
{
    TestCancel2* cancel = data;

     /* It works only once */
    g_assert(giorpc_peer_cancel(cancel->peer, cancel->id));
    g_assert(!giorpc_peer_cancel(cancel->peer, cancel->id));
    g_free(data);
    return G_SOURCE_REMOVE;
}

static
void
test_cancel2()
{
    TestSetup1 test;
    GCancellable* cancel;
    static const guint8 start[] = { START_PACKET() };
    static const guint8 finish[] = { PACKET_FINISH, 0x00 };
    static const guint8 req_data[] = {
        0x11, 0x12, 0x13, 0x13, 0x15, 0x16, 0x17, 0x18,
        0x21, 0x22, 0x23, 0x23, 0x25, 0x26, 0x27, 0x28,
        0x31, 0x32, 0x33, 0x33, 0x35, 0x36, 0x37, 0x38,
        0x41, 0x42, 0x43, 0x43, 0x45, 0x46, 0x47, 0x48,
        0x51, 0x52, 0x53, 0x53, 0x55, 0x56, 0x57, 0x58,
        0x61, 0x62, 0x63, 0x63, 0x65, 0x66, 0x67, 0x68,
        0x71, 0x72, 0x73, 0x73, 0x75, 0x76, 0x77, 0x78,
        0x81, 0x82, 0x83, 0x83, 0x85, 0x86, 0x87, 0x88
    };
    GBytes* req_payload = g_bytes_new_static(req_data, sizeof(req_data));
    gulong handler_id;
    guint call_id;

    test_setup1_init(&test);

    /* Setup the reader */
    cancel = test_reader(test.fd, NULL, req_payload, test.loop);

    /* Send the START packet to the peer */
    g_assert_cmpint(write(test.fd, start, sizeof(start)), == ,sizeof(start));

    /* Wait for the peer to start. The pattern won't match yet */
    handler_id = giorpc_peer_add_state_handler(test.peer,
        test_exit_loop_when_started, test.loop);
    giorpc_peer_start(test.peer);
    GDEBUG("Waiting for peer to start");
    test_run(&test_opt, test.loop);
    g_assert_cmpint(test.peer->state, == ,GIORPC_PEER_STATE_STARTED);
    g_assert_cmpint(test.peer->version, == ,GIORPC_PROTOCOL_VERSION);
    giorpc_peer_remove_handler(test.peer, handler_id);

    /* Now submit the request */
    call_id = giorpc_peer_call(test.peer, TEST_IID, 1, req_payload,
        test_no_response, test.loop, test_exit_loop);
    g_bytes_unref(req_payload);
    g_assert(call_id);

    /* Wait for the request to actually get sent */
    GDEBUG("Waiting for request to be sent");
    test_run(&test_opt, test.loop);

    /* Cancel the request and wait for it to finish (without response) */
    GDEBUG("Waiting for request to get cancelled");
    g_idle_add(test_cancel2_cb, test_cancel2_data_new(test.peer, call_id));
    test_run(&test_opt, test.loop);

    /* Send FINISH to stop the peer */
    g_assert_cmpint(write(test.fd, finish, sizeof(finish)),==,sizeof(finish));
    GDEBUG("Waiting for peer to stop");
    handler_id = giorpc_peer_add_state_handler(test.peer,
        test_exit_loop_when_done, test.loop);
    test_run(&test_opt, test.loop);
    giorpc_peer_remove_handler(test.peer, handler_id);

    g_assert_cmpint(test.peer->state, == ,GIORPC_PEER_STATE_DONE);

    test_setup1_cleanup(&test);
    g_cancellable_cancel(cancel);
    g_object_unref(cancel);
}

/*==========================================================================*
 * cancel/3
 *==========================================================================*/

typedef enum test_cancel3_req {
    TEST_CANCEL3_REQ_DROP,
    TEST_CANCEL3_REQ_UNREF,
    TEST_CANCEL3_REQ_KEEP,
    TEST_CANCEL3_REQ_COUNT
} TEST_CANCEL3_REQ;

typedef struct test_cancel3_data {
    GMainLoop* loop;
    GIoRpcRequest* req[TEST_CANCEL3_REQ_COUNT];
    gulong cancel_id[TEST_CANCEL3_REQ_COUNT];
    int cancel_count;
} TestCancel3;

static
void
test_cancel3_req_cb(
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestCancel3* test = user_data;

    GDEBUG("Received cancel notification for request %06u",
        giorpc_request_id(req));
    g_assert(giorpc_request_is_cancelled(req));
    /* Only TEST_CANCEL3_REQ_KEEP will get here */
    g_assert(test->req[TEST_CANCEL3_REQ_KEEP] == req);
    g_assert_cmpint(test->cancel_count, == ,0);
    test->cancel_count++;
}

static
void
test_cancel3_cb(
    GIoRpcPeer* peer,
    guint id,
    gpointer user_data)
{
    TestCancel3* test = user_data;
    int i;

    GDEBUG("Request %06u is cancelled", id);
    for (i = 0; i < TEST_CANCEL3_REQ_COUNT; i++) {
        GIoRpcRequest* req = test->req[i];

        if (giorpc_request_id(req) == id) {
            switch ((TEST_CANCEL3_REQ)i) {
            case TEST_CANCEL3_REQ_DROP:
                giorpc_request_drop(req);
                test->req[i] = NULL;
                return;
            case TEST_CANCEL3_REQ_UNREF:
                giorpc_request_unref(req);
                test->req[i] = NULL;
                return;
            case TEST_CANCEL3_REQ_KEEP:
                test_quit_later(test->loop);
                return;
            case TEST_CANCEL3_REQ_COUNT:
                g_assert_not_reached();
                break;
            }
        }
    }
    g_assert_not_reached();
}

void
test_cancel3_handler(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* payload,
    GIoRpcRequest* req,
    gpointer user_data)
{
    TestCancel3* test = user_data;
    const int req_index = code - 1;

    GDEBUG("Request %u/%u:%06x received", iid, code, giorpc_request_id(req));
    g_assert_cmpuint(giorpc_request_id(req), != ,0);
    g_assert_cmpuint(iid, == ,TEST_IID);
    g_assert_cmpuint(code, > ,0);
    g_assert_cmpuint(code, <= ,TEST_CANCEL3_REQ_COUNT);

    /* Stash the request */
    g_assert(!test->req[req_index]);
    test->req[req_index] = giorpc_request_ref(req);

    /* NULL handler doesn't get registered */
    g_assert(!giorpc_request_add_cancel_handler(req, NULL, test));

    /* Register the request-specific cancel handler */
    g_assert_cmpuint(test->cancel_id[req_index], == ,0);
    test->cancel_id[req_index] = giorpc_request_add_cancel_handler(req,
        test_cancel3_req_cb, test);
    g_assert_cmpuint(test->cancel_id[req_index], != ,0);

    /* The last quits the loop */
    if (code == TEST_CANCEL3_REQ_COUNT) {
        test_quit_later(test->loop);
    }
}

static
void
test_cancel3()
{
    TestSetup2 setup;
    TestCancel3 test;
    GIoRpcPeer* callee;
    GIoRpcPeer* caller;
    gulong handler_id, cancel_id;
    guint call_id[3];
    int i;

    test_setup2_init(&setup, NULL);
    callee = setup.peer[0];
    caller = setup.peer[1];

    memset(&test, 0, sizeof(test));
    test.loop = setup.loop;

    g_assert((handler_id = giorpc_peer_add_request_handler(callee,
        GIORPC_IID_ANY, GIORPC_CODE_ANY, test_cancel3_handler, &test)));
    g_assert((cancel_id = giorpc_peer_add_cancel_handler(callee,
        test_cancel3_cb, &test)));
    for (i = 0; i < TEST_CANCEL3_REQ_COUNT; i++) {
        g_assert((call_id[i] = giorpc_peer_call(caller, TEST_IID, i + 1,
            NULL, test_response_unreached, NULL, NULL)));
    }

    /* Wait for all requests to be delivered */
    giorpc_peer_start(callee);
    test_run(&test_opt, setup.loop);
    for (i = 0; i < TEST_CANCEL3_REQ_COUNT; i++) {
        g_assert(test.req[i]);
    }

    /* Cancel pending requests */
    GDEBUG("All requests have been delivered, cancelling");
    for (i = 0; i < TEST_CANCEL3_REQ_COUNT; i++) {
        g_assert(giorpc_peer_cancel(caller, call_id[i]));
        g_assert(!giorpc_peer_cancel(caller, call_id[i]));
    }
    test_run(&test_opt, setup.loop);

    g_assert(!test.req[TEST_CANCEL3_REQ_DROP]);
    g_assert(!test.req[TEST_CANCEL3_REQ_UNREF]);
    g_assert(test.req[TEST_CANCEL3_REQ_KEEP]);

    /* All cancel requests have been registered */
    for (i = 0; i < TEST_CANCEL3_REQ_COUNT; i++) {
        g_assert_cmpuint(test.cancel_id[i], != , 0);
        giorpc_request_remove_cancel_handler(test.req[i], 0); /* noop */
        giorpc_request_remove_cancel_handler(test.req[i], test.cancel_id[i]);
    }

    /* But only TEST_CANCEL3_REQ_KEEP reaches test_cancel3_req_cb */
    g_assert_cmpuint(test.cancel_count, == ,1);
    giorpc_request_unref(test.req[TEST_CANCEL3_REQ_KEEP]);

    giorpc_peer_remove_handler(callee, handler_id);
    giorpc_peer_remove_handler(callee, cancel_id);
    test_setup2_cleanup(&setup);
}

/*==========================================================================*
 * broken_stream
 *==========================================================================*/

typedef struct test_broken_stream {
    const char* name;
    GUtilData packet;
} TestBrokenStream;

static const guint8 broken_stream1_data[] = { /* Broken START */
    PACKET_START, 0x80, 0x80, 0x80
};

static const guint8 broken_stream2_data[] = { /* Broken START */
    PACKET_START, 0x00 /* No data at all */
};

static const guint8 broken_stream3_data[] = { /* Broken START */
    PACKET_START, 0x01, 0x00 /* Missing version TLV */
};

static const guint8 broken_stream4_data[] = { /* Broken START */
    PACKET_START, 0x04,
    0x01, GIORPC_START_VERSION_TAG, 0x01, 0x00 /* Zero version */
};

static const guint8 broken_stream5_data[] = { /* Broken START */
    PACKET_START, 0x08, /* Version too large */
    0x01, GIORPC_START_VERSION_TAG, 0x05, 0xff, 0xff, 0xff, 0xff, 0x7f
};

static const guint8 broken_stream6_data[] = { /* Broken START */
    PACKET_START, 0x05,
    0x01, GIORPC_START_VERSION_TAG, 0x01, 0x01 /* Version */,
    0xff /* Trailing garbage */
};

static const guint8 broken_stream7_data[] = { /* Double start */
    START_PACKET(),
    START_PACKET()
};

static const guint8 broken_stream8_data[] = { /* NOTIFY before START */
    START_PACKET(),
    START_PACKET()
};

static const guint8 broken_stream9_data[] = { /* Broken NOTIFY */
    PACKET_NOTIFY, 0x02, 0x01, 0x01,
    PACKET_NOTIFY, 0x02, 0x00, 0x01 /* Zero IID */
};

static const guint8 broken_stream10_data[] = { /* Broken NOTIFY */
    START_PACKET(),
    PACKET_NOTIFY, 0x02, 0x01, 0x00 /* Zero CODE */
};

static const guint8 broken_stream11_data[] = { /* Broken NOTIFY */
    START_PACKET(),
    PACKET_NOTIFY, 0x06, 0xff, 0xff, 0xff, 0xff, 0x7f /* Invalid IID */, 0x01
};

static const guint8 broken_stream12_data[] = { /* Broken NOTIFY */
    START_PACKET(),
    PACKET_NOTIFY, 0x06, 0x01, 0xff, 0xff, 0xff, 0xff, 0x7f /* Invalid CODE */
};

static const guint8 broken_stream13_data[] = { /* Broken NOTIFY */
    START_PACKET(),
    PACKET_NOTIFY, 0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
};

static const guint8 broken_stream14_data[] = { /* Broken NOTIFY */
    START_PACKET(),
    PACKET_NOTIFY, 0x08, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
};

static const guint8 broken_stream15_data[] = { /* REQUEST before START */
    PACKET_REQUEST, 0x04, 0x01, 0x02, 0x03, 0x00,
    START_PACKET()
};

static const guint8 broken_stream16_data[] = { /* Broken REQUEST */
    START_PACKET(),
    PACKET_REQUEST, 0x01, 0x80 /* Truncated IID */
};

static const guint8 broken_stream17_data[] = { /* Broken REQUEST */
    START_PACKET(),
    PACKET_REQUEST, 0x02, 0x01, 0x80 /* Truncated CODE */
};

static const guint8 broken_stream18_data[] = { /* Broken REQUEST */
    START_PACKET(),
    PACKET_REQUEST, 0x03, 0x01, 0x02, 0x80 /* Truncated ID */
};

static const guint8 broken_stream19_data[] = { /* Broken REQUEST */
    START_PACKET(),
    PACKET_REQUEST, 0x04, 0x01, 0x02, 0x03, 0x80 /* Truncated FLAGS */
};

static const guint8 broken_stream20_data[] = { /* Broken REQUEST */
    START_PACKET(),
    PACKET_REQUEST, 0x04, 0x00, /* Zero IID */
    0x01, 0x01, 0x00
};

static const guint8 broken_stream21_data[] = { /* Broken REQUEST */
    START_PACKET(),
    PACKET_REQUEST, 0x04, 0x01, 0x00, /* Zero CODE */
    0x01, 0x00
};

static const guint8 broken_stream22_data[] = { /* Broken REQUST */
    START_PACKET(),
    PACKET_REQUEST, 0x07, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x01, /* Invalid CODE */
    0x00
};

static const guint8 broken_stream23_data[] = { /* Broken REQUEST */
    START_PACKET(),
    PACKET_REQUEST, 0x08, 0x01, 0x02,
    0x81, 0xff, 0xff, 0xff, 0x7f, /* Invalid ID */
    0x00
};

static const guint8 broken_stream24_data[] = { /* RESULT before START */
    PACKET_RESULT, 0x02, 0x03, 0x04
};

static const guint8 broken_stream25_data[] = { /* Broken RESULT */
    START_PACKET(),
    PACKET_RESULT, 0x01, 0x80 /* Truncated ID */
};

static const guint8 broken_stream26_data[] = { /* Broken RESULT */
    START_PACKET(),
    PACKET_RESULT, 0x05, 0x81, 0xff, 0xff, 0xff, 0x7f /* Invalid ID */
};

static const guint8 broken_stream27_data[] = { /* Unexpected RESULT */
    START_PACKET(),
    PACKET_RESULT, 0x01, 0x01, /* RESULT for a non-existent request  */
    PACKET_FINISH, 0x00
};

static const guint8 broken_stream28_data[] = { /* ERROR before START */
    PACKET_ERROR, 0x02, 0x01, 0x01
};

static const guint8 broken_stream29_data[] = { /* Broken ERROR */
    START_PACKET(),
    PACKET_ERROR, 0x01, 0x80 /* Truncated ID */
};

static const guint8 broken_stream30_data[] = { /* Broken ERROR */
    START_PACKET(),
    PACKET_ERROR, 0x06, 0x81, 0xff, 0xff, 0xff, 0x7f, 0x01 /* Invalid ID */
};

static const guint8 broken_stream31_data[] = { /* Broken ERROR */
    START_PACKET(),
    PACKET_ERROR, 0x02, 0x01, 0x80 /* Truncated ERROR CODE */
};

static const guint8 broken_stream32_data[] = { /* Broken ERROR */
    START_PACKET(),
    PACKET_ERROR, 0x01, 0xff,  /* Truncated ID */
};

static const guint8 broken_stream33_data[] = { /* Unexpected ERROR */
    START_PACKET(),
    PACKET_ERROR, 0x02, 0x01, 0x01, /* ERROR for a non-existent request  */
    PACKET_FINISH, 0x00
};

static const guint8 broken_stream34_data[] = { /* CANCEL before START */
    PACKET_CANCEL, 0x01, 0x01
};

static const guint8 broken_stream35_data[] = { /* Broken CANCEL */
    START_PACKET(),
    PACKET_CANCEL, 0x01, 0x80 /* Truncated ID */
};

static const guint8 broken_stream36_data[] = { /* Broken CANCEL */
    START_PACKET(),
    PACKET_CANCEL, 0x05, 0x81, 0xff, 0xff, 0xff, 0x7f /* Invalid ID */
};

static const TestBrokenStream broken_stream_tests[] = {
    #define BROKEN_STREAM_TEST_(x) { TEST_("broken_stream/") #x, \
        { TEST_ARRAY_AND_SIZE(broken_stream##x##_data) } }
    BROKEN_STREAM_TEST_(1),
    BROKEN_STREAM_TEST_(2),
    BROKEN_STREAM_TEST_(3),
    BROKEN_STREAM_TEST_(4),
    BROKEN_STREAM_TEST_(5),
    BROKEN_STREAM_TEST_(6),
    BROKEN_STREAM_TEST_(7),
    BROKEN_STREAM_TEST_(8),
    BROKEN_STREAM_TEST_(9),
    BROKEN_STREAM_TEST_(10),
    BROKEN_STREAM_TEST_(11),
    BROKEN_STREAM_TEST_(12),
    BROKEN_STREAM_TEST_(13),
    BROKEN_STREAM_TEST_(14),
    BROKEN_STREAM_TEST_(15),
    BROKEN_STREAM_TEST_(16),
    BROKEN_STREAM_TEST_(17),
    BROKEN_STREAM_TEST_(18),
    BROKEN_STREAM_TEST_(19),
    BROKEN_STREAM_TEST_(20),
    BROKEN_STREAM_TEST_(21),
    BROKEN_STREAM_TEST_(22),
    BROKEN_STREAM_TEST_(23),
    BROKEN_STREAM_TEST_(24),
    BROKEN_STREAM_TEST_(25),
    BROKEN_STREAM_TEST_(26),
    BROKEN_STREAM_TEST_(27),
    BROKEN_STREAM_TEST_(28),
    BROKEN_STREAM_TEST_(29),
    BROKEN_STREAM_TEST_(30),
    BROKEN_STREAM_TEST_(31),
    BROKEN_STREAM_TEST_(32),
    BROKEN_STREAM_TEST_(33),
    BROKEN_STREAM_TEST_(34),
    BROKEN_STREAM_TEST_(35),
    BROKEN_STREAM_TEST_(36)
    #undef BROKEN_STREAM_TEST_
};

static
void
test_broken_stream(
    gconstpointer data)
{
    const TestBrokenStream* test = data;
    const GUtilData* packet = &test->packet;
    int fd[2];
    GIOStream* stream;
    GIoRpcPeer* peer;
    GCancellable* cancel;
    GMainLoop* loop = g_main_loop_new(NULL, FALSE);
    gulong id;

    g_assert(!socketpair(AF_UNIX, SOCK_STREAM, 0, fd));
    stream = test_io_stream_new_fd(fd[0], FALSE);
    cancel = test_null_reader(fd[1]);

    g_assert((peer = giorpc_peer_new(stream, NULL)));
    g_object_unref(stream);

    id = giorpc_peer_add_state_handler(peer, test_exit_loop_when_done, loop);
    g_assert(id);

    /* Start the reader and send a broken packet to it */
    giorpc_peer_start(peer);
    g_assert_cmpint(write(fd[1], packet->bytes, packet->size), == ,
        packet->size);
    test_run(&test_opt, loop);

    /* Shut down the reader */
    g_cancellable_cancel(cancel);
    test_quit_later_n(loop, 10);
    test_run(&test_opt, loop);
    g_object_unref(cancel);

    giorpc_peer_remove_handler(peer, id);
    giorpc_peer_unref(peer);
    g_main_loop_unref(loop);
    g_assert(!close(fd[0]));
    g_assert(!close(fd[1]));
}

/*==========================================================================*
 * Common
 *==========================================================================*/

int main(int argc, char* argv[])
{
    int i;

    signal(SIGPIPE, SIG_IGN);
    g_test_init(&argc, &argv, NULL);
    test_init(&test_opt, argc, argv);
    g_test_add_func(TEST_("null"), test_null);
    g_test_add_func(TEST_("basic/1"), test_basic1);
    g_test_add_func(TEST_("basic/2"), test_basic2);
    g_test_add_func(TEST_("basic/3"), test_basic3);
    g_test_add_func(TEST_("stop"), test_stop);
    g_test_add_func(TEST_("shutdown"), test_shutdown);
    g_test_add_func(TEST_("notify/main"), test_notify_main);
    g_test_add_func(TEST_("notify/thread"), test_notify_thread);
    g_test_add_func(TEST_("error"), test_error);
    g_test_add_func(TEST_("unhandled/1"), test_unhandled1);
    g_test_add_func(TEST_("unhandled/2"), test_unhandled2);
    g_test_add_func(TEST_("unhandled/3"), test_unhandled3);
    g_test_add_func(TEST_("unhandled/4"), test_unhandled4);
    g_test_add_func(TEST_("unhandled/5"), test_unhandled5);
    g_test_add_func(TEST_("response/main"), test_response_main);
    g_test_add_func(TEST_("response/thread"), test_response_thread);
    g_test_add_func(TEST_("completion/main"), test_completion_main);
    g_test_add_func(TEST_("completion/thread"), test_completion_thread);
    g_test_add_func(TEST_("calls"), test_calls);
    g_test_add_func(TEST_("sync/basic"), test_sync_basic);
    g_test_add_func(TEST_("sync/cancel"), test_sync_cancel);
    g_test_add_func(TEST_("sync/timeout"), test_sync_timeout);
    g_test_add_func(TEST_("sync/unhandled"), test_sync_unhandled);
    g_test_add_func(TEST_("sync/block/none"), test_sync_block_none);
    g_test_add_func(TEST_("sync/block/request"), test_sync_block_request);
    g_test_add_func(TEST_("sync/block/response"), test_sync_block_response);
    g_test_add_func(TEST_("sync/block/notify"), test_sync_block_notify);
    g_test_add_func(TEST_("sync/block/nested"), test_sync_block_nested);
    g_test_add_func(TEST_("defer/result/1"), test_defer_result1);
    g_test_add_func(TEST_("defer/result/2"), test_defer_result2);
    g_test_add_func(TEST_("defer/error"), test_defer_error);
    g_test_add_func(TEST_("cancel/1"), test_cancel1);
    g_test_add_func(TEST_("cancel/2"), test_cancel2);
    g_test_add_func(TEST_("cancel/3"), test_cancel3);
    g_test_add_func(TEST_("unknown_packet"), test_unknown_packet);
    for (i = 0; i < G_N_ELEMENTS(broken_stream_tests); i++) {
        g_test_add_data_func(broken_stream_tests[i].name,
            broken_stream_tests + i, test_broken_stream);
    }
    return g_test_run();
}

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
