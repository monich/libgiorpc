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

#define _GNU_SOURCE /* For pipe2 */

#include "test_common.h"

#include "giorpc_util_p.h"
#include "giorpc_log_p.h"
#include "giorpc_version.h"

#include <gio/gunixinputstream.h>
#include <gio/gunixoutputstream.h>

#include <unistd.h>
#include <fcntl.h>

static TestOpt test_opt;

#define TEST_(name) "/giorpc/util/" name

/*==========================================================================*
 * version
 *==========================================================================*/

static
void
test_version()
{
    const guint v = giorpc_version();

    g_assert_cmpuint(v, == ,GIORPC_VERSION);
    g_assert_cmpuint(GIORPC_VERSION_GET_MAJOR(v), == ,GIORPC_VERSION_MAJOR);
    g_assert_cmpuint(GIORPC_VERSION_GET_MINOR(v), == ,GIORPC_VERSION_MINOR);
    g_assert_cmpuint(GIORPC_VERSION_GET_RELEASE(v), == ,GIORPC_VERSION_RELEASE);
}

/*==========================================================================*
 * tlv_array
 *==========================================================================*/

typedef struct {
    const char* name;
    GUtilData input;
    const guint* tags;
    gboolean result;
    const GUtilData* values;
} TestTlvArray;

static const guint8 test_tlv_array_input_empty[] = { 0 };
static const guint test_tlv_array_tags_1[] = { 1, 0 };
static const guint test_tlv_array_tags_1_2[] = { 1, 2, 0 };
static const guint8 test_tlv_array_input_dup[] = {
    0x02,
    0x01, 0x02, 0x03, 0x04,
    0x01, 0x00
};
static const guint8 test_tlv_array_input_tags_x[] = {
    0x01,
    0x7f, 0x00
};
static const guint8 test_tlv_array_input_tags_2_x[] = {
    0x02,
    0x02, 0x03, 0x04, 0x05, 0x06,
    0x7f, 0x00
};
static const GUtilData test_tlv_array_output_1_empty[] = { { NULL, 0 } };
static const guint8 test_tlv_array_output_2_data[] = { 0x04, 0x05, 0x06 };
static const GUtilData test_tlv_array_output_x_2[] = {
    { NULL, 0 },
    { TEST_ARRAY_AND_SIZE(test_tlv_array_output_2_data) }
};
static const guint8 test_tlv_array_input_garbage1[] = {
    0xff
};
static const guint8 test_tlv_array_input_garbage2[] = {
    0x01, 0x02, 0x03, 0x04, 0x05
};

static const TestTlvArray test_tlv_array_data[] = {
#define TEST_CASE(x) TEST_("tlv_array/") x
    {
        TEST_CASE("empty"),
        { TEST_ARRAY_AND_SIZE(test_tlv_array_input_empty) },
        test_tlv_array_tags_1, TRUE, test_tlv_array_output_1_empty
    },{
        TEST_CASE("dup"),
        { TEST_ARRAY_AND_SIZE(test_tlv_array_input_dup) },
        test_tlv_array_tags_1, FALSE, NULL
    },{
        TEST_CASE("unknown_tag/1"),
        { TEST_ARRAY_AND_SIZE(test_tlv_array_input_tags_x) },
        test_tlv_array_tags_1, TRUE, test_tlv_array_output_1_empty
    },{
        TEST_CASE("unknown_tag/2"),
        { TEST_ARRAY_AND_SIZE(test_tlv_array_input_tags_2_x) },
        test_tlv_array_tags_1_2, TRUE, test_tlv_array_output_x_2
    },{
        TEST_CASE("garbage/1"),
        { TEST_ARRAY_AND_SIZE(test_tlv_array_input_garbage1) },
        test_tlv_array_tags_1, FALSE, NULL
    },{
        TEST_CASE("garbage/2"),
        { TEST_ARRAY_AND_SIZE(test_tlv_array_input_garbage2) },
        test_tlv_array_tags_1, FALSE, NULL
    }
#undef TEST_CASE
};

static
void
test_tlv_array(
    gconstpointer data)
{
    const TestTlvArray* test = data;
    const guint* tag = test->tags;
    GUtilData* vals = NULL;
    GUtilRange in;
    int n = 0;

    while (*tag++) n++;
    vals = g_new(GUtilData, n);
    memset(vals, 0xaa, sizeof(vals[0]) * n);

    in.end = (in.ptr = test->input.bytes) + test->input.size;
    g_assert_cmpint(giorpc_tlv_array_decode(&in, test->tags, vals),
        ==, test->result);

    if (test->result) {
        int i;

        g_assert(in.ptr == in.end);
        for (i = 0; i < n; i++) {
            const GUtilData* expected = test->values + i;

            g_assert_cmpuint(vals[i].size, == ,expected->size);
            if (expected->size) {
                g_assert(!memcmp(vals[i].bytes, test->values[i].bytes,
                    expected->size));
            }
        }
    } else {
        g_assert(in.ptr == test->input.bytes);
    }
    g_free(vals);
}

/*==========================================================================*
 * read_write
 *==========================================================================*/

typedef struct {
    GMainLoop* loop;
    gsize chunk_size;
    gsize out_total;
    gsize in_total;
} TestReadWrite;

static
void
test_read_write_1(
    GInputStream* in,
    gsize nbytes,
    const GError* error,
    gpointer user_data)
{
    TestReadWrite* test = user_data;

    g_assert(!error);
    GDEBUG("%u bytes in", (guint) nbytes);
    g_assert_cmpuint(nbytes, == ,test->chunk_size);
    test->in_total += nbytes;
    g_main_loop_quit(test->loop);
}

static
void
test_read_write_2(
    GOutputStream* out,
    const GError* error,
    gpointer user_data)
{
    TestReadWrite* test = user_data;

    g_assert(!error);
    GDEBUG("Write #1 done");
    test->out_total += 2 * test->chunk_size;
    g_main_loop_quit(test->loop);
}

static
void
test_read_write_3(
    GInputStream* in,
    gsize nbytes,
    const GError* error,
    gpointer user_data)
{
    TestReadWrite* test = user_data;

    g_assert(!error);
    GDEBUG("%u bytes in", (guint) nbytes);
    g_assert_cmpuint(nbytes, == ,2 * test->chunk_size);
    test->in_total += nbytes;
    if (test->in_total == test->out_total) {
        /* Quit the loop when everyting has been written and read */
        g_main_loop_quit(test->loop);
    }
}

static
void
test_read_write_4(
    GOutputStream* out,
    const GError* error,
    gpointer user_data)
{
    TestReadWrite* test = user_data;

    g_assert(!error);
    GDEBUG("Write #2 done");
    test->out_total += test->chunk_size;
    if (test->in_total == test->out_total) {
        /* Quit the loop when everyting has been written and read */
        g_main_loop_quit(test->loop);
    }
}

static
void
test_read_write()
{
    const gsize chunk_size = 100000;
    const gsize total_size = 3 * chunk_size;
    TestReadWrite test;
    GInputStream* in;
    GOutputStream* out;
    GCancellable* cancel = g_cancellable_new();
    guint8* out_data = g_malloc(total_size);
    guint8* in_data = g_malloc0(total_size);
    gsize i;
    int fd[2];

    memset(&test, 0, sizeof(test));
    test.loop = g_main_loop_new(NULL, FALSE);
    test.chunk_size = chunk_size;

    for (i = 0; i < total_size; i++) {
        out_data[i] = (guint8) i;
    }

    g_assert(!pipe2(fd, O_NONBLOCK));
    g_assert((in = g_unix_input_stream_new(fd[0], TRUE)));
    g_assert((out = g_unix_output_stream_new(fd[1], TRUE)));

    /* Write two chunks and read one */
    giorpc_read_all_async(in, in_data, chunk_size, cancel,
        test_read_write_1, &test);
    giorpc_write_all_async(out, out_data, 2 * chunk_size, cancel,
        test_read_write_2, &test);

    /* This loop gets unblocked by test_read_write_1 */
    test_run(&test_opt, test.loop);
    GDEBUG("Read chunk #1");
    g_assert_cmpuint(test.in_total, == ,chunk_size);
    g_assert_cmpuint(test.out_total, == ,0);

    /* Request another two chunks */
    giorpc_read_all_async(in, in_data + chunk_size, 2 * chunk_size, cancel,
        test_read_write_3, &test);

    /* This loop gets unblocked by test_read_write_2 */
    test_run(&test_opt, test.loop);
    GDEBUG("Wrote chunks #1 and #2");
    g_assert_cmpuint(test.in_total, == ,chunk_size);
    g_assert_cmpuint(test.out_total, == ,2 * chunk_size);

    /* Write the last chunk */
    giorpc_write_all_async(out, out_data + 2 * chunk_size, chunk_size, cancel,
        test_read_write_4, &test);
    test_run(&test_opt, test.loop);
    GDEBUG("Done");
    g_assert_cmpuint(test.in_total, == ,3 * chunk_size);
    g_assert_cmpuint(test.out_total, == ,3 * chunk_size);
    g_assert(!memcmp(in_data, out_data, 3 * chunk_size));

    /* Cleanup */
    g_main_loop_unref(test.loop);
    g_object_unref(cancel);
    g_object_unref(in);
    g_object_unref(out);
    g_free(in_data);
    g_free(out_data);
}

/*==========================================================================*
 * read_cancel
 *==========================================================================*/

static
void
test_read_cancel_done(
    GInputStream* in,
    gsize nbytes,
    const GError* error,
    gpointer user_data)
{
    g_assert(!nbytes);
    g_assert(error);
    g_assert(error->domain == G_IO_ERROR);
    g_assert(error->code == G_IO_ERROR_CANCELLED);
    GDEBUG("%s", GERRMSG(error));
    g_main_loop_quit(user_data);
}

static
void
test_read_cancel()
{
    GMainLoop* loop = g_main_loop_new(NULL, FALSE);
    GCancellable* cancel = g_cancellable_new();
    GInputStream* in;
    guint8 buf[1];
    int fd[2];

    g_assert(!pipe2(fd, O_NONBLOCK));
    g_assert((in = g_unix_input_stream_new(fd[0], TRUE)));

    /* Submit the read */
    giorpc_read_all_async(in, buf, sizeof(buf), cancel,
        test_read_cancel_done, loop);

    /* Cancel the read and expect it to fail */
    g_cancellable_cancel(cancel);
    test_run(&test_opt, loop);
    GDEBUG("Done");

    /* Cleanup */
    close(fd[1]);
    g_main_loop_unref(loop);
    g_object_unref(cancel);
    g_object_unref(in);
}

/*==========================================================================*
 * read_eof
 *==========================================================================*/

static
void
test_read_eof_done(
    GInputStream* in,
    gsize nbytes,
    const GError* error,
    gpointer user_data)
{
    g_assert(!nbytes);
    g_assert(!error);
    GDEBUG("EOF");
    g_main_loop_quit(user_data);
}

static
void
test_read_eof()
{
    GMainLoop* loop = g_main_loop_new(NULL, FALSE);
    GCancellable* cancel = g_cancellable_new();
    GInputStream* in;
    guint8 buf[1];
    int fd[2];

    g_assert(!pipe2(fd, O_NONBLOCK));
    g_assert((in = g_unix_input_stream_new(fd[0], TRUE)));

    /* Submit the read */
    giorpc_read_all_async(in, buf, sizeof(buf), cancel,
        test_read_eof_done, loop);

    /* Close the write end of the pipe and expect EOF on the read end */
    close(fd[1]);
    test_run(&test_opt, loop);
    GDEBUG("Done");

    /* Cleanup */
    g_main_loop_unref(loop);
    g_object_unref(cancel);
    g_object_unref(in);
}

/*==========================================================================*
 * write_error
 *==========================================================================*/

static
void
test_write_error_done(
    GOutputStream* out,
    const GError* error,
    gpointer user_data)
{
    g_assert(error);
    GDEBUG("%s", GERRMSG(error));
    g_main_loop_quit(user_data);
}

static
void
test_write_error()
{
    GMainLoop* loop = g_main_loop_new(NULL, FALSE);
    GCancellable* cancel = g_cancellable_new();
    GOutputStream* out;
    guint8 data[1];
    int fd[2];

    g_assert(!pipe2(fd, O_NONBLOCK));
    g_assert((out = g_unix_output_stream_new(fd[1], TRUE)));

    /* Close the read end of the pipe and expect write to fail */
    data[0] = 0;
    close(fd[0]);
    giorpc_write_all_async(out, data, sizeof(data), cancel,
        test_write_error_done, loop);

    test_run(&test_opt, loop);
    GDEBUG("Done");

    /* Cleanup */
    g_main_loop_unref(loop);
    g_object_unref(cancel);
    g_object_unref(out);
}

/*==========================================================================*
 * Common
 *==========================================================================*/

int main(int argc, char* argv[])
{
    guint i;

    signal(SIGPIPE, SIG_IGN);
    g_test_init(&argc, &argv, NULL);
    test_init(&test_opt, argc, argv);

    for (i = 0; i < G_N_ELEMENTS(test_tlv_array_data); i++) {
        g_test_add_data_func(test_tlv_array_data[i].name,
            test_tlv_array_data + i, test_tlv_array);
    }
    g_test_add_func(TEST_("version"), test_version);
    g_test_add_func(TEST_("read_write"), test_read_write);
    g_test_add_func(TEST_("read_cancel"), test_read_cancel);
    g_test_add_func(TEST_("read_eof"), test_read_eof);
    g_test_add_func(TEST_("write_error"), test_write_error);
    return g_test_run();
}

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
