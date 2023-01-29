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

#include "test_io_stream.h"

#include <gio/gunixinputstream.h>
#include <gio/gunixoutputstream.h>

typedef GIOStreamClass TestIoStreamClass;

typedef struct test_io_stream {
    GIOStream parent;
    GInputStream* in;
    GOutputStream* out;
} TestIoStream;

#define PARENT_CLASS test_io_stream_parent_class
#define THIS_TYPE test_io_stream_get_type()
#define THIS(obj) G_TYPE_CHECK_INSTANCE_CAST(obj, THIS_TYPE, TestIoStream)

G_DEFINE_TYPE(TestIoStream, test_io_stream, G_TYPE_IO_STREAM)

static
void
test_io_stream_finalize(
    GObject* object)
{
    TestIoStream* self = THIS(object);

    g_object_unref(self->in);
    g_object_unref(self->out);
    G_OBJECT_CLASS(PARENT_CLASS)->finalize(object);
}

static
GInputStream*
test_io_stream_get_input_stream(
    GIOStream* stream)
{
    return THIS(stream)->in;
}

static
GOutputStream*
test_io_stream_get_output_stream(
    GIOStream* stream)
{
    return THIS(stream)->out;
}

static
void
test_io_stream_init(
    TestIoStream* self)
{
}

static
void
test_io_stream_class_init(
    TestIoStreamClass* klass)
{
    G_OBJECT_CLASS(klass)->finalize = test_io_stream_finalize;
    klass->get_input_stream = test_io_stream_get_input_stream;
    klass->get_output_stream = test_io_stream_get_output_stream;
}

GIOStream*
test_io_stream_new(
    GInputStream* in,
    GOutputStream* out)
{
    if (in && out) {
        TestIoStream* self = g_object_new(THIS_TYPE, NULL);

        g_object_ref(self->in = in);
        g_object_ref(self->out = out);
        return &self->parent;
    }
    return NULL;
}

GIOStream*
test_io_stream_new_fd(
    int fd,
    gboolean close_fd)
{
    GInputStream* in = g_unix_input_stream_new(fd, FALSE);

    if (in) {
        GOutputStream* out = g_unix_output_stream_new(fd, close_fd);

        if (out) {
            TestIoStream* self = g_object_new(THIS_TYPE, NULL);

            self->in = in;
            self->out = out;
            return &self->parent;
        }
        g_object_unref(in);
    }
    return NULL;
}

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
