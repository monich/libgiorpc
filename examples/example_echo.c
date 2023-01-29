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

#include <giorpc.h>

#include <glib-unix.h>

#include <gutil_log.h>
#include <gutil_misc.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/*
 * This example implements both giopc client and server.
 * Without the arguments it runs as a server on the default port:
 *
 * $ example_echo
 *
 * and with the hostname as an argument it runs as a client, e.g.
 *
 * $ example_echo localhost
 *
 * The client reads the input data (standard input by default), calls
 * the server which prints the data to standard output and send the data
 * back to the client.
 *
 */

/* Protocol constants */

#define EXAMPLE_IID (1)
#define EXAMPLE_CALL_ECHO (1)

/* Defaults */

#define DEFAULT_PORT 2345

/* Return codes */

typedef enum retval {
    RET_OK,
    RET_ERR,
    RET_CMDLINE
} RETVAL;

/* Program context */

typedef struct example_client {
    GIoRpcPeer* peer;
    GCancellable* cancel;
    GError* error;
    guint read_id;
} ExampleClient;

typedef struct example_server {
    GMainLoop* loop;
    GSocketListener* listener;
    GCancellable* cancel;
    GList* active;
} ExampleServer;

enum {
    EXAMPLE_CONNECTION_STATE_CHANGED,
    EXAMPLE_CONNECTION_INCOMING_CALL,
    EXAMPLE_CONNECTION_EVENT_COUNT
};

typedef struct example_connection {
    ExampleServer* server;
    guint16 remote_port;
    char* remote_addr;
    GIOStream* stream;
    GIoRpcPeer* peer;
    gulong event_id[EXAMPLE_CONNECTION_EVENT_COUNT];
} ExampleConnection;

/*==========================================================================*
 * Utilities
 *==========================================================================*/

static
gboolean
example_parse_port(
    const char* value,
    guint16* port,
    GError** error)
{
    guint n;

    if (gutil_parse_uint(value, 0, &n) && n > 0 && n <= 0xffff) {
        *port = (guint16) n;
        return TRUE;
    } else {
        g_set_error(error, G_OPTION_ERROR, G_OPTION_ERROR_BAD_VALUE,
            "Invalid port '%s'", value);
        return FALSE;
    }
}

static
char*
example_address_str(
    GSocketAddress* address,
    guint16* port)
{
    char* str = NULL;

    if (address) {
        const gsize sa_size = g_socket_address_get_native_size(address);
        struct sockaddr* sa = g_malloc(sa_size);

        if (g_socket_address_to_native(address, sa, sa_size, NULL)) {
            const int af = sa->sa_family;
            const void* addr = NULL;

            if (af == AF_INET) {
                const struct sockaddr_in* in = (void*)sa;

                addr = &in->sin_addr;
                if (port) {
                    *port = ntohs(in->sin_port);
                }
            } else if (af == AF_INET6) {
                const struct sockaddr_in6* in6 = (void*)sa;

                addr = &in6->sin6_addr;
                if (port) {
                    *port = ntohs(in6->sin6_port);
                }
            }

            if (addr) {
                char buf[INET6_ADDRSTRLEN];

                str = g_strdup(inet_ntop(af, addr, buf, sizeof(buf)));
            }
            g_free(sa);
        }
    }
    return str;
}

static
char*
example_remote_address_str(
    GSocketConnection* connection,
    guint16* port)
{
    char* str = NULL;

    if (connection) {
        GSocketAddress* remote = g_socket_connection_get_remote_address
            (connection, NULL);

        if (remote) {
            str = example_address_str(remote, port);
            g_object_unref(remote);
        }
    }
    return str;
}

/*==========================================================================*
 * Connection
 *==========================================================================*/

static
void
example_connection_incoming_call(
    GIoRpcPeer* peer,
    guint iid,
    guint code,
    GBytes* data,
    GIoRpcRequest* req,
    gpointer user_data)
{
    /* Handle incoming calls */
    if (code == EXAMPLE_CALL_ECHO) {
        gsize len;
        const char* str = g_bytes_get_data(data, &len);

        printf("%.*s", (int) len, str);

        /* Send the same data back */
        giorpc_request_complete(req, data);
    }
}

static
void
example_connection_free(
    ExampleConnection* connection)
{
    giorpc_peer_remove_all_handlers(connection->peer, connection->event_id);
    giorpc_peer_unref(connection->peer);
    g_object_unref(connection->stream);
    g_free(connection->remote_addr);
    g_free(connection);
}

static
void
example_connection_state_changed(
    GIoRpcPeer* peer,
    void* user_data)
{
    if (peer->state == GIORPC_PEER_STATE_DONE) {
        ExampleConnection* connection = user_data;
        ExampleServer* server = connection->server;

        GINFO("Disconnected from %s:%hu", connection->remote_addr,
            connection->remote_port);

        /* Remove connection from the list */
        server->active = g_list_remove(server->active, connection);

        /* And free the context */
        example_connection_free(connection);

        /*
         * If we are in the process of being stopped and this was the last
         * active connection, terminate the event loop.
         */
        if (g_cancellable_is_cancelled(server->cancel) && !server->active) {
            g_main_loop_quit(server->loop);
        }
    }
}

static
ExampleConnection*
example_connection_new(
    ExampleServer* server,
    GSocketConnection* socket_connection)
{
    ExampleConnection* connection = g_new0(ExampleConnection, 1);

    connection->remote_addr = example_remote_address_str(socket_connection,
        &connection->remote_port);
    GINFO("Accepted connection from %s:%hu", connection->remote_addr,
        connection->remote_port);

    g_object_ref(connection->stream = G_IO_STREAM(socket_connection));
    connection->server = server;
    connection->peer = giorpc_peer_new(connection->stream, NULL);

    /* State change handler destroys the connection when peer is done */
    connection->event_id[EXAMPLE_CONNECTION_STATE_CHANGED] =
        giorpc_peer_add_state_handler(connection->peer,
            example_connection_state_changed, connection);

    /* And request handler handles the remote calls */
    connection->event_id[EXAMPLE_CONNECTION_INCOMING_CALL] =
        giorpc_peer_add_request_handler(connection->peer,
            EXAMPLE_IID, GIORPC_CODE_ANY,
            example_connection_incoming_call, connection);

    /* Start listening for incoming calls */
    giorpc_peer_start(connection->peer);
    return connection;
}

/*==========================================================================*
 * Server
 *==========================================================================*/

static
gboolean
example_server_stop(
    gpointer user_data)
{
    ExampleServer* server = user_data;

    if (!g_cancellable_is_cancelled(server->cancel)) {
        GINFO("Caught signal, shutting down...");
        g_cancellable_cancel(server->cancel);

        if (server->active) {
            GList* l;

            /* Wait until all peers are done */
            for (l = server->active; l; l = l->next) {
                ExampleConnection* connection = l->data;

                giorpc_peer_stop(connection->peer);
            }
        } else {
            /* There's nothing to wait for, quit right away */
             g_main_loop_quit(server->loop);
        }
    }
    return G_SOURCE_CONTINUE;
}

static
void
example_server_accept(
    GObject* object,
    GAsyncResult* result,
    gpointer user_data)
{
    ExampleServer* server = user_data;
    GError* error = NULL;
    GSocketListener* listener = G_SOCKET_LISTENER(object);
    GSocketConnection* connection = g_socket_listener_accept_finish(listener,
        result, NULL, &error);

    if (connection) {
        /* Start talking to the remote peer */
        server->active = g_list_append(server->active,
            example_connection_new(server, connection));

        g_object_unref(connection);
    } else {
        GWARN("%s", GERRMSG(error));
    }

    if (!error || !g_error_matches(error, G_IO_ERROR, G_IO_ERROR_CANCELLED)) {
        g_socket_listener_accept_async(listener, server->cancel,
            example_server_accept, server);
    }

    g_clear_error(&error);
}

static
GError*
example_server_run_with_port(
    guint16 port)
{
    GSocketListener* listener = g_socket_listener_new();
    GError* error = NULL;

    if (g_socket_listener_add_inet_port(listener, port, NULL, &error)) {
        ExampleServer server;
        guint sigterm, sigint;

        /* Set up the server context */
        memset(&server, 0, sizeof(server));
        server.listener = listener;
        server.loop = g_main_loop_new(NULL, FALSE);
        server.cancel = g_cancellable_new();

        /* Register signal handlers */
        sigterm = g_unix_signal_add(SIGTERM, example_server_stop, &server);
        sigint = g_unix_signal_add(SIGINT, example_server_stop, &server);

        /* Start listening */
        GINFO("Listening on port %hu", port);
        g_socket_listener_accept_async(listener, server.cancel,
            example_server_accept, &server);

        /* Run the loop */
        g_main_loop_run(server.loop);

        /* Cleanup */
        g_source_remove(sigterm);
        g_source_remove(sigint);
        g_main_loop_unref(server.loop);
        g_object_unref(server.cancel);
    }
    g_object_unref(listener);
    return error;
}

static
GError*
example_server_run(
    const char* port)
{
    GError* error = NULL;

    if (port && port[0]) {
        guint16 value = DEFAULT_PORT;

        if (example_parse_port(port, &value, &error)) {
            /* Run the server on the specific port */
            error = example_server_run_with_port(value);
        }
    } else {
        /* Run the server on the default port */
        error = example_server_run_with_port(DEFAULT_PORT);
    }
    return error;
}

/*==========================================================================*
 * Client
 *==========================================================================*/

static
gboolean
example_client_stop(
    gpointer user_data)
{
    ExampleClient* client = user_data;

    if (!g_cancellable_is_cancelled(client->cancel)) {
        GINFO("Caught signal, shutting down...");
        g_cancellable_cancel(client->cancel);

        /* The state change handler (below) will terminate the event loop */
        giorpc_peer_stop(client->peer);
    }
    return G_SOURCE_CONTINUE;
}

static
void
example_client_quit(
    GIoRpcPeer* peer,
    void* user_data)
{
    /* And state change handler quits the loop */
    if (peer->state == GIORPC_PEER_STATE_DONE) {
        g_main_loop_quit((GMainLoop*)user_data);
    }
}

static
gboolean
example_client_read(
    ExampleClient* client,
    GIOChannel* in)
{
    char buf[512];
    gsize bytes_read = 0;
    GIOStatus status = g_io_channel_read_chars(in, buf, sizeof(buf),
        &bytes_read, &client->error);

    if (client->error) {
        GDEBUG("%s", GERRMSG(client->error));
    } else if (status == G_IO_STATUS_EOF) {
        GVERBOSE("EOF");
    } else {
        /* Synchronously send the data to the peer */
        GBytes* in = g_bytes_new(buf, bytes_read);
        GBytes* out = giorpc_peer_call_sync(client->peer,
            EXAMPLE_IID, EXAMPLE_CALL_ECHO, in, GIORPC_SYNC_FLAGS_NONE,
            GIORPC_SYNC_TIMEOUT_INFINITE, client->cancel, &client->error);

        g_bytes_unref(in);
        if (out) {
            gsize len;
            const char* str = g_bytes_get_data(out, &len);

            /* We should have receive our data back from the peer */
            printf("%.*s", (int) len, str);
            g_bytes_unref(out);
            return TRUE;
        }
    }
    return FALSE;
}

static
gboolean
example_client_read_cb(
    GIOChannel* in,
    GIOCondition condition,
    gpointer user_data)
{
    ExampleClient* client = user_data;

    if (example_client_read(client, in)) {
        return G_SOURCE_CONTINUE;
    } else {
        client->read_id = 0;
        giorpc_peer_stop(client->peer);
        return G_SOURCE_REMOVE;
    }
}

static
GError*
example_client_run_stream(
    GIOChannel* in,
    GIOStream* stream)
{
    GMainLoop* loop = g_main_loop_new(NULL, FALSE);
    ExampleClient client;
    guint sigterm, sigint;
    gulong id;

    /* Set up the client context */
    memset(&client, 0, sizeof(client));
    client.peer = giorpc_peer_new(stream, NULL);
    client.cancel = g_cancellable_new();
    g_io_channel_set_flags(in, G_IO_FLAG_NONBLOCK, NULL);
    client.read_id = g_io_add_watch(in, G_IO_IN | G_IO_ERR | G_IO_HUP,
        example_client_read_cb, &client);

    /* State change handler quits the loop */
    id = giorpc_peer_add_state_handler(client.peer, example_client_quit, loop);

    /*
     * giorpc_peer_call_sync would automatically start the peer but we
     * want to do it explicitely in order to detect connection failure
     * happening before the first call.
     */
    giorpc_peer_start(client.peer);

    /* Run the loop */
    sigterm = g_unix_signal_add(SIGTERM, example_client_stop, &client);
    sigint = g_unix_signal_add(SIGINT, example_client_stop, &client);
    g_main_loop_run(loop);
    g_source_remove(sigterm);
    g_source_remove(sigint);

    /* Cleanup */
    if (client.read_id) {
        g_source_remove(client.read_id);
    }
    giorpc_peer_remove_handler(client.peer, id);
    giorpc_peer_unref(client.peer);
    g_object_unref(client.cancel);
    g_main_loop_unref(loop);

    /* Return the error (if there was any) */
    return client.error;
}

static
GError*
example_client_run(
    const char* addr,
    const char* file)
{
    GError* error = NULL;
    GIOChannel* in = NULL;

    if (file) {
        int fd = open(file, O_RDONLY);

        if (fd >= 0) {
            in = g_io_channel_unix_new(fd);
            g_io_channel_set_close_on_unref(in, TRUE);
        } else {
            error = g_error_new(G_IO_ERROR, g_io_error_from_errno(errno),
                "%s: %s", file,  g_strerror(errno));
        }
    } else {
        in = g_io_channel_unix_new(STDIN_FILENO);
    }

    if (in) {
        GSocketConnectable* connectable = g_network_address_parse(addr,
            DEFAULT_PORT, &error);

        if (connectable) {
            GSocketClient* client = g_socket_client_new();
            GSocketConnection* connection = g_socket_client_connect(client,
                connectable, NULL, &error);

            g_object_unref(connectable);
            if (connection) {
                guint16 port = 0;
                char* str = example_remote_address_str(connection, &port);

                if (str) {
                    GINFO("Connected to %s:%hu", str, port);
                    g_free(str);
                }

                /* Connection established, run the client */
                error = example_client_run_stream(in, G_IO_STREAM(connection));
                g_object_unref(connection);
            }
            g_object_unref(client);
        }
        g_io_channel_unref(in);
    }
    return error;
}

/*==========================================================================*
 * main
 *==========================================================================*/

int
main(
    int argc,
    char* argv[])
{
    RETVAL ret = RET_ERR;
    gboolean verbose = FALSE;
    char* file = NULL;
    GError* error = NULL;
    GOptionContext* options;
    const GOptionEntry entries[] = {
        { "verbose", 'v', 0, G_OPTION_ARG_NONE, &verbose,
          "Enable verbose output", NULL },
        { "input", 'i', 0, G_OPTION_ARG_FILENAME, &file,
          "Read input from FILE (client only)", "FILE" },
        { NULL }
    };

    /*
     * g_type_init has been deprecated since version 2.36
     * The type system is initialised automagically since then
     */
    G_GNUC_BEGIN_IGNORE_DEPRECATIONS;
    g_type_init();
    G_GNUC_END_IGNORE_DEPRECATIONS;

    /* Set up logging */
    gutil_log_timestamp = FALSE;
    gutil_log_func = gutil_log_stderr;
    gutil_log_default.name = "example";

    options = g_option_context_new("[ADDR][:PORT]");
    g_option_context_add_main_entries(options, entries, NULL);
    g_option_context_set_summary(options,
        "GIORPC demo. Implements a trivial echo protocol.\n\n"
        "ADDR may be an IPv6 or IPv4 address, or a domain name. Quoting "
        "with [] is supported for all address types. The port may be "
        "specified in the usual way with a colon. Ports may be given "
        "as decimal numbers or symbolic names. The default port is "
        G_STRINGIFY(DEFAULT_PORT) "\n\n"
        "If no ADDR is provided, acts as a server. In no FILE is provided,"
        " client reads data from standard input.");

    if (g_option_context_parse(options, &argc, &argv, &error) && argc <= 2) {
        char* addr = (argc > 1) ? g_strchomp(g_strdup(argv[1])) : NULL;

        /* Setup logging */
        gutil_log_func = gutil_log_stderr;
        gutil_log_timestamp = FALSE;
        gutil_log_default.level = verbose ?
            GLOG_LEVEL_VERBOSE :
            GLOG_LEVEL_INFO;

        if (addr && addr[0] != ':') {
            error = example_client_run(addr, file);
        } else if (file) {
            error = g_error_new(G_OPTION_ERROR, G_OPTION_ERROR_BAD_VALUE,
                "-i FILE option is only valid in client mode");
        } else {
            /* Skip the colon in ":PORT" */
            error = example_server_run(addr ? (addr + 1) : NULL);
        }

        if (error) {
            ret = (error->domain == G_OPTION_ERROR) ? RET_CMDLINE : RET_ERR;
            GINFO("%s", GERRMSG(error));
            g_error_free(error);
        } else {
            ret = RET_OK;
        }

        g_free(addr);
    } else {
        if (error) {
            GINFO("%s", GERRMSG(error));
            g_error_free(error);
        } else {
            char* help = g_option_context_get_help(options, TRUE, NULL);

            fprintf(stderr, "%s", help);
            g_free(help);
        }
        ret = RET_CMDLINE;
    }

    g_free(file);
    g_option_context_free(options);
    return ret;
}

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
