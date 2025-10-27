// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* Prepare the connection buffer to send the reply header. */
	int rc;

	if (conn->state == STATE_SENDING_404 || conn->state == STATE_404_SENT)
		rc = snprintf(conn->send_buffer, BUFSIZ, "HTTP/1.1 404 Not Found\r\n\r\n");
	else
		rc = snprintf(conn->send_buffer, BUFSIZ, "HTTP/1.1 200 OK\r\n\r\n");
	DIE(rc < 0 || rc > HTTP_MAX_HEADER_SIZE, "Send reply");
	conn->send_len = rc;
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* Prepare the connection buffer to send the 404 header. */
	conn->state = STATE_SENDING_404;
	connection_prepare_send_reply_header(conn);
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (!strncmp(conn->request_path + 1, AWS_REL_STATIC_FOLDER, strlen(AWS_REL_STATIC_FOLDER)))
		return RESOURCE_TYPE_STATIC;
	else if (!strncmp(conn->request_path + 1, AWS_REL_DYNAMIC_FOLDER, strlen(AWS_REL_DYNAMIC_FOLDER)))
		return RESOURCE_TYPE_DYNAMIC;

	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn = malloc(sizeof(struct connection));

	DIE(conn == NULL, "Malloc");

	conn->sockfd = sockfd;
	conn->recv_len = 0;
	conn->send_len = 0;
	conn->state = STATE_INITIAL;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	if (!conn->file_pos) {
		int rc = io_setup(10, &conn->ctx);

		DIE(rc < 0, "IO setup");
	}
	io_set_eventfd(&conn->iocb, conn->eventfd); // Create event fd
	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, BUFSIZ, conn->file_pos);
	conn->piocb[0] = &conn->iocb;
	io_submit(conn->ctx, 1, conn->piocb);
	conn->file_pos += BUFSIZ;
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	close(conn->sockfd);
	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	/* TODO: Set socket to be non-blocking. */
	fcntl(sockfd, F_SETFL, O_NONBLOCK);

	/* TODO: Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* TODO: Add socket to epoll. */
	int rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);

	DIE(rc < 0, "w_epoll_add_in");

	/* TODO: Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
}

void receive_data(struct connection *conn)
{
	/* Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	char buffer[BUFSIZ];
	int total_bytes = 0;

	while (1) {
		int bytes_recv = recv(conn->sockfd, buffer + total_bytes, BUFSIZ, 0);

		if (bytes_recv > 0)
			total_bytes += bytes_recv;
		else
			break;
	}

	// Transfer data from buffer to conn
	memcpy(conn->recv_buffer, buffer, total_bytes);
	conn->recv_len = total_bytes;
	conn->state = STATE_REQUEST_RECEIVED;
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	char *path = conn->request_path + 1;
	int fd = open(path, O_RDONLY); // Open file

	if (fd < 0) {
		ERR("open\n");
		return -1;
	}

	// Update connections
	conn->fd = fd;
	struct stat st;

	conn->file_size = fstat(fd, &st);
	conn->file_pos = 0;
	conn->send_len = st.st_size;
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	struct io_event events[10];
	int num_events = io_getevents(conn->ctx, 1, 10, events, NULL);

	DIE(num_events < 0, "Async IO");
	conn->send_len = events[0].res;
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	// Attach the connection object to the parser for use in the callbacks.
	conn->request_parser.data = conn;
	int rc = http_parser_execute(&conn->request_parser, &settings_on_path,
								conn->recv_buffer, conn->recv_len);

	if (rc != conn->recv_len) {
		ERR("Parser failed\n");
		return -1;
	}

	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	int total_bytes = 0;
	off_t offset = 0;

	if (connection_open_file(conn) < 0) {
		ERR("Open file at static\n");
		return STATE_404_SENT;
	}
	while (conn->send_len > 0) {
		int bytes_sent = sendfile(conn->sockfd, conn->fd, &offset, conn->send_len);

		if (bytes_sent > 0) {
			total_bytes += bytes_sent;
			conn->send_len -= bytes_sent;
		} else if (bytes_sent < 0) {
			ERR("sendfile\n");
			close(conn->fd);
			return STATE_404_SENT;
		}
	}
	close(conn->fd);
	conn->fd = -1;
	return STATE_DATA_SENT;
}


int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	int total_bytes = 0;

	while (conn->send_len > 0) {
		int bytes_sent = send(conn->sockfd, conn->send_buffer + total_bytes, conn->send_len, 0);

		if (bytes_sent < 0) {
			ERR("Send\n");
			return -1;
		}
		total_bytes += bytes_sent;
		conn->send_len -= bytes_sent;
	}
	int rc = w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);

	if (rc < 0) {
		ERR("w_epoll_in\n");
		return -1;
	}

	return total_bytes;
}

int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	conn->state = STATE_SENDING_DATA;
	conn->file_pos = 0;
	int rc = connection_open_file(conn);

	if (rc < 0) {
		connection_prepare_send_404(conn);
		connection_send_data(conn);
		return -1;
	}

	connection_prepare_send_reply_header(conn);
	while (1) {
		connection_start_async_io(conn);
		connection_complete_async_io(conn);
		if (!conn->send_len)
			break;
		connection_send_data(conn);
	}

	return 0;
}

void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */
	receive_data(conn);
	int rc = parse_header(conn);

	if (rc < 0) { // Header wasn't parsed, 404 must be sent
		connection_prepare_send_404(conn);
		conn->state = STATE_SENDING_HEADER;
		connection_send_data(conn);
	} else {
		switch (connection_get_resource_type(conn)) {
		case RESOURCE_TYPE_STATIC:
			conn->state = STATE_SENDING_HEADER;
			rc = connection_open_file(conn);
			if (rc < 0) {
				connection_prepare_send_404(conn);
				connection_send_data(conn);
			} else {
				connection_prepare_send_reply_header(conn);
				connection_send_data(conn);
				connection_send_static(conn);
			}
			break;

		case RESOURCE_TYPE_DYNAMIC:
			connection_send_dynamic(conn);
			break;

		case RESOURCE_TYPE_NONE:
			connection_prepare_send_404(conn); // Send 404, no static/dynamic resource
			conn->state = STATE_SENDING_HEADER;
			connection_send_data(conn);
			break;

		default:
			printf("shouldn't get here %d\n", connection_get_resource_type(conn));
			break;
		}
	}
	connection_remove(conn);
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */
	switch (conn->state) {
	case STATE_SENDING_HEADER:
		connection_send_data(conn);

		if (connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC)
			connection_send_static(conn);
		else if (connection_get_resource_type(conn) == RESOURCE_TYPE_DYNAMIC)
			connection_send_dynamic(conn);
		else
			connection_remove(conn);  // No valid resource, close the connection
		break;

	case STATE_SENDING_DATA:
		if (connection_get_resource_type(conn) == RESOURCE_TYPE_DYNAMIC)
			connection_send_dynamic(conn);
		break;

	case STATE_DATA_SENT:
		// Data sent successfully, close the connection
		connection_remove(conn);
		break;

	default:
		ERR("Unexpected state\n");
		exit(1);
	}
}


void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	if (event & EPOLLIN) {
		handle_input(conn);
	} else if (event & EPOLLOUT) {
		handle_output(conn);
	} else {
		ERR("Unkwon event\n");
		exit(1);
	}
}


int main(void)
{
	int rc;
	/* TODO: Initialize asynchronous operations. */
	io_setup(10, &ctx);

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* TODO: Add server socket to epoll object*/
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		if (rev.data.fd == listenfd)
			handle_new_connection();
		else
			handle_client(rev.events, rev.data.ptr);
	}

	return 0;
}
