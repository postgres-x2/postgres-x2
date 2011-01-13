/*-------------------------------------------------------------------------
 *
 * pgxcnode.c
 *
 *	  Functions for the Coordinator communicating with the PGXC nodes:
 *	  Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "commands/prepare.h"
#include "gtm/gtm_c.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "tcop/dest.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "../interfaces/libpq/libpq-fe.h"


static int	datanode_count = 0;
static int	coord_count = 0;
/*
 * Datanode handles, saved in Transaction memory context when PostgresMain is launched
 * Those handles are used inside a transaction by a coordinator to Datanodes
 */
static PGXCNodeHandle *dn_handles = NULL;
/*
 * Coordinator handles, saved in Transaction memory context
 * when PostgresMain is launched.
 * Those handles are used inside a transaction by a coordinator to other coordinators.
 */
static PGXCNodeHandle *co_handles = NULL;

static void pgxc_node_init(PGXCNodeHandle *handle, int sock, int nodenum);
static void pgxc_node_free(PGXCNodeHandle *handle);

static int	get_int(PGXCNodeHandle * conn, size_t len, int *out);
static int	get_char(PGXCNodeHandle * conn, char *out);


/*
 * Initialize PGXCNodeHandle struct
 */
static void
init_pgxc_handle(PGXCNodeHandle *pgxc_handle)
{
	/*
	 * Socket descriptor is small non-negative integer,
	 * Indicate the handle is not initialized yet
	 */
	pgxc_handle->sock = NO_SOCKET;

	/* Initialise buffers */
	pgxc_handle->error = NULL;
	pgxc_handle->outSize = 16 * 1024;
	pgxc_handle->outBuffer = (char *) palloc(pgxc_handle->outSize);
	pgxc_handle->inSize = 16 * 1024;
	pgxc_handle->inBuffer = (char *) palloc(pgxc_handle->inSize);
	pgxc_handle->combiner = NULL;

	if (pgxc_handle->outBuffer == NULL || pgxc_handle->inBuffer == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}
}


/*
 * Allocate and initialize memory to store Datanode and Coordinator handles.
 */
void
InitMultinodeExecutor(void)
{
	int			i;

	/* This function could get called multiple times because of sigjmp */
	if (dn_handles != NULL && co_handles != NULL)
		return;

	/*
	 * Should be in TopMemoryContext.
	 * Assume the caller takes care of context switching
	 * Initialize Datanode handles.
	 */
	if (dn_handles == NULL)
	{
		dn_handles = (PGXCNodeHandle *) palloc(NumDataNodes * sizeof(PGXCNodeHandle));

		if (!dn_handles)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));

		/* initialize storage then */
		for (i = 0; i < NumDataNodes; i++)
			init_pgxc_handle(&dn_handles[i]);
	}

	/* Same but for Coordinators */
	if (co_handles == NULL)
	{
		co_handles = (PGXCNodeHandle *) palloc(NumCoords * sizeof(PGXCNodeHandle));

		if (!co_handles)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));

		for (i = 0; i < NumCoords; i++)
			init_pgxc_handle(&co_handles[i]);
	}

	datanode_count = 0;
	coord_count = 0;
}

/*
 * Builds up a connection string
 */
char *
PGXCNodeConnStr(char *host, char *port, char *dbname,
				char *user, char *password, char *remote_type)
{
	char	   *out,
				connstr[256];
	int			num;

	/*
	 * Build up connection string
	 * remote type can be coordinator, datanode or application.
	 */
	num = snprintf(connstr, sizeof(connstr),
				   "host=%s port=%s dbname=%s user=%s password=%s options='-c remotetype=%s'",
				   host, port, dbname, user, password, remote_type);

	/* Check for overflow */
	if (num > 0 && num < sizeof(connstr))
	{
		/* Output result */
		out = (char *) palloc(num + 1);
		strcpy(out, connstr);
		return out;
	}

	/* return NULL if we have problem */
	return NULL;
}


/*
 * Connect to a Data Node using a connection string
 */
NODE_CONNECTION *
PGXCNodeConnect(char *connstr)
{
	PGconn	   *conn;

	/* Delegate call to the pglib */
	conn = PQconnectdb(connstr);
	return (NODE_CONNECTION *) conn;
}


/*
 * Close specified connection
 */
void
PGXCNodeClose(NODE_CONNECTION *conn)
{
	/* Delegate call to the pglib */
	PQfinish((PGconn *) conn);
}


/*
 * Checks if connection active
 */
int
PGXCNodeConnected(NODE_CONNECTION *conn)
{
	/* Delegate call to the pglib */
	PGconn	   *pgconn = (PGconn *) conn;

	/*
	 * Simple check, want to do more comprehencive -
	 * check if it is ready for guery
	 */
	return pgconn && PQstatus(pgconn) == CONNECTION_OK;
}



/* Close the socket handle (this process' copy) and free occupied memory
 *
 * Note that we do not free the handle and its members. This will be
 * taken care of when the transaction ends, when TopTransactionContext
 * is destroyed in xact.c.
 */
static void
pgxc_node_free(PGXCNodeHandle *handle)
{
	close(handle->sock);
	handle->sock = NO_SOCKET;
}


/*
 * Create and initialise internal structure to communicate to
 * Data Node via supplied socket descriptor.
 * Structure stores state info and I/O buffers
 */
static void
pgxc_node_init(PGXCNodeHandle *handle, int sock, int nodenum)
{
	handle->nodenum = nodenum;
	handle->sock = sock;
	handle->transaction_status = 'I';
	handle->state = DN_CONNECTION_STATE_IDLE;
	handle->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
	handle->have_row_desc = false;
#endif
	handle->error = NULL;
	handle->outEnd = 0;
	handle->inStart = 0;
	handle->inEnd = 0;
	handle->inCursor = 0;
}


/*
 * Wait while at least one of specified connections has data available and read
 * the data into the buffer
 */
int
pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout)
{
	int			i,
				res_select,
				nfds = 0;
	fd_set		readfds;

	FD_ZERO(&readfds);
	for (i = 0; i < conn_count; i++)
	{
		/* If connection finished sending do not wait input from it */
		if (connections[i]->state == DN_CONNECTION_STATE_IDLE
				|| HAS_MESSAGE_BUFFERED(connections[i]))
			continue;

		/* prepare select params */
		if (connections[i]->sock > 0)
		{
			FD_SET(connections[i]->sock, &readfds);
			nfds = connections[i]->sock;
		}
		else
		{
			/* flag as bad, it will be removed from the list */
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
		}
	}

	/*
	 * Return if we do not have connections to receive input
	 */
	if (nfds == 0)
		return 0;

retry:
	res_select = select(nfds + 1, &readfds, NULL, NULL, timeout);
	if (res_select < 0)
	{
		/* error - retry if EINTR or EAGAIN */
		if (errno == EINTR || errno == EAGAIN)
			goto retry;

		if (errno == EBADF)
		{
			elog(WARNING, "select() bad file descriptor set");
		}
		elog(WARNING, "select() error: %d", errno);
		return errno;
	}

	if (res_select == 0)
	{
		/* Handle timeout */
		elog(WARNING, "timeout while waiting for response");
		return EOF;
	}

	/* read data */
	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *conn = connections[i];

		if (FD_ISSET(conn->sock, &readfds))
		{
			int	read_status = pgxc_node_read_data(conn);

			if (read_status == EOF || read_status < 0)
			{
				/* Can not read - no more actions, just discard connection */
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				add_error_message(conn, "unexpected EOF on datanode connection");
				elog(WARNING, "unexpected EOF on datanode connection");
				/* Should we read from the other connections before returning? */
				return EOF;
			}
		}
	}
	return 0;
}


/*
 * Read up incoming messages from the PGXC node connection
 */
int
pgxc_node_read_data(PGXCNodeHandle *conn)
{
	int			someread = 0;
	int			nread;

	if (conn->sock < 0)
	{
		add_error_message(conn, "bad socket");
		return EOF;
	}

	/* Left-justify any data in the buffer to make room */
	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if (conn->inSize - conn->inEnd < 8192)
	{
		if (ensure_in_buffer_capacity(conn->inEnd + (size_t) 8192, conn) != 0)
		{
			/*
			 * We don't insist that the enlarge worked, but we need some room
			 */
			if (conn->inSize - conn->inEnd < 100)
			{
				add_error_message(conn, "can not allocate buffer");
				return -1;
			}
		}
	}

retry:
	nread = recv(conn->sock, conn->inBuffer + conn->inEnd,
				 conn->inSize - conn->inEnd, 0);

	if (nread < 0)
	{
		elog(DEBUG1, "dnrd errno = %d", errno);
		if (errno == EINTR)
			goto retry;
		/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
		if (errno == EAGAIN)
			return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
		if (errno == EWOULDBLOCK)
			return someread;
#endif
		/* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
		if (errno == ECONNRESET)
		{
			/*
			 * OK, we are getting a zero read even though select() says ready. This
			 * means the connection has been closed.  Cope.
			 */
			add_error_message(conn,
							  "data node closed the connection unexpectedly\n"
				"\tThis probably means the data node terminated abnormally\n"
							  "\tbefore or while processing the request.\n");
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;	/* No more connection to
														 * backend */
			closesocket(conn->sock);
			conn->sock = NO_SOCKET;

			return -1;
		}
#endif
		add_error_message(conn, "could not receive data from server");
		return -1;

	}

	if (nread > 0)
	{
		conn->inEnd += nread;

		/*
		 * Hack to deal with the fact that some kernels will only give us back
		 * 1 packet per recv() call, even if we asked for more and there is
		 * more available.	If it looks like we are reading a long message,
		 * loop back to recv() again immediately, until we run out of data or
		 * buffer space.  Without this, the block-and-restart behavior of
		 * libpq's higher levels leads to O(N^2) performance on long messages.
		 *
		 * Since we left-justified the data above, conn->inEnd gives the
		 * amount of data already read in the current message.	We consider
		 * the message "long" once we have acquired 32k ...
		 */
		if (conn->inEnd > 32768 &&
			(conn->inSize - conn->inEnd) >= 8192)
		{
			someread = 1;
			goto retry;
		}
		return 1;
	}

	if (nread == 0)
	{
		elog(DEBUG1, "nread returned 0");
		return EOF;
	}

	if (someread)
		return 1;				/* got a zero read after successful tries */

	return 0;
}


/*
 * Get one character from the connection buffer and advance cursor
 */
static int
get_char(PGXCNodeHandle * conn, char *out)
{
	if (conn->inCursor < conn->inEnd)
	{
		*out = conn->inBuffer[conn->inCursor++];
		return 0;
	}
	return EOF;
}

/*
 * Read an integer from the connection buffer and advance cursor
 */
static int
get_int(PGXCNodeHandle *conn, size_t len, int *out)
{
	unsigned short tmp2;
	unsigned int tmp4;

	if (conn->inCursor + len > conn->inEnd)
		return EOF;

	switch (len)
	{
		case 2:
			memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
			conn->inCursor += 2;
			*out = (int) ntohs(tmp2);
			break;
		case 4:
			memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
			conn->inCursor += 4;
			*out = (int) ntohl(tmp4);
			break;
		default:
			add_error_message(conn, "not supported int size");
			return EOF;
	}

	return 0;
}


/*
 * get_message
 * If connection has enough data read entire message from the connection buffer
 * and returns message type. Message data and data length are returned as
 * var parameters.
 * If buffer does not have enough data leaves cursor unchanged, changes
 * connection status to DN_CONNECTION_STATE_QUERY indicating it needs to
 * receive more and returns \0
 * conn - connection to read from
 * len - returned length of the data where msg is pointing to
 * msg - returns pointer to memory in the incoming buffer. The buffer probably
 * will be overwritten upon next receive, so if caller wants to refer it later
 * it should make a copy.
 */
char
get_message(PGXCNodeHandle *conn, int *len, char **msg)
{
	char 		msgtype;

	if (get_char(conn, &msgtype) || get_int(conn, 4, len))
	{
		/* Successful get_char would move cursor, restore position */
		conn->inCursor = conn->inStart;
		return '\0';
	}

	*len -= 4;

	if (conn->inCursor + *len > conn->inEnd)
	{
		/*
		 * Not enough data in the buffer, we should read more.
		 * Reading function will discard already consumed data in the buffer
		 * till conn->inBegin. Then we want the message that is partly in the
		 * buffer now has been read completely, to avoid extra read/handle
		 * cycles. The space needed is 1 byte for message type, 4 bytes for
		 * message length and message itself which size is currently in *len.
		 * The buffer may already be large enough, in this case the function
		 * ensure_in_buffer_capacity() will immediately return
		 */
		ensure_in_buffer_capacity(5 + (size_t) *len, conn);
		conn->inCursor = conn->inStart;
		return '\0';
	}

	*msg = conn->inBuffer + conn->inCursor;
	conn->inCursor += *len;
	conn->inStart = conn->inCursor;
	return msgtype;
}


/*
 * Release all data node connections  and coordinator connections
 * back to pool and release occupied memory
 */
void
release_handles(void)
{
	int			i;
	int 		dn_discard[NumDataNodes];
	int			co_discard[NumCoords];
	int			dn_ndisc = 0;
	int			co_ndisc = 0;

	if (datanode_count == 0 && coord_count == 0)
		return;

	/* Do not release connections if we have prepared statements on nodes */
	if (HaveActiveDatanodeStatements())
		return;

	/* Collect Data Nodes handles */
	for (i = 0; i < NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = &dn_handles[i];

		if (handle->sock != NO_SOCKET)
		{
			if (handle->state == DN_CONNECTION_STATE_ERROR_FATAL)
				dn_discard[dn_ndisc++] = handle->nodenum;
			else if (handle->state != DN_CONNECTION_STATE_IDLE)
			{
				elog(WARNING, "Connection to Datanode %d has unexpected state %d and will be dropped", handle->nodenum, handle->state);
				dn_discard[dn_ndisc++] = handle->nodenum;
			}
			pgxc_node_free(handle);
		}
	}

	/* Collect Coordinator handles */
	for (i = 0; i < NumCoords; i++)
	{
		PGXCNodeHandle *handle = &co_handles[i];

		if (handle->sock != NO_SOCKET)
		{
			if (handle->state == DN_CONNECTION_STATE_ERROR_FATAL)
				co_discard[co_ndisc++] = handle->nodenum;
			else if (handle->state != DN_CONNECTION_STATE_IDLE)
			{
				elog(WARNING, "Connection to Coordinator %d has unexpected state %d and will be dropped", handle->nodenum, handle->state);
				co_discard[co_ndisc++] = handle->nodenum;
			}
			pgxc_node_free(handle);
		}
	}

	/* Here We have to add also the list of Coordinator Connections we want to drop at the same time */
	PoolManagerReleaseConnections(dn_ndisc, dn_discard, co_ndisc, co_discard);

	datanode_count = 0;
	coord_count = 0;
}


/*
 * Ensure specified amount of data can fit to the incoming buffer and
 * increase it if necessary
 */
int
ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{
	int			newsize = handle->inSize;
	char	   *newbuf;

	if (bytes_needed <= (size_t) newsize)
		return 0;

	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = repalloc(handle->inBuffer, newsize);
		if (newbuf)
		{
			/* repalloc succeeded */
			handle->inBuffer = newbuf;
			handle->inSize = newsize;
			return 0;
		}
	}

	newsize = handle->inSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = repalloc(handle->inBuffer, newsize);
		if (newbuf)
		{
			/* repalloc succeeded */
			handle->inBuffer = newbuf;
			handle->inSize = newsize;
			return 0;
		}
	}

	return EOF;
}


/*
 * Ensure specified amount of data can fit to the outgoing buffer and
 * increase it if necessary
 */
int
ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{
	int			newsize = handle->outSize;
	char	   *newbuf;

	if (bytes_needed <= (size_t) newsize)
		return 0;

	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = repalloc(handle->outBuffer, newsize);
		if (newbuf)
		{
			/* repalloc succeeded */
			handle->outBuffer = newbuf;
			handle->outSize = newsize;
			return 0;
		}
	}

	newsize = handle->outSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = repalloc(handle->outBuffer, newsize);
		if (newbuf)
		{
			/* repalloc succeeded */
			handle->outBuffer = newbuf;
			handle->outSize = newsize;
			return 0;
		}
	}

	return EOF;
}


/*
 * Send specified amount of data from the outgoing buffer over the connection
 */
int
send_some(PGXCNodeHandle *handle, int len)
{
	char	   *ptr = handle->outBuffer;
	int			remaining = handle->outEnd;
	int			result = 0;

	/* while there's still data to send */
	while (len > 0)
	{
		int			sent;

#ifndef WIN32
		sent = send(handle->sock, ptr, len, 0);
#else
		/*
		 * Windows can fail on large sends, per KB article Q201213. The failure-point
		 * appears to be different in different versions of Windows, but 64k should
		 * always be safe.
		 */
		sent = send(handle->sock, ptr, Min(len, 65536), 0);
#endif

		if (sent < 0)
		{
			/*
			 * Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble. If it's
			 * EPIPE or ECONNRESET, assume we've lost the backend connection
			 * permanently.
			 */
			switch (errno)
			{
#ifdef EAGAIN
				case EAGAIN:
					break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
					break;
#endif
				case EINTR:
					continue;

				case EPIPE:
#ifdef ECONNRESET
				case ECONNRESET:
#endif
					add_error_message(handle, "server closed the connection unexpectedly\n"
					"\tThis probably means the server terminated abnormally\n"
							  "\tbefore or while processing the request.\n");

					/*
					 * We used to close the socket here, but that's a bad idea
					 * since there might be unread data waiting (typically, a
					 * NOTICE message from the backend telling us it's
					 * committing hara-kiri...).  Leave the socket open until
					 * pqReadData finds no more data can be read.  But abandon
					 * attempt to send data.
					 */
					handle->outEnd = 0;
					return -1;

				default:
					add_error_message(handle, "could not send data to server");
					/* We don't assume it's a fatal error... */
					handle->outEnd = 0;
					return -1;
			}
		}
		else
		{
			ptr += sent;
			len -= sent;
			remaining -= sent;
		}

		if (len > 0)
		{
			/*
			 * We did not send it all
			 * return 1 to indicate that data is still pending.
			 */
			result = 1;
			break;
		}
	}

	/* shift the remaining contents of the buffer */
	if (remaining > 0)
		memmove(handle->outBuffer, ptr, remaining);
	handle->outEnd = remaining;

	return result;
}

/*
 * Send PARSE message with specified statement down to the Data node
 */
int
pgxc_node_send_parse(PGXCNodeHandle * handle, const char* statement,
					 const char *query)
{
	/* statement name size (allow NULL) */
	int			stmtLen = statement ? strlen(statement) + 1 : 1;
	/* size of query string */
	int			strLen = strlen(query) + 1;
	/* size of parameter array (always empty for now) */
	int 		paramLen = 2;

	/* size + stmtLen + strlen + paramLen */
	int			msgLen = 4 + stmtLen + strLen + paramLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'P';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* statement name */
	if (statement)
	{
		memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
		handle->outEnd += stmtLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* query */
	memcpy(handle->outBuffer + handle->outEnd, query, strLen);
	handle->outEnd += strLen;
	/* parameter types (none) */
	handle->outBuffer[handle->outEnd++] = 0;
	handle->outBuffer[handle->outEnd++] = 0;

 	return 0;
}


/*
 * Send BIND message down to the Data node
 */
int
pgxc_node_send_bind(PGXCNodeHandle * handle, const char *portal,
					const char *statement, int paramlen, char *params)
{
	uint16		n16;
	int			pnameLen;
	int			stmtLen;
	int 		paramCodeLen;
	int 		paramValueLen;
	int 		paramOutLen;
	int			msgLen;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* portal name size (allow NULL) */
	pnameLen = portal ? strlen(portal) + 1 : 1;
	/* statement name size (allow NULL) */
	stmtLen = statement ? strlen(statement) + 1 : 1;
	/* size of parameter codes array (always empty for now) */
	paramCodeLen = 2;
	/* size of parameter values array, 2 if no params */
	paramValueLen = paramlen ? paramlen : 2;
	/* size of output parameter codes array (always empty for now) */
	paramOutLen = 2;
	/* size + pnameLen + stmtLen + parameters */
	msgLen = 4 + pnameLen + stmtLen + paramCodeLen + paramValueLen + paramOutLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'B';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* portal name */
	if (portal)
	{
		memcpy(handle->outBuffer + handle->outEnd, portal, pnameLen);
		handle->outEnd += pnameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* statement name */
	if (statement)
	{
		memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
		handle->outEnd += stmtLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* parameter codes (none) */
	handle->outBuffer[handle->outEnd++] = 0;
	handle->outBuffer[handle->outEnd++] = 0;
	/* parameter values */
	if (paramlen)
	{
		memcpy(handle->outBuffer + handle->outEnd, params, paramlen);
		handle->outEnd += paramlen;
	}
	else
	{
		handle->outBuffer[handle->outEnd++] = 0;
		handle->outBuffer[handle->outEnd++] = 0;
	}
	/* output parameter codes (none) */
	handle->outBuffer[handle->outEnd++] = 0;
	handle->outBuffer[handle->outEnd++] = 0;

 	return 0;
}


/*
 * Send DESCRIBE message (portal or statement) down to the Data node
 */
int
pgxc_node_send_describe(PGXCNodeHandle * handle, bool is_statement,
						const char *name)
{
	int			nameLen;
	int			msgLen;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* statement or portal name size (allow NULL) */
	nameLen = name ? strlen(name) + 1 : 1;

	/* size + statement/portal + name */
	msgLen = 4 + 1 + nameLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'D';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* statement/portal flag */
	handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
	/* object name */
	if (name)
	{
		memcpy(handle->outBuffer + handle->outEnd, name, nameLen);
		handle->outEnd += nameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

 	return 0;
}


/*
 * Send CLOSE message (portal or statement) down to the Data node
 */
int
pgxc_node_send_close(PGXCNodeHandle * handle, bool is_statement,
					 const char *name)
{
	/* statement or portal name size (allow NULL) */
	int			nameLen = name ? strlen(name) + 1 : 1;

	/* size + statement/portal + name */
	int			msgLen = 4 + 1 + nameLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'C';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* statement/portal flag */
	handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
	/* object name */
	if (name)
	{
		memcpy(handle->outBuffer + handle->outEnd, name, nameLen);
		handle->outEnd += nameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

	handle->state = DN_CONNECTION_STATE_QUERY;

 	return 0;
}

/*
 * Send EXECUTE message down to the Data node
 */
int
pgxc_node_send_execute(PGXCNodeHandle * handle, const char *portal, int fetch)
{
	/* portal name size (allow NULL) */
	int			pnameLen = portal ? strlen(portal) + 1 : 1;

	/* size + pnameLen + fetchLen */
	int			msgLen = 4 + pnameLen + 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'E';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* portal name */
	if (portal)
	{
		memcpy(handle->outBuffer + handle->outEnd, portal, pnameLen);
		handle->outEnd += pnameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

	/* fetch */
	fetch = htonl(fetch);
	memcpy(handle->outBuffer + handle->outEnd, &fetch, 4);
	handle->outEnd += 4;

	handle->state = DN_CONNECTION_STATE_QUERY;

	return 0;
}


/*
 * Send FLUSH message down to the Data node
 */
int
pgxc_node_send_flush(PGXCNodeHandle * handle)
{
	/* size */
	int			msgLen = 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'H';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	return pgxc_node_flush(handle);
}


/*
 * Send SYNC message down to the Data node
 */
int
pgxc_node_send_sync(PGXCNodeHandle * handle)
{
	/* size */
	int			msgLen = 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'S';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	return pgxc_node_flush(handle);
}


/*
 * Send the GXID down to the Data node
 */
int
pgxc_node_send_query_extended(PGXCNodeHandle *handle, const char *query,
							  const char *statement, const char *portal,
							  int paramlen, char *params,
							  bool send_describe, int fetch_size)
{
	/* NULL query indicates already prepared statement */
	if (query)
		if (pgxc_node_send_parse(handle, statement, query))
			return EOF;
	if (pgxc_node_send_bind(handle, portal, statement, paramlen, params))
		return EOF;
	if (send_describe)
		if (pgxc_node_send_describe(handle, false, portal))
			return EOF;
	if (fetch_size >= 0)
		if (pgxc_node_send_execute(handle, portal, fetch_size))
			return EOF;
	if (pgxc_node_send_sync(handle))
		return EOF;

	return 0;
}

/*
 * This method won't return until connection buffer is empty or error occurs
 * To ensure all data are on the wire before waiting for response
 */
int
pgxc_node_flush(PGXCNodeHandle *handle)
{
	while (handle->outEnd)
	{
		if (send_some(handle, handle->outEnd) < 0)
		{
			add_error_message(handle, "failed to send data to datanode");
			return EOF;
		}
	}
	return 0;
}

/*
 * Send specified statement down to the PGXC node
 */
int
pgxc_node_send_query(PGXCNodeHandle * handle, const char *query)
{
	int			strLen;
	int			msgLen;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	strLen = strlen(query) + 1;
	/* size + strlen */
	msgLen = 4 + strLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'Q';
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, query, strLen);
	handle->outEnd += strLen;

	handle->state = DN_CONNECTION_STATE_QUERY;

 	return pgxc_node_flush(handle);
}


/*
 * Send the GXID down to the PGXC node
 */
int
pgxc_node_send_gxid(PGXCNodeHandle *handle, GlobalTransactionId gxid)
{
	int			msglen = 8;
	int			i32;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'g';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	i32 = htonl(gxid);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	return 0;
}


/*
 * Send the snapshot down to the PGXC node
 */
int
pgxc_node_send_snapshot(PGXCNodeHandle *handle, Snapshot snapshot)
{
	int			msglen;
	int			nval;
	int			i;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* calculate message length */
	msglen = 20;
	if (snapshot->xcnt > 0)
		msglen += snapshot->xcnt * 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 's';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	nval = htonl(snapshot->xmin);
	memcpy(handle->outBuffer + handle->outEnd, &nval, 4);
	handle->outEnd += 4;

	nval = htonl(snapshot->xmax);
	memcpy(handle->outBuffer + handle->outEnd, &nval, 4);
	handle->outEnd += 4;

	nval = htonl(snapshot->recent_global_xmin);
	memcpy(handle->outBuffer + handle->outEnd, &nval, 4);
	handle->outEnd += 4;

	nval = htonl(snapshot->xcnt);
	memcpy(handle->outBuffer + handle->outEnd, &nval, 4);
	handle->outEnd += 4;

	for (i = 0; i < snapshot->xcnt; i++)
	{
		nval = htonl(snapshot->xip[i]);
		memcpy(handle->outBuffer + handle->outEnd, &nval, 4);
		handle->outEnd += 4;
	}

	return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int
pgxc_node_send_timestamp(PGXCNodeHandle *handle, TimestampTz timestamp)
{
	int		msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
	uint32	n32;
	int64	i = (int64) timestamp;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}
	handle->outBuffer[handle->outEnd++] = 't';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	/* High order half first */
#ifdef INT64_IS_BUSTED
	/* don't try a right shift of 32 on a 32-bit word */
	n32 = (i < 0) ? -1 : 0;
#else
	n32 = (uint32) (i >> 32);
#endif
	n32 = htonl(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = htonl(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	return 0;
}


/*
 * Add another message to the list of errors to be returned back to the client
 * at the convenient time
 */
void
add_error_message(PGXCNodeHandle *handle, const char *message)
{
	handle->transaction_status = 'E';
	if (handle->error)
	{
		/* PGXCTODO append */
	}
	else
		handle->error = pstrdup(message);
}

/*
 * for specified list return array of PGXCNodeHandles
 * acquire from pool if needed.
 * the lenth of returned array is the same as of nodelist
 * For Datanodes, Special case is empty or NIL nodeList, in this case return all the nodes.
 * The returned list should be pfree'd when no longer needed.
 * For Coordinator, do not get a connection if Coordinator list is NIL,
 * Coordinator fds is returned only if transaction uses a DDL
 */
PGXCNodeAllHandles *
get_handles(List *datanodelist, List *coordlist, bool is_coord_only_query)
{
	PGXCNodeAllHandles *result;
	ListCell   *node_list_item;
	List	   *dn_allocate = NIL;
	List	   *co_allocate = NIL;
	MemoryContext old_context;

	/* index of the result array */
	int			i = 0;

	/* Handles should be there while transaction lasts */
	old_context = MemoryContextSwitchTo(TopTransactionContext);

	result = (PGXCNodeAllHandles *) palloc(sizeof(PGXCNodeAllHandles));
	if (!result)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	result->primary_handle = NULL;
	result->datanode_handles = NULL;
	result->coord_handles = NULL;
	result->co_conn_count = list_length(coordlist);
	result->dn_conn_count = list_length(datanodelist);

	/*
	 * Get Handles for Datanodes
	 * If node list is empty execute request on current nodes.
	 * It is also possible that the query has to be launched only on Coordinators.
	 */
	if (!is_coord_only_query)
	{
		if (list_length(datanodelist) == 0)
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */
			result->datanode_handles = (PGXCNodeHandle **)
									   palloc(NumDataNodes * sizeof(PGXCNodeHandle *));
			if (!result->datanode_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			for (i = 0; i < NumDataNodes; i++)
			{
				result->datanode_handles[i] = &dn_handles[i];
				if (dn_handles[i].sock == NO_SOCKET)
					dn_allocate = lappend_int(dn_allocate, i + 1);
			}
		}
		else
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */
			result->datanode_handles = (PGXCNodeHandle **)
									   palloc(list_length(datanodelist) * sizeof(PGXCNodeHandle *));
			if (!result->datanode_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			i = 0;
			foreach(node_list_item, datanodelist)
			{
				int			node = lfirst_int(node_list_item);

				Assert(node > 0 && node <= NumDataNodes);

				result->datanode_handles[i++] = &dn_handles[node - 1];
				if (dn_handles[node - 1].sock == NO_SOCKET)
					dn_allocate = lappend_int(dn_allocate, node);
			}
		}
	}

	/*
	 * Get Handles for Coordinators
	 * If node list is empty execute request on current nodes
	 * There are transactions where the coordinator list is NULL Ex:COPY
	 */
	if (coordlist)
	{
		if (list_length(coordlist) == 0)
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */
			result->coord_handles = (PGXCNodeHandle **)
									palloc(NumCoords * sizeof(PGXCNodeHandle *));
			if (!result->coord_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			for (i = 0; i < NumCoords; i++)
			{
				result->coord_handles[i] = &co_handles[i];
				if (co_handles[i].sock == NO_SOCKET)
					co_allocate = lappend_int(co_allocate, i + 1);
			}
		}
		else
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */
			result->coord_handles = (PGXCNodeHandle **)
									palloc(list_length(coordlist) * sizeof(PGXCNodeHandle *));
			if (!result->coord_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			i = 0;
			/* Some transactions do not need Coordinators, ex: COPY */
			foreach(node_list_item, coordlist)
			{
				int			node = lfirst_int(node_list_item);

				Assert(node > 0 && node <= NumCoords);

				result->coord_handles[i++] = &co_handles[node - 1];
				if (co_handles[node - 1].sock == NO_SOCKET)
					co_allocate = lappend_int(co_allocate, node);
			}
		}
	}

	/*
	 * Pooler can get activated even if list of Coordinator or Datanode is NULL
	 * If both lists are NIL, we don't need to call Pooler.
	 */
	if (dn_allocate || co_allocate)
	{
		int			j = 0;
		int		   *fds = PoolManagerGetConnections(dn_allocate, co_allocate);

		if (!fds)
		{
			if (coordlist)
				if (result->coord_handles)
					pfree(result->coord_handles);
			if (datanodelist)
				if (result->datanode_handles)
					pfree(result->datanode_handles);

			pfree(result);
			if (dn_allocate)
				list_free(dn_allocate);
			if (co_allocate)
				list_free(co_allocate);
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("Failed to get pooled connections")));
		}
		/* Initialisation for Datanodes */
		if (dn_allocate)
		{
			foreach(node_list_item, dn_allocate)
			{
				int			node = lfirst_int(node_list_item);
				int			fdsock = fds[j++];

				pgxc_node_init(&dn_handles[node - 1], fdsock, node);
				datanode_count++;
			}
		}
		/* Initialisation for Coordinators */
		if (co_allocate)
		{
			foreach(node_list_item, co_allocate)
			{
				int			node = lfirst_int(node_list_item);
				int			fdsock = fds[j++];

				pgxc_node_init(&co_handles[node - 1], fdsock, node);
				coord_count++;
			}
		}

		pfree(fds);

		if (co_allocate)
			list_free(co_allocate);
		if (dn_allocate)
			list_free(dn_allocate);
	}

	/* restore context */
	MemoryContextSwitchTo(old_context);

	return result;
}


/*
 * Return handles involved into current transaction, to run commit or rollback
 * on them, as requested.
 * Depending on the connection type, Coordinator or Datanode connections are returned.
 *
 * Transaction is not started on nodes when read-only statement is executed
 * on it, so we do not have to commit or rollback on those nodes.
 * Parameter should point to array able to store at least datanode_count pointers
 * to a PGXCNodeHandle structure.
 * The function returns number of pointers written to the connections array.
 * Remaining items in the array, if any, will be kept unchanged
 *
 * In an implicit 2PC, status of connections is set back to idle after preparing
 * the transaction on each backend.
 * At commit phase, it is necessary to get backends in idle state to be able to
 * commit properly the backends.
 *
 * In the case of an error occuring with an implicit 2PC that has been partially
 * committed on nodes, return the list of connections that has an error state
 * to register the list of remaining nodes not commit prepared on GTM.
 */
int
get_transaction_nodes(PGXCNodeHandle **connections, char client_conn_type,
					  PGXCNode_HandleRequested status_requested)
{
	int			tran_count = 0;
	int			i;

	if (datanode_count && client_conn_type == REMOTE_CONN_DATANODE)
	{
		for (i = 0; i < NumDataNodes; i++)
		{
			if (dn_handles[i].sock != NO_SOCKET &&
				(dn_handles[i].state != DN_CONNECTION_STATE_ERROR_FATAL ||
				 status_requested == HANDLE_ERROR))
			{
				if (status_requested == HANDLE_IDLE && dn_handles[i].transaction_status == 'I')
					connections[tran_count++] = &dn_handles[i];
				else if (status_requested == HANDLE_ERROR && dn_handles[i].transaction_status == 'E')
					connections[tran_count++] = &dn_handles[i];
				else if (dn_handles[i].transaction_status != 'I')
					connections[tran_count++] = &dn_handles[i];
			}
		}
	}

	if (coord_count && client_conn_type == REMOTE_CONN_COORD)
	{
		for (i = 0; i < NumCoords; i++)
		{
			if (co_handles[i].sock != NO_SOCKET &&
				(co_handles[i].state != DN_CONNECTION_STATE_ERROR_FATAL ||
				 status_requested == HANDLE_ERROR))
			{
				if (status_requested == HANDLE_IDLE && co_handles[i].transaction_status == 'I')
					connections[tran_count++] = &co_handles[i];
				else if (status_requested == HANDLE_ERROR && co_handles[i].transaction_status == 'E')
					connections[tran_count++] = &co_handles[i];
				else if (co_handles[i].transaction_status != 'I')
					connections[tran_count++] = &co_handles[i];
			}
		}
	}

	return tran_count;
}

/*
 * Collect node numbers for the given Datanode and Coordinator connections
 * and return it for prepared transactions
 */
PGXC_NodeId*
collect_pgxcnode_numbers(int conn_count, PGXCNodeHandle **connections, char client_conn_type)
{
	PGXC_NodeId *pgxcnodes = NULL;
	int i;

	/* It is also necessary to save in GTM the local Coordinator that is being prepared */
	if (client_conn_type == REMOTE_CONN_COORD)
		pgxcnodes = (PGXC_NodeId *) palloc((conn_count + 1) * sizeof(PGXC_NodeId));
	else
		pgxcnodes = (PGXC_NodeId *) palloc(conn_count * sizeof(PGXC_NodeId));

	if (!pgxcnodes)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	for (i = 0; i < conn_count; i++)
	{
		pgxcnodes[i] = connections[i]->nodenum;
	}

	/* Save here the Coordinator number where we are */
	if (client_conn_type == REMOTE_CONN_COORD)
		pgxcnodes[coord_count] = PGXCNodeId;

	return pgxcnodes;
}

/* Determine if the connection is active */
static bool
is_active_connection(PGXCNodeHandle *handle)
{
	return handle->sock != NO_SOCKET &&
				handle->state != DN_CONNECTION_STATE_IDLE &&
				!DN_CONNECTION_STATE_ERROR(handle);
}

/*
 * Return those node connections that appear to be active and
 * have data to consume on them.
 */
int
get_active_nodes(PGXCNodeHandle **connections)
{
	int			active_count = 0;
	int			i;

	if (datanode_count)
	{
		for (i = 0; i < NumDataNodes; i++)
		{
			if (is_active_connection(&dn_handles[i]))
				connections[active_count++] = &dn_handles[i];
		}
	}

	if (coord_count)
	{
		for (i = 0; i < NumCoords; i++)
		{
			if (is_active_connection(&co_handles[i]))
				connections[active_count++] = &co_handles[i];
		}
	}

	return active_count;
}

/*
 * Send gxid to all the handles except primary connection (treated separatly)
 */
int
pgxc_all_handles_send_gxid(PGXCNodeAllHandles *pgxc_handles, GlobalTransactionId gxid, bool stop_at_error)
{
	int dn_conn_count = pgxc_handles->dn_conn_count;
	int co_conn_count = pgxc_handles->co_conn_count;
	int i;
	int result = 0;

	if (pgxc_handles->primary_handle)
		dn_conn_count--;

	/* Send GXID to Datanodes */
	for (i = 0; i < dn_conn_count; i++)
	{
		if (pgxc_node_send_gxid(pgxc_handles->datanode_handles[i], gxid))
		{
			add_error_message(pgxc_handles->datanode_handles[i], "Can not send request");
			result = EOF;
			if (stop_at_error)
				goto finish;
		}
	}

	/* Send GXID to Coordinators handles */
	for (i = 0; i < co_conn_count; i++)
	{
		if (pgxc_node_send_gxid(pgxc_handles->coord_handles[i], gxid))
		{
			add_error_message(pgxc_handles->coord_handles[i], "Can not send request");
			result = EOF;
			if (stop_at_error)
				goto finish;
		}
	}

finish:
	return result;
}

/*
 * Send query to all the handles except primary connection (treated separatly)
 */
int
pgxc_all_handles_send_query(PGXCNodeAllHandles *pgxc_handles, const char *buffer, bool stop_at_error)
{
	int dn_conn_count = pgxc_handles->dn_conn_count;
	int co_conn_count = pgxc_handles->co_conn_count;
	int i;
	int result = 0;

	if (pgxc_handles->primary_handle)
		dn_conn_count--;

    /* Send to Datanodes */
    for (i = 0; i < dn_conn_count; i++)
	{
		if (pgxc_handles->datanode_handles[i]->state != DN_CONNECTION_STATE_IDLE)
		{
			pgxc_handles->datanode_handles[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			continue;
		}
		if (pgxc_node_send_query(pgxc_handles->datanode_handles[i], buffer))
		{
			add_error_message(pgxc_handles->datanode_handles[i], "Can not send request");
			result = EOF;
			if (stop_at_error)
				goto finish;
		}
	}
    /* Send to Coordinators */
    for (i = 0; i < co_conn_count; i++)
	{
		if (pgxc_node_send_query(pgxc_handles->coord_handles[i], buffer))
		{
			add_error_message(pgxc_handles->coord_handles[i], "Can not send request");
			result = EOF;
			if (stop_at_error)
				goto finish;
		}
	}

finish:
	return result;
}
