/*-------------------------------------------------------------------------
 *
 * datanode.c
 *
 *	  Functions for the coordinator communicating with the data nodes
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
#include "gtm/gtm_c.h"
#include "pgxc/datanode.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "tcop/dest.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "../interfaces/libpq/libpq-fe.h"

#define NO_SOCKET -1

static int	node_count = 0;
static DataNodeHandle *handles = NULL;

static void data_node_init(DataNodeHandle *handle, int sock, int nodenum);
static void data_node_free(DataNodeHandle *handle);

static int	get_int(DataNodeHandle * conn, size_t len, int *out);
static int	get_char(DataNodeHandle * conn, char *out);


/*
 * Allocate and initialize memory to store DataNode handles.
 */
void
InitMultinodeExecutor(void)
{
	int			i;

	/* This function could get called multiple times because of sigjmp */
	if (handles != NULL)
		return;

	/*
	 * Should be in TopMemoryContext.
	 * Assume the caller takes care of context switching
	 */
	handles = (DataNodeHandle *) palloc(NumDataNodes * sizeof(DataNodeHandle));
	if (!handles)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* initialize storage then */
	for (i = 0; i < NumDataNodes; i++)
	{
		/*
		 * Socket descriptor is small non-negative integer,
		 * Indicate the handle is not initialized yet
		 */
		handles[i].sock = NO_SOCKET;

		/* Initialise buffers */
		handles[i].error = NULL;
		handles[i].outSize = 16 * 1024;
		handles[i].outBuffer = (char *) palloc(handles[i].outSize);
		handles[i].inSize = 16 * 1024;
		handles[i].inBuffer = (char *) palloc(handles[i].inSize);

		if (handles[i].outBuffer == NULL || handles[i].inBuffer == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}
	}

	node_count = 0;
}

/*
 * Builds up a connection string
 */
char *
DataNodeConnStr(char *host, char *port, char *dbname,
				char *user, char *password)
{
	char	   *out,
				connstr[256];
	int			num;

	/* Build up connection string */
	num = snprintf(connstr, sizeof(connstr),
				   "host=%s port=%s dbname=%s user=%s password=%s",
				   host, port, dbname, user, password);

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
DataNodeConnect(char *connstr)
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
DataNodeClose(NODE_CONNECTION *conn)
{
	/* Delegate call to the pglib */
	PQfinish((PGconn *) conn);
}


/*
 * Checks if connection active
 */
int
DataNodeConnected(NODE_CONNECTION *conn)
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
data_node_free(DataNodeHandle *handle)
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
data_node_init(DataNodeHandle *handle, int sock, int nodenum)
{
	handle->nodenum = nodenum;
	handle->sock = sock;
	handle->transaction_status = 'I';
	handle->state = DN_CONNECTION_STATE_IDLE;
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
data_node_receive(const int conn_count,
				  DataNodeHandle ** connections, struct timeval * timeout)
{
	int			i,
				res_select,
				nfds = 0;
	fd_set		readfds;

	FD_ZERO(&readfds);
	for (i = 0; i < conn_count; i++)
	{
		/* If connection finised sending do not wait input from it */
		if (connections[i]->state == DN_CONNECTION_STATE_IDLE
				|| connections[i]->state == DN_CONNECTION_STATE_HAS_DATA)
			continue;

		/* prepare select params */
		if (nfds < connections[i]->sock)
			nfds = connections[i]->sock;

		FD_SET(connections[i]->sock, &readfds);
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
		DataNodeHandle *conn = connections[i];

		if (FD_ISSET(conn->sock, &readfds))
		{
			int	read_status = data_node_read_data(conn);

			if (read_status == EOF || read_status < 0)
			{
				add_error_message(conn, "unexpected EOF on datanode connection");
				elog(WARNING, "unexpected EOF on datanode connection");
				return EOF;
			}
			else
			{
				conn->state = DN_CONNECTION_STATE_HAS_DATA;
			}
		}
	}
	return 0;
}


/*
 * Read up incoming messages from the Data ndoe connection
 */
int
data_node_read_data(DataNodeHandle *conn)
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
get_char(DataNodeHandle * conn, char *out)
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
get_int(DataNodeHandle *conn, size_t len, int *out)
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
get_message(DataNodeHandle *conn, int *len, char **msg)
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
		conn->state = DN_CONNECTION_STATE_QUERY;
		conn->inCursor = conn->inStart;
		return '\0';
	}

	*msg = conn->inBuffer + conn->inCursor;
	conn->inCursor += *len;
	conn->inStart = conn->inCursor;
	return msgtype;
}


/* Release all data node connections back to pool and release occupied memory */
void
release_handles(void)
{
	int			i;
	int 		discard[NumDataNodes];
	int			ndisc = 0;

	if (node_count == 0)
		return;

	for (i = 0; i < NumDataNodes; i++)
	{
		DataNodeHandle *handle = &handles[i];

		if (handle->sock != NO_SOCKET)
		{
			if (handle->state != DN_CONNECTION_STATE_IDLE)
			{
				elog(WARNING, "Connection to data node %d has unexpected state %d and will be dropped", handle->nodenum, handle->state);
				discard[ndisc++] = handle->nodenum;
			}
			data_node_free(handle);
		}
	}
	PoolManagerReleaseConnections(ndisc, discard);
	node_count = 0;
}


/*
 * Ensure specified amount of data can fit to the incoming buffer and
 * increase it if necessary
 */
int
ensure_in_buffer_capacity(size_t bytes_needed, DataNodeHandle *handle)
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
ensure_out_buffer_capacity(size_t bytes_needed, DataNodeHandle *handle)
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
send_some(DataNodeHandle *handle, int len)
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
 * This method won't return until connection buffer is empty or error occurs
 * To ensure all data are on the wire before waiting for response
 */
int
data_node_flush(DataNodeHandle *handle)
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
 * Send specified statement down to the Data node
 */
int
data_node_send_query(DataNodeHandle * handle, const char *query)
{
	int			strLen = strlen(query) + 1;

	/* size + strlen */
	int			msgLen = 4 + strLen;

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

 	return data_node_flush(handle);
}


/*
 * Send the GXID down to the Data node
 */
int
data_node_send_gxid(DataNodeHandle *handle, GlobalTransactionId gxid)
{
	int			msglen = 8;
	int			i32;

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
 * Send the snapshot down to the Data node
 */
int
data_node_send_snapshot(DataNodeHandle *handle, Snapshot snapshot)
{
	int			msglen;
	int			nval;
	int			i;

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
 * Send the timestamp down to the Datanode
 */
int
data_node_send_timestamp(DataNodeHandle *handle, TimestampTz timestamp)
{
	int		msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
	uint32	n32;
	int64	i = (int64) timestamp;

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
add_error_message(DataNodeHandle *handle, const char *message)
{
	handle->transaction_status = 'E';
	handle->state = DN_CONNECTION_STATE_ERROR_NOT_READY;
	if (handle->error)
	{
		/* PGXCTODO append */
	}
	else
		handle->error = pstrdup(message);
}

/*
 * for specified list return array of DataNodeHandles
 * acquire from pool if needed.
 * the lenth of returned array is the same as of nodelist
 * Special case is empty or NIL nodeList, in this case return all the nodes.
 * The returned list should be pfree'd when no longer needed.
 */
DataNodeHandle **
get_handles(List *nodelist)
{
	DataNodeHandle **result;
	ListCell   *node_list_item;
	List	   *allocate = NIL;
	MemoryContext old_context;

	/* index of the result array */
	int			i = 0;

	/* Handles should be there while transaction lasts */
	old_context = MemoryContextSwitchTo(TopTransactionContext);
	/* If node list is empty execute request on current nodes */
	if (list_length(nodelist) == 0)
	{
		/*
		 * We do not have to zero the array - on success all items will be set
		 * to correct pointers, on error the array will be freed
		 */
		result = (DataNodeHandle **) palloc(NumDataNodes * sizeof(DataNodeHandle *));
		if (!result)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		for (i = 0; i < NumDataNodes; i++)
		{
			result[i] = &handles[i];
			if (handles[i].sock == NO_SOCKET)
				allocate = lappend_int(allocate, i + 1);
		}
	}
	else
	{
		/*
		 * We do not have to zero the array - on success all items will be set
		 * to correct pointers, on error the array will be freed
		 */
		result = (DataNodeHandle **) palloc(list_length(nodelist) * sizeof(DataNodeHandle *));
		if (!result)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		i = 0;
		foreach(node_list_item, nodelist)
		{
			int			node = lfirst_int(node_list_item);

			Assert(node > 0 && node <= NumDataNodes);

			result[i++] = &handles[node - 1];
			if (handles[node - 1].sock == NO_SOCKET)
				allocate = lappend_int(allocate, node);
		}
	}

	if (allocate)
	{
		int			j = 0;
		int		   *fds = PoolManagerGetConnections(allocate);

		if (!fds)
		{
			pfree(result);
			list_free(allocate);
			return NULL;
		}
		foreach(node_list_item, allocate)
		{
			int			node = lfirst_int(node_list_item);
			int			fdsock = fds[j++];

			data_node_init(&handles[node - 1], fdsock, node);
			node_count++;
		}
		pfree(fds);
		list_free(allocate);
	}

	/* restore context */
	MemoryContextSwitchTo(old_context);

	return result;
}


/*
 * Return handles involved into current transaction, to run commit or rollback
 * on them, as requested.
 * Transaction is not started on nodes when read-only statement is executed
 * on it, so we do not have to commit or rollback on those nodes.
 * Parameter should point to array able to store at least node_count pointers
 * to a DataNodeHandle structure.
 * The function returns number of pointers written to the connections array.
 * Remaining items in the array, if any, will be kept unchanged
 */
int
get_transaction_nodes(DataNodeHandle **connections)
{
	int			tran_count = 0;
	int			i;

	if (node_count)
	{
		for (i = 0; i < NumDataNodes; i++)
		{
			if (handles[i].sock != NO_SOCKET && handles[i].transaction_status != 'I')
				connections[tran_count++] = &handles[i];
		}
	}

	return tran_count;
}
