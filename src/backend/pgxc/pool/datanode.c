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

#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "pgxc/poolmgr.h"
#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "postgres.h"
#include "utils/snapmgr.h"
#include "gtm/gtm_c.h"
#include "pgxc/datanode.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "utils/elog.h"
#include "utils/memutils.h"


#define NO_SOCKET -1

static int	node_count = 0;
static DataNodeHandle *handles = NULL;
static bool autocommit = true;
static DataNodeHandle **write_node_list = NULL;
static int	write_node_count = 0;

static DataNodeHandle **get_handles(List *nodelist);
static int	get_transaction_nodes(DataNodeHandle ** connections);
static void release_handles(void);

static void data_node_init(DataNodeHandle * handle, int sock);
static void data_node_free(DataNodeHandle * handle);

static int	data_node_begin(int conn_count, DataNodeHandle ** connections, ResponseCombiner combiner, GlobalTransactionId gxid);
static int	data_node_commit(int conn_count, DataNodeHandle ** connections, ResponseCombiner combiner);
static int	data_node_rollback(int conn_count, DataNodeHandle ** connections, ResponseCombiner combiner);

static int	ensure_in_buffer_capacity(size_t bytes_needed, DataNodeHandle * handle);
static int	ensure_out_buffer_capacity(size_t bytes_needed, DataNodeHandle * handle);

static int	data_node_send_query(DataNodeHandle * handle, const char *query);
static int	data_node_send_gxid(DataNodeHandle * handle, GlobalTransactionId gxid);
static int	data_node_send_snapshot(DataNodeHandle * handle, Snapshot snapshot);

static void add_error_message(DataNodeHandle * handle, const char *message);

static int	data_node_read_data(DataNodeHandle * conn);
static int	handle_response(DataNodeHandle * conn, ResponseCombiner combiner, bool inErrorState);

static int	get_int(DataNodeHandle * conn, size_t len, int *out);
static int	get_char(DataNodeHandle * conn, char *out);

static void clear_write_node_list();

#define MAX_STATEMENTS_PER_TRAN 10

/* Variables to collect statistics */
static int	total_transactions = 0;
static int	total_statements = 0;
static int	total_autocommit = 0;
static int	nonautocommit_2pc = 0;
static int	autocommit_2pc = 0;
static int	current_tran_statements = 0;
static int *statements_per_transaction = NULL;
static int *nodes_per_transaction = NULL;

/*
 * statistics collection: count a statement
 */
static void
stat_statement()
{
	total_statements++;
	current_tran_statements++;
}

/*
 * To collect statistics: count a transaction
 */
static void
stat_transaction(int node_count)
{
	total_transactions++;
	if (autocommit)
		total_autocommit++;
	if (!statements_per_transaction)
	{
		statements_per_transaction = (int *) malloc((MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
		memset(statements_per_transaction, 0, (MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
	}
	if (current_tran_statements > MAX_STATEMENTS_PER_TRAN)
		statements_per_transaction[MAX_STATEMENTS_PER_TRAN]++;
	else
		statements_per_transaction[current_tran_statements]++;
	current_tran_statements = 0;
	if (node_count > 0 && node_count <= NumDataNodes)
	{
		if (!nodes_per_transaction)
		{
			nodes_per_transaction = (int *) malloc(NumDataNodes * sizeof(int));
			memset(nodes_per_transaction, 0, NumDataNodes * sizeof(int));
		}
		nodes_per_transaction[node_count - 1]++;
	}
}


/*
 * To collect statistics: count a two-phase commit on nodes
 */
static void
stat_2pc()
{
	if (autocommit)
		autocommit_2pc++;
	else
		nonautocommit_2pc++;
}


/*
 * Output collected statistics to the log
 */
static void
stat_log()
{
	elog(DEBUG1, "Total Transactions: %d Total Statements: %d", total_transactions, total_statements);
	elog(DEBUG1, "Autocommit: %d 2PC for Autocommit: %d 2PC for non-Autocommit: %d",
		 total_autocommit, autocommit_2pc, nonautocommit_2pc);
	if (total_transactions)
	{
		if (statements_per_transaction)
		{
			int			i;

			for (i = 0; i < MAX_STATEMENTS_PER_TRAN; i++)
				elog(DEBUG1, "%d Statements per Transaction: %d (%d%%)",
					 i, statements_per_transaction[i], statements_per_transaction[i] * 100 / total_transactions);
		}
		elog(DEBUG1, "%d+ Statements per Transaction: %d (%d%%)",
			 MAX_STATEMENTS_PER_TRAN, statements_per_transaction[MAX_STATEMENTS_PER_TRAN], statements_per_transaction[MAX_STATEMENTS_PER_TRAN] * 100 / total_transactions);
		if (nodes_per_transaction)
		{
			int			i;

			for (i = 0; i < NumDataNodes; i++)
				elog(DEBUG1, "%d Nodes per Transaction: %d (%d%%)",
					 i + 1, nodes_per_transaction[i], nodes_per_transaction[i] * 100 / total_transactions);
		}
	}
}

/*
 * Allocate and initialize memory to store DataNode handles.
 */
void
InitMultinodeExecutor()
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
DataNodeClose(NODE_CONNECTION * conn)
{
	/* Delegate call to the pglib */
	PQfinish((PGconn *) conn);
}


/* 
 * Checks if connection active 
 */
int
DataNodeConnected(NODE_CONNECTION * conn)
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
data_node_free(DataNodeHandle * handle)
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
data_node_init(DataNodeHandle * handle, int sock)
{
	handle->sock = sock;
	handle->transaction_status = 'I';
	handle->state = DN_CONNECTION_STATE_IDLE;
	handle->error = NULL;
	handle->outEnd = 0;
	handle->inStart = 0;
	handle->inEnd = 0;
	handle->inCursor = 0;
}


/*
 * Handle responses from the Data node connections 
 */
static int
data_node_receive_responses(int conn_count, DataNodeHandle ** connections,
						 struct timeval * timeout, ResponseCombiner combiner)
{
	int			result = 0;
	int			retry_count;
	bool		timed_out = false;
	bool		inErrorState = false;

	int			count = conn_count;
	DataNodeHandle *to_receive[conn_count];

	/* make a copy of the pointers to the connections */
	memcpy(to_receive, connections, conn_count * sizeof(DataNodeHandle *));

	/*
	 * Read results.
	 * Note we try and read from data node connections even if there is an error on one,
	 * so as to avoid reading incorrect results on the next statement.
	 * It might be better to just destroy these connections and tell the pool manager.
	 */
	while (count > 0)
	{
		int			i,
					res_select,
					nfds = 0;
		fd_set		readfds;

		FD_ZERO(&readfds);
		for (i = 0; i < count; i++)
		{
			/* note if a connection has error */
			if (!to_receive[i]
				|| to_receive[i]->state == DN_CONNECTION_STATE_ERROR
				|| to_receive[i]->sock >= 1024)
			{
				result = EOF;

				/* Handling is done, do not track this connection */
				count--;

				/* Move last connection in its place */
				if (i < count)
				{
					to_receive[i] = to_receive[count];
					/* stay on the current position */
					i--;
				}
				continue;
			}

			/* prepare select params */
			if (nfds < to_receive[i]->sock)
				nfds = to_receive[i]->sock;

			FD_SET		(to_receive[i]->sock, &readfds);
		}

		/* Make sure we still have valid connections */
		if (count == 0)
			break;

		retry_count = 0;
retry:
		res_select = select(nfds + 1, &readfds, NULL, NULL, timeout);
		if (res_select < 0)
		{
			/* error - retry if EINTR or EAGAIN */
			if (errno == EINTR || errno == EAGAIN)
				goto retry;

			/*
			 * PGXCTODO - we may want to close the connections and notify the
			 * pooler that these are invalid.
			 */
			if (errno == EBADF)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("select() bad file descriptor set")));
				return EOF;
			}
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("select() error: %d", errno)));
			return EOF;
		}

		if (res_select == 0)
		{
			/* Handle timeout */
			result = EOF;
			timed_out = true;
		}

		/* read data */
		for (i = 0; i < count; i++)
		{
			DataNodeHandle *conn = to_receive[i];

			if (FD_ISSET(conn->sock, &readfds))
			{
				int			read_status = data_node_read_data(conn);

				if (read_status == EOF || read_status < 0)
				{
					count--;
					/* Move last connection in place */
					if (i < count)
					{
						to_receive[i] = to_receive[count];
						/* stay on the current position */
						i--;
					}

					inErrorState = true;
					result = EOF;
					continue;
				}
			}

			if (conn->inStart < conn->inEnd)
			{
				if (handle_response(conn, combiner, inErrorState) == 0)
				{
					/* Handling is done, do not track this connection */
					count--;
					/* Move last connection in place */
					if (i < count)
					{
						to_receive[i] = to_receive[count];
						/* stay on the current position */
						i--;
					}
				}

				/*
				 * See if we flagged an error on connection. Note, if
				 * handle_response was not 0 above, an error occurred, we
				 * still need to consume the ReadyForQuery message
				 */
				if (conn->state == DN_CONNECTION_STATE_ERROR)
				{
					inErrorState = true;
					result = EOF;
				}
			}
		}
	}

	return result;
}

/*
 * Read up incoming messages from the Data ndoe connection
 */
static int
data_node_read_data(DataNodeHandle * conn)
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
			conn->state = DN_CONNECTION_STATE_ERROR;	/* No more connection to
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
get_int(DataNodeHandle * conn, size_t len, int *out)
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
 * Read next message from the connection and update the combiner accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 */
static int
handle_response(DataNodeHandle * conn, ResponseCombiner combiner, bool inErrorState)
{
	char		msg_type;
	int			msg_len;
	bool		connError = false;

	for (;;)
	{
		/* try to read the message, return if not enough data */
		conn->inCursor = conn->inStart;
		if (conn->inEnd - conn->inCursor < 5)
			return EOF;

		if (get_char(conn, &msg_type))
			return EOF;

		if (get_int(conn, 4, &msg_len))
			return EOF;

		msg_len -= 4;

		if (conn->inEnd - conn->inCursor < msg_len)
		{
			ensure_in_buffer_capacity(conn->inCursor + (size_t) msg_len, conn);
			return EOF;
		}

		/* TODO handle other possible responses */
		switch (msg_type)
		{
			case 'C':			/* CommandComplete */
				/* no need to parse, just move cursor */
				conn->inCursor += msg_len;
				conn->state = DN_CONNECTION_STATE_COMPLETED;
				if (!inErrorState)
					CombineResponse(combiner, msg_type,
									conn->inBuffer + conn->inStart + 5,
									conn->inCursor - conn->inStart - 5);

				break;
			case 'T':			/* RowDescription */
			case 'G':			/* CopyInResponse */
			case 'H':			/* CopyOutResponse */
			case 'D':			/* DataRow */
				/* no need to parse, just move cursor */
				conn->inCursor += msg_len;
				if (!inErrorState)
					CombineResponse(combiner, msg_type,
									conn->inBuffer + conn->inStart + 5,
									conn->inCursor - conn->inStart - 5);
				break;
			case 'E':			/* ErrorResponse */
				/* no need to parse, just move cursor */
				conn->inCursor += msg_len;
				if (!inErrorState)
					CombineResponse(combiner, msg_type,
									conn->inBuffer + conn->inStart + 5,
									conn->inCursor - conn->inStart - 5);
				conn->inStart = conn->inCursor;
				connError = inErrorState = true;
				/* conn->state = DN_CONNECTION_STATE_ERROR;  */

				/*
				 * Do not return with an error, we still need to consume Z,
				 * ready-for-query
				 */
				break;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
				conn->inCursor += msg_len;

				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
				get_char(conn, &conn->transaction_status);
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->inStart = conn->inCursor;
				/* Now it is ok to flag the connection as having an error */
				if (connError)
				{
					conn->state = DN_CONNECTION_STATE_ERROR;
					return EOF;
				}
				return 0;
			case 'I':			/* EmptyQuery */
			default:
				/* sync lost? */
				conn->state = DN_CONNECTION_STATE_ERROR;
				inErrorState = true;
				return EOF;
		}
		conn->inStart = conn->inCursor;

	}
	return EOF;
}


/*
 * Send BEGIN command to the Data nodes and receive responses
 */
static int
data_node_begin(int conn_count, DataNodeHandle ** connections, ResponseCombiner combiner, GlobalTransactionId gxid)
{
	int			i;
	struct timeval *timeout = NULL;

	/* Send BEGIN */
	for (i = 0; i < conn_count; i++)
	{
		if (GlobalTransactionIdIsValid(gxid) && data_node_send_gxid(connections[i], gxid))
			return EOF;

		if (data_node_send_query(connections[i], "BEGIN"))
			return EOF;
	}

	/* Receive responses */
	if (data_node_receive_responses(conn_count, connections, timeout, combiner))
		return EOF;

	/* Verify status? */

	return 0;
}


/* Clears the write node list */
static void
clear_write_node_list()
{
	/* we just malloc once and use counter */
	if (write_node_list == NULL)
	{
		write_node_list = (DataNodeHandle **) malloc(NumDataNodes * sizeof(DataNodeHandle *));
	}
	write_node_count = 0;
}


/*
 * Switch autocommmit mode off, so all subsequent statements will be in the same transaction
 */
void
DataNodeBegin(void)
{
	autocommit = false;
	clear_write_node_list();
}


/*
 * Commit current transaction, use two-phase commit if necessary
 */
int
DataNodeCommit(CommandDest dest)
{
	int			res;
	int			tran_count;
	DataNodeHandle *connections[node_count];
	ResponseCombiner combiner;

	/* Quick check to make sure we have connections */
	if (node_count == 0)
		goto finish;

	/* gather connections to commit */
	tran_count = get_transaction_nodes(connections);

	/*
	 * If we do not have open transactions we have nothing to commit, just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	combiner = CreateResponseCombiner(tran_count,
									  COMBINE_TYPE_NONE, dest);
	res = data_node_commit(tran_count, connections, combiner);
	if (!ValidateAndCloseCombiner(combiner) || res)
		return EOF;

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(node_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	clear_write_node_list();
	return 0;
}


/*
 * Send COMMIT or PREPARE/COMMIT PREPARED down to the Data nodes and handle responses
 */
static int
data_node_commit(int conn_count, DataNodeHandle ** connections, ResponseCombiner combiner)
{
	int			i;
	struct timeval *timeout = NULL;
	char		buffer[256];
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	int			result = 0;


	/* can set this to false to disable temporarily */
	/* bool do2PC = conn_count > 1; */

	/*
	 * Only use 2PC if more than one node was written to. Otherwise, just send
	 * COMMIT to all
	 */
	bool		do2PC = write_node_count > 1;

	/* Extra XID for Two Phase Commit */
	GlobalTransactionId two_phase_xid = 0;

	if (do2PC)
	{
		stat_2pc();

		/*
		 * Formally we should be using GetCurrentGlobalTransactionIdIfAny() here,
		 * but since we need 2pc, we surely have sent down a command and got
		 * gxid for it. Hence GetCurrentGlobalTransactionId() just returns
		 * already allocated gxid
		 */
/* #ifdef PGXC_COORD  */
		gxid = GetCurrentGlobalTransactionId();
/* #endif */

		sprintf(buffer, "PREPARE TRANSACTION 'T%d'", gxid);
		/* Send PREPARE */
		for (i = 0; i < conn_count; i++)
		{
			if (data_node_send_query(connections[i], buffer))
				return EOF;
		}

		/* Receive responses */
		if (data_node_receive_responses(conn_count, connections, timeout, combiner))
			return EOF;

		/* Reset combiner */
		if (!ValidateAndResetCombiner(combiner))
			return EOF;
	}

	if (!do2PC)
		strcpy(buffer, "COMMIT");
	else
	{
		sprintf(buffer, "COMMIT PREPARED 'T%d'", gxid);

		/* We need to use a new xid, the data nodes have reset */
		two_phase_xid = BeginTranGTM();
		for (i = 0; i < conn_count; i++)
		{
			if (data_node_send_gxid(connections[i], two_phase_xid))
			{
				add_error_message(connections[i], "Can not send request");
				result = EOF;
				goto finish;
			}
		}
	}

	/* Send COMMIT */
	for (i = 0; i < conn_count; i++)
	{
		if (data_node_send_query(connections[i], buffer))
		{
			result = EOF;
			goto finish;
		}
	}

	/* Receive responses */
	if (data_node_receive_responses(conn_count, connections, timeout, combiner))
		result = EOF;

finish:
	if (do2PC)
		CommitTranGTM((GlobalTransactionId) two_phase_xid);

	return result;
}


/*
 * Rollback current transaction
 */
int
DataNodeRollback(CommandDest dest)
{
	int			res = 0;
	int			tran_count;
	DataNodeHandle *connections[node_count];
	ResponseCombiner combiner;
	int			i;

	/* Quick check to make sure we have connections */
	if (node_count == 0)
		goto finish;

	/* gather connections to rollback */
	tran_count = get_transaction_nodes(connections);

	/*
	 * If we do not have open transactions we have nothing to rollback just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	combiner = CreateResponseCombiner(tran_count,
									  COMBINE_TYPE_NONE, dest);
	res = data_node_rollback(tran_count, connections, combiner);

	/* Assume connection got cleaned up. Reset so we can reuse without error. */
	for (i = 0; i < tran_count; i++)
	{
		connections[i]->transaction_status = 'I';
		connections[i]->state = DN_CONNECTION_STATE_IDLE;
	}

	if (!ValidateAndCloseCombiner(combiner) || res)
		res = EOF;

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(node_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	clear_write_node_list();
	return res;
}


/* Release all data node connections back to pool and release occupied memory */
static void
release_handles(void)
{
	int			i;

	if (node_count == 0)
		return;

	PoolManagerReleaseConnections();
	for (i = 0; i < NumDataNodes; i++)
	{
		DataNodeHandle *handle = &handles[i];

		if (handle->sock != NO_SOCKET)
			data_node_free(handle);
	}

	node_count = 0;
}


/*
 * Send ROLLBACK command down to the Data nodes and handle responses
 */
static int
data_node_rollback(int conn_count, DataNodeHandle ** connections, ResponseCombiner combiner)
{
	int			i;
	struct timeval *timeout = NULL;
	int			result = 0;

	/* Send ROLLBACK - */
	for (i = 0; i < conn_count; i++)
	{
		if (data_node_send_query(connections[i], "ROLLBACK"))
			result = EOF;
	}

	/* Receive responses */
	if (data_node_receive_responses(conn_count, connections, timeout, combiner))
		return EOF;

	/* Verify status? */
	return 0;
}


/*
 * Execute specified statement on specified Data nodes, combine responses and
 * send results back to the client
 *
 * const char *query       - SQL string to execute
 * List *primarynode       - if a write operation on a replicated table, the primary node
 * List *nodelist          - the nodes to execute on (excludes primary, if set in primarynode
 * CommandDest dest        - destination for results
 * Snapshot snapshot       - snapshot to use
 * bool force_autocommit   - force autocommit
 * List *simple_aggregates - list of simple aggregates to execute
 * bool is_read_only       - if this is a read-only query
 */
int
DataNodeExec(const char *query, Exec_Nodes *exec_nodes, CommandDest dest, Snapshot snapshot,
		   bool force_autocommit, List *simple_aggregates, bool is_read_only)
{
	int			i;
	int			j;
	int			regular_conn_count;
	int			total_conn_count;
	struct timeval *timeout = NULL;		/* wait forever */
	ResponseCombiner combiner;
	ResponseCombiner primary_combiner = NULL;
	int			primary_row_count = 0;
	int			row_count = 0;
	int			res;
	int			new_count = 0;
	bool		need_tran;
	bool		found;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	DataNodeHandle *new_connections[NumDataNodes];
	DataNodeHandle **connections = NULL;
	DataNodeHandle **primaryconnection = NULL;
	List *nodelist = NIL;
	List *primarynode = NIL;
	/* add up affected row count by default, override for replicated writes */
	CombineType combine_type = COMBINE_TYPE_SUM; 


	if (exec_nodes)
	{
		nodelist = exec_nodes->nodelist;
		primarynode = exec_nodes->primarynodelist;
	}

	if (list_length(nodelist) == 0)
	{
		if (primarynode)
			regular_conn_count = NumDataNodes - 1;
		else
			regular_conn_count = NumDataNodes;
	}
	else
	{
		regular_conn_count = list_length(nodelist);
	}

	total_conn_count = regular_conn_count;

	/* Get connection for primary node, if used */
	if (primarynode)
	{
		primaryconnection = get_handles(primarynode);
		total_conn_count++;
	}

	/* Get other connections (non-primary) */
	if (regular_conn_count == 0)
		return EOF;
	else
	{
		connections = get_handles(nodelist);
		if (!connections)
			return EOF;
	}

	if (force_autocommit)
		need_tran = false;
	else
		need_tran = !autocommit || total_conn_count > 1;

	elog(DEBUG1, "autocommit = %s, has primary = %s, regular_conn_count = %d, need_tran = %s", autocommit ? "true" : "false", primarynode ? "true" : "false", regular_conn_count, need_tran ? "true" : "false");

	stat_statement();
	if (autocommit)
		stat_transaction(total_conn_count);

	/* We normally clear for transactions, but if autocommit, clear here, too */
	if (autocommit == true)
	{
		clear_write_node_list();
	}

	/* Check status of connections */

	/*
	 * We want to track new "write" nodes, and new nodes in the current
	 * transaction whether or not they are write nodes.
	 */
	if (!is_read_only && write_node_count < NumDataNodes)
	{
		if (primaryconnection)
		{
			found = false;
			for (j = 0; j < write_node_count && !found; j++)
			{
				if (write_node_list[j] == primaryconnection[0])
					found = true;
			}
			if (!found)
			{
				/* Add to transaction wide-list */
				write_node_list[write_node_count++] = primaryconnection[0];
				/* Add to current statement list */
				new_connections[new_count++] = primaryconnection[0];
			}
		}
		for (i = 0; i < regular_conn_count; i++)
		{
			found = false;
			for (j = 0; j < write_node_count && !found; j++)
			{
				if (write_node_list[j] == connections[i])
					found = true;
			}
			if (!found)
			{
				/* Add to transaction wide-list */
				write_node_list[write_node_count++] = connections[i];
				/* Add to current statement list */
				new_connections[new_count++] = connections[i];
			}
		}
		/* Check connection state is DN_CONNECTION_STATE_IDLE */
	}

	gxid = GetCurrentGlobalTransactionId();

	if (!GlobalTransactionIdIsValid(gxid))
	{
		pfree(connections);
		return EOF;
	}
	if (new_count > 0 && need_tran)
	{
		combiner = CreateResponseCombiner(new_count, COMBINE_TYPE_NONE, DestNone);

		/* Start transaction on connections where it is not started */
		res = data_node_begin(new_count, new_connections, combiner, gxid);
		if (!ValidateAndCloseCombiner(combiner) || res)
		{
			pfree(connections);
			return EOF;
		}
	}

	/* See if we have a primary nodes, execute on it first before the others */
	if (primaryconnection)
	{
		/* If explicit transaction is needed gxid is already sent */
		if (!need_tran && data_node_send_gxid(primaryconnection[0], gxid))
		{
			add_error_message(primaryconnection[0], "Can not send request");
   			if (connections) 
				pfree(connections);
			if (primaryconnection) 
				pfree(primaryconnection);
			return EOF;
		}
		if (snapshot && data_node_send_snapshot(primaryconnection[0], snapshot))
		{
			add_error_message(primaryconnection[0], "Can not send request");
   			if (connections) 
				pfree(connections);
			pfree(primaryconnection);
			return EOF;
		}
		if (data_node_send_query(primaryconnection[0], query) != 0)
		{
			add_error_message(primaryconnection[0], "Can not send request");
   			if (connections) 
				pfree(connections);
			if (primaryconnection) 
				pfree(primaryconnection);
			return EOF;
		}

		/* Use DestNone so that a response will not be sent until run on secondary nodes */
		/* Use COMBINE_TYPE_SAME so that we get the affected row count */
		primary_combiner = CreateResponseCombiner(1, COMBINE_TYPE_SAME, DestNone);

		/* Receive responses */
		res = data_node_receive_responses(1, primaryconnection, timeout, primary_combiner);
		primary_row_count = primary_combiner->row_count;
		if (!ValidateAndCloseCombiner(primary_combiner) || res)
		{
			if (autocommit)
			{
				if (need_tran)
					DataNodeRollback(DestNone);
				else if (!PersistentConnections)
					release_handles();
			}

			if (primaryconnection) 
				pfree(primaryconnection);
   			if (connections) 
				pfree(connections);
			return EOF;
		}
		/* 
		 * If we get here, the statement has executed successfully on the primary data node 
		 * and it is ok to try and execute on the other data nodes.
		 * Set combine_type for secondary so that we do not sum up row counts.
		 */
		combine_type = COMBINE_TYPE_SAME;
	}

	/* Send query to nodes */
	for (i = 0; i < regular_conn_count; i++)
	{
		/* If explicit transaction is needed gxid is already sent */
		if (!need_tran && data_node_send_gxid(connections[i], gxid))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			return EOF;
		}
		if (snapshot && data_node_send_snapshot(connections[i], snapshot))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			return EOF;
		}
		if (data_node_send_query(connections[i], query) != 0)
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			return EOF;
		}
	}

	combiner = CreateResponseCombiner(regular_conn_count, combine_type, dest);
	AssignCombinerAggregates(combiner, simple_aggregates);

	/* Receive responses */
	res = data_node_receive_responses(regular_conn_count, connections, timeout, combiner);
	row_count = combiner->row_count;

	/* Check for errors and if primary nodeMake sure primary and secondary nodes were updated the same */
	if (!ValidateAndCloseCombiner(combiner) || res
			|| (primaryconnection && row_count != primary_row_count))
	{
		if (autocommit)
		{
			if (need_tran)
				DataNodeRollback(DestNone);
			else if (!PersistentConnections)
				release_handles();
		}

		pfree(connections);

		if (primaryconnection && row_count != primary_row_count)
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Write to replicated table returned different results between primary and secondary data nodes. Primary: %d, secondary: %d", primary_row_count, row_count
						)));

		return EOF;
	}

	if (autocommit)
	{
		if (need_tran)
			DataNodeCommit(DestNone);	/* PGXCTODO - call CommitTransaction()
										 * instead? */
		else if (!PersistentConnections)
			release_handles();
	}

	/* Verify status? */
	pfree(connections);
	return 0;
}


/*
 * Ensure specified amount of data can fit to the incoming buffer and
 * increase it if necessary
 */
static int
ensure_in_buffer_capacity(size_t bytes_needed, DataNodeHandle * handle)
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
static int
ensure_out_buffer_capacity(size_t bytes_needed, DataNodeHandle * handle)
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
static int
send_some(DataNodeHandle * handle, int len)
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
 * Send specified statement down to the Data node
 */
static int
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

	/* We need response right away, so send immediately */
	if (send_some(handle, handle->outEnd) < 0)
		return EOF;

	handle->state = DN_CONNECTION_STATE_BUSY;

	return 0;
}


/*
 * Send the GXID down to the Data node
 */
static int
data_node_send_gxid(DataNodeHandle * handle, GlobalTransactionId gxid)
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
static int
data_node_send_snapshot(DataNodeHandle * handle, Snapshot snapshot)
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
 * Add another message to the list of errors to be returned back to the client
 * at the convenient time
 */
static void
add_error_message(DataNodeHandle * handle, const char *message)
{
	handle->transaction_status = 'E';
	handle->state = DN_CONNECTION_STATE_ERROR;
	if (handle->error)
	{
		/* PGXCTODO append */
	}
	else
	{
		handle->error = pstrdup(message);
	}
}

/*
 * for specified list return array of DataNodeHandles
 * acquire from pool if needed.
 * the lenth of returned array is the same as of nodelist
 * Special case is empty or NIL nodeList, in this case return all the nodes.
 * The returned list should be pfree'd when no longer needed.
 */
static DataNodeHandle **
get_handles(List *nodelist)
{
	DataNodeHandle **result;
	ListCell   *node_list_item;
	List	   *allocate = NIL;

	/* index of the result array */
	int			i = 0;

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
			int			node = node_list_item->data.int_value;

			if (node > NumDataNodes || node <= 0)
				elog(ERROR, "Node number: %d passed is not a known node", node);
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
			int			node = node_list_item->data.int_value;
			int			fdsock = fds[j++];

			data_node_init(&handles[node - 1], fdsock);
			node_count++;
		}
		pfree(fds);
		list_free(allocate);
	}

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
static int
get_transaction_nodes(DataNodeHandle ** connections)
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


/*
 * Called when the backend is ending.
 */
void
DataNodeCleanAndRelease(int code, Datum arg)
{
	/* Rollback on Data Nodes */
	if (IsTransactionState())
	{
		DataNodeRollback(DestNone);

		/* Rollback on GTM if transaction id opened. */
		RollbackTranGTM((GlobalTransactionId) GetCurrentTransactionIdIfAny());
	}

	/* Release data node connections */
	release_handles();

	/* Close connection with GTM */
	CloseGTM();

	/* Dump collected statistics to the log */
	stat_log();
}
