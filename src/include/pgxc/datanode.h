/*-------------------------------------------------------------------------
 *
 * datanode.h
 *
 *	  Utility functions to communicate to Data Node
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef DATANODE_H
#define DATANODE_H
#include "postgres.h"
#include "gtm/gtm_c.h"
#include "utils/timestamp.h"
#include "nodes/pg_list.h"
#include "utils/snapshot.h"
#include <unistd.h>

#define NO_SOCKET -1


/* Connection to data node maintained by Pool Manager */
typedef struct PGconn NODE_CONNECTION;

/* Helper structure to access data node from Session */
typedef enum
{
	DN_CONNECTION_STATE_IDLE,			/* idle, ready for query */
	DN_CONNECTION_STATE_QUERY,			/* query is sent, response expected */
	DN_CONNECTION_STATE_HAS_DATA,		/* buffer has data to process */
	DN_CONNECTION_STATE_COMPLETED,		/* query completed, no ReadyForQury yet */
	DN_CONNECTION_STATE_ERROR_NOT_READY,	/* error, but need ReadyForQuery message */
	DN_CONNECTION_STATE_ERROR_FATAL,	/* fatal error */
	DN_CONNECTION_STATE_COPY_IN,
	DN_CONNECTION_STATE_COPY_OUT
}	DNConnectionState;

#define DN_CONNECTION_STATE_ERROR(dnconn) \
	(dnconn)->state == DN_CONNECTION_STATE_ERROR_FATAL \
	|| (dnconn)->state == DN_CONNECTION_STATE_ERROR_NOT_READY

struct data_node_handle
{
	int 		nodenum; /* node identifier 1..NumDataNodes */
	/* fd of the connection */
	int			sock;
	/* Connection state */
	char		transaction_status;
	DNConnectionState state;
#ifdef DN_CONNECTION_DEBUG
	bool		have_row_desc;
#endif
	char	   *error;
	/* Output buffer */
	char	   *outBuffer;
	size_t		outSize;
	size_t		outEnd;
	/* Input buffer */
	char	   *inBuffer;
	size_t		inSize;
	size_t		inStart;
	size_t		inEnd;
	size_t		inCursor;
};
typedef struct data_node_handle DataNodeHandle;

extern void InitMultinodeExecutor(void);

/* Open/close connection routines (invoked from Pool Manager) */
extern char *DataNodeConnStr(char *host, char *port, char *dbname, char *user,
				char *password);
extern NODE_CONNECTION *DataNodeConnect(char *connstr);
extern void DataNodeClose(NODE_CONNECTION * conn);
extern int	DataNodeConnected(NODE_CONNECTION * conn);
extern int	DataNodeConnClean(NODE_CONNECTION * conn);
extern void DataNodeCleanAndRelease(int code, Datum arg);

extern DataNodeHandle **get_handles(List *nodelist);
extern void release_handles(bool force_drop);
extern int	get_transaction_nodes(DataNodeHandle ** connections);
extern int	get_active_nodes(DataNodeHandle ** connections);

extern int	ensure_in_buffer_capacity(size_t bytes_needed, DataNodeHandle * handle);
extern int	ensure_out_buffer_capacity(size_t bytes_needed, DataNodeHandle * handle);

extern int	data_node_send_query(DataNodeHandle * handle, const char *query);
extern int	data_node_send_gxid(DataNodeHandle * handle, GlobalTransactionId gxid);
extern int	data_node_send_snapshot(DataNodeHandle * handle, Snapshot snapshot);
extern int	data_node_send_timestamp(DataNodeHandle * handle, TimestampTz timestamp);

extern int data_node_receive(const int conn_count,
				  DataNodeHandle ** connections, struct timeval * timeout);
extern int	data_node_read_data(DataNodeHandle * conn);
extern int	send_some(DataNodeHandle * handle, int len);
extern int	data_node_flush(DataNodeHandle *handle);

extern char get_message(DataNodeHandle *conn, int *len, char **msg);

extern void add_error_message(DataNodeHandle * handle, const char *message);
extern void clear_socket_data (DataNodeHandle *conn);

#endif
