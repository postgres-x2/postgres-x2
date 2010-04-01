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
#include "combiner.h"
#include "nodes/pg_list.h"
#include "utils/snapshot.h"
#include <unistd.h>

/* Connection to data node maintained by Pool Manager */
typedef struct PGconn NODE_CONNECTION;

/* Helper structure to access data node from Session */
typedef enum
{
	DN_CONNECTION_STATE_IDLE,
	DN_CONNECTION_STATE_BUSY,
	DN_CONNECTION_STATE_COMPLETED,
	DN_CONNECTION_STATE_ERROR

}	DNConnectionState;

struct data_node_handle
{
	/* fd of the connection */
	int			sock;
	/* Connection state */
	char		transaction_status;
	DNConnectionState state;
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

/* Multinode Executor */
extern void DataNodeBegin(void);
extern int	DataNodeCommit(CommandDest dest);
extern int	DataNodeRollback(CommandDest dest);

extern int	DataNodeExec(const char *query, List *nodelist, CommandDest dest, Snapshot snapshot, bool force_autocommit, List *simple_aggregates, bool is_read_only);

#endif
