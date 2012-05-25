/*-------------------------------------------------------------------------
 *
 * execRemote.h
 *
 *	  Functions to execute commands on multiple Data Nodes
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H
#include "locator.h"
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "planner.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/snapshot.h"
#ifdef PGXC
#include "tcop/pquery.h"
#endif

/* GUC parameters */
extern bool EnforceTwoPhaseCommit;

/* Outputs of handle_response() */
#define RESPONSE_EOF EOF
#define RESPONSE_COMPLETE 0
#define RESPONSE_SUSPENDED 1
#define RESPONSE_TUPDESC 2
#define RESPONSE_DATAROW 3
#define RESPONSE_COPY 4
#define RESPONSE_BARRIER_OK 5

typedef enum
{
	REQUEST_TYPE_NOT_DEFINED,	/* not determined yet */
	REQUEST_TYPE_COMMAND,		/* OK or row count response */
	REQUEST_TYPE_QUERY,			/* Row description response */
	REQUEST_TYPE_COPY_IN,		/* Copy In response */
	REQUEST_TYPE_COPY_OUT		/* Copy Out response */
}	RequestType;

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag
{
	CmdType cmdType;						/* DML command type */
	char	data[COMPLETION_TAG_BUFSIZE];	/* execution result combination data */
} CombineTag;

/*
 * Represents a DataRow message received from a remote node.
 * Contains originating node number and message body in DataRow format without
 * message code and length. Length is separate field
 */
typedef struct RemoteDataRowData
{
	char	*msg;					/* last data row message */
	int 	msglen;					/* length of the data row message */
	int 	msgnode;				/* node number of the data row message */
} 	RemoteDataRowData;
typedef RemoteDataRowData *RemoteDataRow;

typedef struct RemoteQueryState
{
	ScanState	ss;						/* its first field is NodeTag */
	int			node_count;				/* total count of participating nodes */
	PGXCNodeHandle **connections;		/* data node connections being combined */
	int			conn_count;				/* count of active connections */
	int			current_conn;			/* used to balance load when reading from connections */
	CombineType combine_type;			/* see CombineType enum */
	int			command_complete_count; /* count of received CommandComplete messages */
	RequestType request_type;			/* see RequestType enum */
	TupleDesc	tuple_desc;				/* tuple descriptor to be referenced by emitted tuples */
	int			description_count;		/* count of received RowDescription messages */
	int			copy_in_count;			/* count of received CopyIn messages */
	int			copy_out_count;			/* count of received CopyOut messages */
	char		errorCode[5];			/* error code to send back to client */
	char	   *errorMessage;			/* error message to send back to client */
	char	   *errorDetail;			/* error detail to send back to client */
	bool		query_Done;				/* query has been sent down to data nodes */
	RemoteDataRowData currentRow;		/* next data ro to be wrapped into a tuple */
	/* TODO use a tuplestore as a rowbuffer */
	List 	   *rowBuffer;				/* buffer where rows are stored when connection
										 * should be cleaned for reuse by other RemoteQuery */
	/*
	 * To handle special case - if there is a simple sort and sort connection
	 * is buffered. If EOF is reached on a connection it should be removed from
	 * the array, but we need to know node number of the connection to find
	 * messages in the buffer. So we store nodenum to that array if reach EOF
	 * when buffering
	 */
	int 	   *tapenodes;
	/*
	 * While we are not supporting grouping use this flag to indicate we need
	 * to initialize collecting of aggregates from the DNs
	 */
	bool		initAggregates;
	void	   *tuplesortstate;			/* for merge sort */
	/* Simple DISTINCT support */
	FmgrInfo   *eqfunctions; 			/* functions to compare tuples */
	MemoryContext tmp_ctx;				/* separate context is needed to compare tuples */
	FILE	   *copy_file;      		/* used if copy_dest == COPY_FILE */
	uint64		processed;				/* count of data rows when running CopyOut */
	/* cursor support */
	char	   *cursor;					/* cursor name */
	char	   *update_cursor;			/* throw this cursor current tuple can be updated */
	int			cursor_count;			/* total count of participating nodes */
	PGXCNodeHandle **cursor_connections;/* data node connections being combined */
	/* Support for parameters */
	char	   *paramval_data;		/* parameter data, format is like in BIND */
	int			paramval_len;		/* length of parameter values data */

	int			eflags;			/* capability flags to pass to tuplestore */
	bool		eof_underlying; /* reached end of underlying plan? */
	Tuplestorestate *tuplestorestate;
	List	   *tlist_vars;				/* Vars extracted from target list */
	TupleTableSlot *fulltupleslot;		/* intermediate slot for applying quals */

}	RemoteQueryState;

/* Multinode Executor */
extern void PGXCNodeBegin(void);
extern void PGXCNodeSetBeginQuery(char *query_string);
extern void	PGXCNodeCommit(bool bReleaseHandles);
extern int	PGXCNodeRollback(void);
extern bool	PGXCNodePrepare(char *gid);
extern bool	PGXCNodeRollbackPrepared(char *gid);
extern void PGXCNodeCommitPrepared(char *gid);

/* Copy command just involves Datanodes */
extern PGXCNodeHandle** DataNodeCopyBegin(const char *query, List *nodelist, Snapshot snapshot, bool is_from);
extern int DataNodeCopyIn(char *data_row, int len, ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections);
extern uint64 DataNodeCopyOut(ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections, FILE* copy_file);
extern void DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int primary_dn_index, CombineType combine_type);
extern bool DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error);
extern int DataNodeCopyInBinaryForAll(char *msg_buf, int len, PGXCNodeHandle** copy_connections);

extern int ExecCountSlotsRemoteQuery(RemoteQuery *node);
extern RemoteQueryState *ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags);
extern TupleTableSlot* ExecRemoteQuery(RemoteQueryState *step);
extern void ExecEndRemoteQuery(RemoteQueryState *step);
extern void ExecRemoteUtility(RemoteQuery *node);

extern int handle_response(PGXCNodeHandle * conn, RemoteQueryState *combiner);
extern bool	is_data_node_ready(PGXCNodeHandle * conn);
extern void HandleCmdComplete(CmdType commandType, CombineTag *combine, const char *msg_body, size_t len);
extern bool FetchTuple(RemoteQueryState *combiner, TupleTableSlot *slot);
extern void BufferConnection(PGXCNodeHandle *conn);

extern void ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt);

extern int ParamListToDataRow(ParamListInfo params, char** result);

extern void ExecCloseRemoteStatement(const char *stmt_name, List *nodelist);
extern void PreCommit_Remote(char *prepareGID, bool preparedLocalNode);
extern char *PrePrepare_Remote(char *prepareGID, bool localNode, bool implicit);
extern void PostPrepare_Remote(char *prepareGID, char *nodestring, bool implicit);
extern bool	PreAbort_Remote(void);
extern void AtEOXact_Remote(void);
extern bool IsTwoPhaseCommitRequired(bool localWrite);
extern bool FinishRemotePreparedTransaction(char *prepareGID, bool commit);

/* Flags related to temporary objects included in query */
extern void ExecSetTempObjectIncluded(void);
extern bool ExecIsTempObjectIncluded(void);
extern void ExecRemoteQueryStandard(Relation resultRelationDesc, RemoteQueryState *resultRemoteRel, TupleTableSlot *slot);
#endif
