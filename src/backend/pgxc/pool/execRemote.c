/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote data nodes
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>
#include "postgres.h"
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"

#define END_QUERY_TIMEOUT	20
#define DATA_NODE_FETCH_SIZE 1


/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD 1024 * 1024

static bool autocommit = true;
static bool is_ddl = false;
static bool implicit_force_autocommit = false;
static bool temp_object_included = false;
static PGXCNodeHandle **write_node_list = NULL;
static int	write_node_count = 0;
static char *begin_string = NULL;

static bool analyze_node_string(char *nodestring,
								List **datanodelist,
								List **coordlist);
static int	pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
				GlobalTransactionId gxid);
static int	pgxc_node_commit(PGXCNodeAllHandles * pgxc_handles);
static int	pgxc_node_rollback(PGXCNodeAllHandles * pgxc_handles);
static int	pgxc_node_prepare(PGXCNodeAllHandles * pgxc_handles, char *gid);
static int	pgxc_node_rollback_prepared(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
				PGXCNodeAllHandles * pgxc_handles, char *gid);
static int	pgxc_node_commit_prepared(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
				PGXCNodeAllHandles * pgxc_handles, char *gid);
static PGXCNodeAllHandles * get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type);
static int	pgxc_node_implicit_commit_prepared(GlobalTransactionId prepare_xid,
											   GlobalTransactionId commit_xid,
											   PGXCNodeAllHandles * pgxc_handles,
											   char *gid,
											   bool is_commit);
static int	pgxc_node_implicit_prepare(GlobalTransactionId prepare_xid,
				PGXCNodeAllHandles * pgxc_handles, char *gid);

static int	pgxc_node_receive_and_validate(const int conn_count,
										   PGXCNodeHandle ** connections,
										   bool reset_combiner);
static void clear_write_node_list(void);

static void close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor);

static PGXCNodeAllHandles *pgxc_get_all_transaction_nodes(PGXCNode_HandleRequested status_requested);
static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
					bool need_tran, GlobalTransactionId gxid, TimestampTz timestamp,
					RemoteQueryState *remotestate, int total_conn_count, Snapshot snapshot);
static bool ExecRemoteQueryInnerPlan(RemoteQueryState *node);
static TupleTableSlot * RemoteQueryNext(RemoteQueryState *node);

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


#ifdef NOT_USED
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
#endif


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
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
static RemoteQueryState *
CreateResponseCombiner(int node_count, CombineType combine_type)
{
	RemoteQueryState *combiner;

	/* ResponseComber is a typedef for pointer to ResponseCombinerData */
	combiner = makeNode(RemoteQueryState);
	if (combiner == NULL)
	{
		/* Out of memory */
		return combiner;
	}

	combiner->node_count = node_count;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->combine_type = combine_type;
	combiner->command_complete_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->tuple_desc = NULL;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->errorMessage = NULL;
	combiner->errorDetail = NULL;
	combiner->query_Done = false;
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
	combiner->initAggregates = true;
	combiner->copy_file = NULL;

	return combiner;
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
	int			digits = 0;
	int			pos;

	*rowcount = 0;
	/* skip \0 string terminator */
	for (pos = 0; pos < len - 1; pos++)
	{
		if (message[pos] >= '0' && message[pos] <= '9')
		{
			*rowcount = *rowcount * 10 + message[pos] - '0';
			digits++;
		}
		else
		{
			*rowcount = 0;
			digits = 0;
		}
	}
	return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
static TupleDesc
create_tuple_desc(char *msg_body, size_t len)
{
	TupleDesc 	result;
	int 		i, nattr;
	uint16		n16;

	/* get number of attributes */
	memcpy(&n16, msg_body, 2);
	nattr = ntohs(n16);
	msg_body += 2;

	result = CreateTemplateTupleDesc(nattr, false);

	/* decode attributes */
	for (i = 1; i <= nattr; i++)
	{
		AttrNumber	attnum;
		char		*attname;
		char		*typname;
		Oid 		oidtypeid;
		int32 		typemode, typmod;

		attnum = (AttrNumber) i;

		/* attribute name */
		attname = msg_body;
		msg_body += strlen(attname) + 1;

		/* type name */
		typname = msg_body;
		msg_body += strlen(typname) + 1;

		/* table OID, ignored */
		msg_body += 4;

		/* column no, ignored */
		msg_body += 2;

		/* data type OID, ignored */
		msg_body += 4;

		/* type len, ignored */
		msg_body += 2;

		/* type mod */
		memcpy(&typemode, msg_body, 4);
		typmod = ntohl(typemode);
		msg_body += 4;

		/* PGXCTODO text/binary flag? */
		msg_body += 2;

		/* Get the OID type and mode type from typename */
		parseTypeString(typname, &oidtypeid, NULL);

		TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
	}
	return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a data node connection
 */
static void
HandleCopyOutComplete(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'c' message, current request type %d", combiner->request_type)));
	/* Just do nothing, close message is managed by the coordinator */
	combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a data node connection
 */
static void
HandleCommandComplete(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	int 			digits = 0;
	EState		   *estate = combiner->ss.ps.state;

	/*
	 * If we did not receive description we are having rowcount or OK response
	 */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;
	/* Extract rowcount */
	if (combiner->combine_type != COMBINE_TYPE_NONE && estate)
	{
		uint64	rowcount;
		digits = parse_row_count(msg_body, len, &rowcount);
		if (digits > 0)
		{
			/* Replicated write, make sure they are the same */
			if (combiner->combine_type == COMBINE_TYPE_SAME)
			{
				if (combiner->command_complete_count)
				{
					if (rowcount != estate->es_processed)
						/* There is a consistency issue in the database with the replicated table */
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("Write to replicated table returned different results from the data nodes")));
				}
				else
					/* first result */
					estate->es_processed = rowcount;
			}
			else
				estate->es_processed += rowcount;
		}
		else
			combiner->combine_type = COMBINE_TYPE_NONE;
	}

	combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a data node connection
 */
static bool
HandleRowDescription(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'T' message, current request type %d", combiner->request_type)));
	}
	/* Increment counter and check if it was first */
	if (combiner->description_count++ == 0)
	{
		combiner->tuple_desc = create_tuple_desc(msg_body, len);
		return true;
	}
	return false;
}


#ifdef NOT_USED
/*
 * Handle ParameterStatus ('S') message from a data node connection (SET command)
 */
static void
HandleParameterStatus(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'S' message, current request type %d", combiner->request_type)));
	}
	/* Proxy last */
	if (++combiner->description_count == combiner->node_count)
	{
		pq_putmessage('S', msg_body, len);
	}
}
#endif

/*
 * Handle CopyInResponse ('G') message from a data node connection
 */
static void
HandleCopyIn(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_IN;
	if (combiner->request_type != REQUEST_TYPE_COPY_IN)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'G' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an G message when it runs in the
	 * coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a data node connection
 */
static void
HandleCopyOut(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'H' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an H message when it runs in the
	 * coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a data node connection
 */
static void
HandleCopyDataRow(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	/* Inconsistent responses */
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'd' message, current request type %d", combiner->request_type)));

	/* count the row */
	combiner->processed++;

	/* If there is a copy file, data has to be sent to the local file */
	if (combiner->copy_file)
		/* write data to the copy file */
		fwrite(msg_body, 1, len, combiner->copy_file);
	else
		pq_putmessage('d', msg_body, len);
}

/*
 * Handle DataRow ('D') message from a data node connection
 * The function returns true if buffer can accept more data rows.
 * Caller must stop reading if function returns false
 */
static void
HandleDataRow(RemoteQueryState *combiner, char *msg_body, size_t len, int nid)
{
	/* We expect previous message is consumed */
	Assert(combiner->currentRow.msg == NULL);

	if (nid < 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("invalid node id %d",
						nid)));

	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'D' message, current request type %d", combiner->request_type)));
	}

	/*
	 * If we got an error already ignore incoming data rows from other nodes
	 * Still we want to continue reading until get CommandComplete
	 */
	if (combiner->errorMessage)
		return;

	/*
	 * We are copying message because it points into connection buffer, and
	 * will be overwritten on next socket read
	 */
	combiner->currentRow.msg = (char *) palloc(len);
	memcpy(combiner->currentRow.msg, msg_body, len);
	combiner->currentRow.msglen = len;
	combiner->currentRow.msgnode = nid;
}

/*
 * Handle ErrorResponse ('E') message from a data node connection
 */
static void
HandleError(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	/* parse error message */
	char *severity = NULL;
	char *code = NULL;
	char *message = NULL;
	char *detail = NULL;
	char *hint = NULL;
	char *position = NULL;
	char *int_position = NULL;
	char *int_query = NULL;
	char *where = NULL;
	char *file = NULL;
	char *line = NULL;
	char *routine = NULL;
	int   offset = 0;

	/*
	 * Scan until point to terminating \0
	 */
	while (offset + 1 < len)
	{
		/* pointer to the field message */
		char *str = msg_body + offset + 1;

		switch (msg_body[offset])
		{
			case 'S':
				severity = str;
				break;
			case 'C':
				code = str;
				break;
			case 'M':
				message = str;
				break;
			case 'D':
				detail = str;
				break;
			case 'H':
				hint = str;
				break;
			case 'P':
				position = str;
				break;
			case 'p':
				int_position = str;
				break;
			case 'q':
				int_query = str;
				break;
			case 'W':
				where = str;
				break;
			case 'F':
				file = str;
				break;
			case 'L':
				line = str;
				break;
			case 'R':
				routine = str;
				break;
		}

		/* code, message and \0 */
		offset += strlen(str) + 2;
	}

	/*
	 * We may have special handling for some errors, default handling is to
	 * throw out error with the same message. We can not ereport immediately
	 * because we should read from this and other connections until
	 * ReadyForQuery is received, so we just store the error message.
	 * If multiple connections return errors only first one is reported.
	 */
	if (!combiner->errorMessage)
	{
		combiner->errorMessage = pstrdup(message);
		/* Error Code is exactly 5 significant bytes */
		if (code)
			memcpy(combiner->errorCode, code, 5);
	}

	if (!combiner->errorDetail && detail != NULL)
	{
		combiner->errorDetail = pstrdup(detail);
	}

	/*
	 * If data node have sent ErrorResponse it will never send CommandComplete.
	 * Increment the counter to prevent endless waiting for it.
	 */
	combiner->command_complete_count++;
}

/*
 * HandleCmdComplete -
 *	combine deparsed sql statements execution results
 *
 * Input parameters: 
 *	commandType is dml command type
 *	combineTag is used to combine the completion result
 *	msg_body is execution result needed to combine
 *	len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine, 
						const char *msg_body, size_t len)
{
	int	digits = 0;
	uint64	originrowcount = 0;
	uint64	rowcount = 0;
	uint64	total = 0;
	
	if (msg_body == NULL)
		return;
	
	/* if there's nothing in combine, just copy the msg_body */
	if (strlen(combine->data) == 0)
	{
		strcpy(combine->data, msg_body);
		combine->cmdType = commandType;
		return;
	}
	else
	{
		/* commandType is conflict */
		if (combine->cmdType != commandType)
			return;
		
		/* get the processed row number from msg_body */
		digits = parse_row_count(msg_body, len + 1, &rowcount);
		elog(DEBUG1, "digits is %d\n", digits);
		Assert(digits >= 0);

		/* no need to combine */
		if (digits == 0)
			return;

		/* combine the processed row number */
		parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
		elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
		total = originrowcount + rowcount;

	}

	/* output command completion tag */
	switch (commandType)
	{
		case CMD_SELECT:
			strcpy(combine->data, "SELECT");
			break;
		case CMD_INSERT:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
			   "INSERT %u %lu", 0, total);
			break;
		case CMD_UPDATE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "UPDATE %lu", total);
			break;
		case CMD_DELETE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "DELETE %lu", total);
			break;
		default:
			strcpy(combine->data, "");
			break;
	}
	
}

/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
static bool
validate_combiner(RemoteQueryState *combiner)
{
	/* There was error message while combining */
	if (combiner->errorMessage)
		return false;
	/* Check if state is defined */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		return false;

	/* Check all nodes completed */
	if ((combiner->request_type == REQUEST_TYPE_COMMAND
	        || combiner->request_type == REQUEST_TYPE_QUERY)
	        && combiner->command_complete_count != combiner->node_count)
		return false;

	/* Check count of description responses */
	if (combiner->request_type == REQUEST_TYPE_QUERY
	        && combiner->description_count != combiner->node_count)
		return false;

	/* Check count of copy-in responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_IN
	        && combiner->copy_in_count != combiner->node_count)
		return false;

	/* Check count of copy-out responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_OUT
	        && combiner->copy_out_count != combiner->node_count)
		return false;

	/* Add other checks here as needed */

	/* All is good if we are here */
	return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
static void
CloseCombiner(RemoteQueryState *combiner)
{
	if (combiner)
	{
		if (combiner->connections)
			pfree(combiner->connections);
		if (combiner->tuple_desc)
			FreeTupleDesc(combiner->tuple_desc);
		if (combiner->errorMessage)
			pfree(combiner->errorMessage);
		if (combiner->errorDetail)
			pfree(combiner->errorDetail);
		if (combiner->cursor_connections)
			pfree(combiner->cursor_connections);
		if (combiner->tapenodes)
			pfree(combiner->tapenodes);
		pfree(combiner);
	}
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
static bool
ValidateAndCloseCombiner(RemoteQueryState *combiner)
{
	bool		valid = validate_combiner(combiner);

	CloseCombiner(combiner);

	return valid;
}

/*
 * Validate combiner and reset storage
 */
static bool
ValidateAndResetCombiner(RemoteQueryState *combiner)
{
	bool		valid = validate_combiner(combiner);
	ListCell   *lc;

	if (combiner->connections)
		pfree(combiner->connections);
	if (combiner->tuple_desc)
		FreeTupleDesc(combiner->tuple_desc);
	if (combiner->currentRow.msg)
		pfree(combiner->currentRow.msg);
	foreach(lc, combiner->rowBuffer)
	{
		RemoteDataRow dataRow = (RemoteDataRow) lfirst(lc);
		pfree(dataRow->msg);
	}
	list_free_deep(combiner->rowBuffer);
	if (combiner->errorMessage)
		pfree(combiner->errorMessage);
	if (combiner->errorDetail)
		pfree(combiner->errorDetail);
	if (combiner->tapenodes)
		pfree(combiner->tapenodes);

	combiner->command_complete_count = 0;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->tuple_desc = NULL;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->errorMessage = NULL;
	combiner->errorDetail = NULL;
	combiner->query_Done = false;
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
	combiner->copy_file = NULL;

	return valid;
}

/*
 * It is possible if multiple steps share the same data node connection, when
 * executor is running multi-step query or client is running multiple queries
 * using Extended Query Protocol. After returning next tuple ExecRemoteQuery
 * function passes execution control to the executor and then it can be given
 * to the same RemoteQuery or to different one. It is possible that before
 * returning a tuple the function do not read all data node responses. In this
 * case pending responses should be read in context of original RemoteQueryState
 * till ReadyForQuery message and data rows should be stored (buffered) to be
 * available when fetch from that RemoteQueryState is requested again.
 * BufferConnection function does the job.
 * If a RemoteQuery is going to use connection it should check connection state.
 * DN_CONNECTION_STATE_QUERY indicates query has data to read and combiner
 * points to the original RemoteQueryState. If combiner differs from "this" the
 * connection should be buffered.
 */
void
BufferConnection(PGXCNodeHandle *conn)
{
	RemoteQueryState *combiner = conn->combiner;
	MemoryContext oldcontext;

	if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
		return;

	/*
	 * When BufferConnection is invoked CurrentContext is related to other
	 * portal, which is trying to control the connection.
	 * TODO See if we can find better context to switch to
	 */
	oldcontext = MemoryContextSwitchTo(combiner->ss.ss_ScanTupleSlot->tts_mcxt);

	/* Verify the connection is in use by the combiner */
	combiner->current_conn = 0;
	while (combiner->current_conn < combiner->conn_count)
	{
		if (combiner->connections[combiner->current_conn] == conn)
			break;
		combiner->current_conn++;
	}
	Assert(combiner->current_conn < combiner->conn_count);

	/*
	 * Buffer data rows until data node return number of rows specified by the
	 * fetch_size parameter of last Execute message (PortalSuspended message)
	 * or end of result set is reached (CommandComplete message)
	 */
	while (conn->state == DN_CONNECTION_STATE_QUERY)
	{
		int res;

		/* Move to buffer currentRow (received from the data node) */
		if (combiner->currentRow.msg)
		{
			RemoteDataRow dataRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData));
			*dataRow = combiner->currentRow;
			combiner->currentRow.msg = NULL;
			combiner->currentRow.msglen = 0;
			combiner->currentRow.msgnode = 0;
			combiner->rowBuffer = lappend(combiner->rowBuffer, dataRow);
		}

		res = handle_response(conn, combiner);
		/*
		 * If response message is a DataRow it will be handled on the next
		 * iteration.
		 * PortalSuspended will cause connection state change and break the loop
		 * The same is for CommandComplete, but we need additional handling -
		 * remove connection from the list of active connections.
		 * We may need to add handling error response
		 */
		if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
			{
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				add_error_message(conn, "Failed to fetch from data node");
			}
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/*
			 * End of result set is reached, so either set the pointer to the
			 * connection to NULL (step with sort) or remove it from the list
			 * (step without sort)
			 */
			if (combiner->tuplesortstate)
			{
				combiner->connections[combiner->current_conn] = NULL;
				if (combiner->tapenodes == NULL)
					combiner->tapenodes = (int*) palloc0(NumDataNodes * sizeof(int));
				combiner->tapenodes[combiner->current_conn] =
						PGXCNodeGetNodeId(conn->nodeoid,
										  PGXC_NODE_DATANODE_MASTER);
			}
			else
				/* Remove current connection, move last in-place, adjust current_conn */
				if (combiner->current_conn < --combiner->conn_count)
					combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
				else
					combiner->current_conn = 0;
		}
		/*
		 * Before output RESPONSE_COMPLETE or PORTAL_SUSPENDED handle_response()
		 * changes connection state to DN_CONNECTION_STATE_IDLE, breaking the
		 * loop. We do not need to do anything specific in case of
		 * PORTAL_SUSPENDED so skiping "else if" block for that case
		 */
	}
	MemoryContextSwitchTo(oldcontext);
	conn->combiner = NULL;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
static void
CopyDataRowTupleToSlot(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	char 		*msg;
	MemoryContext	oldcontext;
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
	msg = (char *)palloc(combiner->currentRow.msglen);
	memcpy(msg, combiner->currentRow.msg, combiner->currentRow.msglen);
	ExecStoreDataRowTuple(msg, combiner->currentRow.msglen, combiner->currentRow.msgnode, slot, true);
	pfree(combiner->currentRow.msg);
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Get next data row from the combiner's buffer into provided slot
 * Just clear slot and return false if buffer is empty, that means end of result
 * set is reached
 */
bool
FetchTuple(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	bool have_tuple = false;

	/* If we have message in the buffer, consume it */
	if (combiner->currentRow.msg)
	{
		CopyDataRowTupleToSlot(combiner, slot);
		have_tuple = true;
	}

	/*
	 * If this is ordered fetch we can not know what is the node
	 * to handle next, so sorter will choose next itself and set it as
	 * currentRow to have it consumed on the next call to FetchTuple.
	 * Otherwise allow to prefetch next tuple
	 */
	if (((RemoteQuery *)combiner->ss.ps.plan)->sort)
		return have_tuple;

	/*
	 * Note: If we are fetching not sorted results we can not have both
	 * currentRow and buffered rows. When connection is buffered currentRow
	 * is moved to buffer, and then it is cleaned after buffering is
	 * completed. Afterwards rows will be taken from the buffer bypassing
	 * currentRow until buffer is empty, and only after that data are read
	 * from a connection.
	 * PGXCTODO: the message should be allocated in the same memory context as
	 * that of the slot. Are we sure of that in the call to
	 * ExecStoreDataRowTuple below? If one fixes this memory issue, please
	 * consider using CopyDataRowTupleToSlot() for the same.
	 */
	if (list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
		combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
		ExecStoreDataRowTuple(dataRow->msg, dataRow->msglen, dataRow->msgnode, slot, true);
		pfree(dataRow);
		return true;
	}

	while (combiner->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

		/* Going to use a connection, buffer it if needed */
		if (conn->state == DN_CONNECTION_STATE_QUERY && conn->combiner != NULL
				&& conn->combiner != combiner)
			BufferConnection(conn);

		/*
		 * If current connection is idle it means portal on the data node is
		 * suspended. If we have a tuple do not hurry to request more rows,
		 * leave connection clean for other RemoteQueries.
		 * If we do not have, request more and try to get it
		 */
		if (conn->state == DN_CONNECTION_STATE_IDLE)
		{
			/*
			 * If we have tuple to return do not hurry to request more, keep
			 * connection clean
			 */
			if (have_tuple)
				return true;
			else
			{
				if (pgxc_node_send_execute(conn, combiner->cursor, 1) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from data node")));
				if (pgxc_node_send_sync(conn) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from data node")));
				if (pgxc_node_receive(1, &conn, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from data node")));
				conn->combiner = combiner;
			}
		}

		/* read messages */
		res = handle_response(conn, combiner);
		if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to fetch from data node")));
			continue;
		}
		else if (res == RESPONSE_SUSPENDED)
		{
			/* Make next connection current */
			if (++combiner->current_conn >= combiner->conn_count)
				combiner->current_conn = 0;
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/* Remove current connection, move last in-place, adjust current_conn */
			if (combiner->current_conn < --combiner->conn_count)
				combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
			else
				combiner->current_conn = 0;
		}
		else if (res == RESPONSE_DATAROW && have_tuple)
		{
			/*
			 * We already have a tuple and received another one, leave it till
			 * next fetch
			 */
			return true;
		}

		/* If we have message in the buffer, consume it */
		if (combiner->currentRow.msg)
		{
			CopyDataRowTupleToSlot(combiner, slot);
			have_tuple = true;
		}

		/*
		 * If this is ordered fetch we can not know what is the node
		 * to handle next, so sorter will choose next itself and set it as
		 * currentRow to have it consumed on the next call to FetchTuple.
		 * Otherwise allow to prefetch next tuple
		 */
		if (((RemoteQuery *)combiner->ss.ps.plan)->sort)
			return have_tuple;
	}

	/* report end of data to the caller */
	if (!have_tuple)
		ExecClearTuple(slot);

	return have_tuple;
}


/*
 * Handle responses from the Data node connections
 */
static int
pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
						 struct timeval * timeout, RemoteQueryState *combiner)
{
	int			count = conn_count;
	PGXCNodeHandle *to_receive[conn_count];

	/* make a copy of the pointers to the connections */
	memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

	/*
	 * Read results.
	 * Note we try and read from data node connections even if there is an error on one,
	 * so as to avoid reading incorrect results on the next statement.
	 * Other safegaurds exist to avoid this, however.
	 */
	while (count > 0)
	{
		int i = 0;

		if (pgxc_node_receive(count, to_receive, timeout))
			return EOF;
		while (i < count)
		{
			int result =  handle_response(to_receive[i], combiner);
			switch (result)
			{
				case RESPONSE_EOF: /* have something to read, keep receiving */
					i++;
					break;
				case RESPONSE_COMPLETE:
				case RESPONSE_COPY:
					/* Handling is done, do not track this connection */
					count--;
					/* Move last connection in place */
					if (i < count)
						to_receive[i] = to_receive[count];
					break;
				default:
					/* Inconsistent responses */
					add_error_message(to_receive[i], "Unexpected response from the data nodes");
					elog(WARNING, "Unexpected response from the data nodes, result = %d, request type %d", result, combiner->request_type);
					/* Stop tracking and move last connection in place */
					count--;
					if (i < count)
						to_receive[i] = to_receive[count];
			}
		}
	}

	return 0;
}

/*
 * Read next message from the connection and update the combiner accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_COMPLETE - done with the connection
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
handle_response(PGXCNodeHandle * conn, RemoteQueryState *combiner)
{
	char		*msg;
	int		msg_len;
	char		msg_type;
	bool		suspended = false;

	for (;;)
	{
		Assert(conn->state != DN_CONNECTION_STATE_IDLE);

		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return RESPONSE_COMPLETE;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		Assert(conn->combiner == combiner || conn->combiner == NULL);

		/* TODO handle other possible responses */
		msg_type = get_message(conn, &msg_len, &msg);
		switch (msg_type)
		{
			case '\0':			/* Not enough data in the buffer */
				return RESPONSE_EOF;
			case 'c':			/* CopyToCommandComplete */
				HandleCopyOutComplete(combiner);
				break;
			case 'C':			/* CommandComplete */
				HandleCommandComplete(combiner, msg, msg_len);
				break;
			case 'T':			/* RowDescription */
#ifdef DN_CONNECTION_DEBUG
				Assert(!conn->have_row_desc);
				conn->have_row_desc = true;
#endif
				if (HandleRowDescription(combiner, msg, msg_len))
					return RESPONSE_TUPDESC;
				break;
			case 'D':			/* DataRow */
#ifdef DN_CONNECTION_DEBUG
				Assert(conn->have_row_desc);
#endif
				HandleDataRow(combiner, msg, msg_len, PGXCNodeGetNodeId(conn->nodeoid,
																		PGXC_NODE_DATANODE_MASTER));
				return RESPONSE_DATAROW;
			case 's':			/* PortalSuspended */
				suspended = true;
				break;
			case '1': /* ParseComplete */
			case '2': /* BindComplete */
			case '3': /* CloseComplete */
			case 'n': /* NoData */
				/* simple notifications, continue reading */
				break;
			case 'G': /* CopyInResponse */
				conn->state = DN_CONNECTION_STATE_COPY_IN;
				HandleCopyIn(combiner);
				/* Done, return to caller to let it know the data can be passed in */
				return RESPONSE_COPY;
			case 'H': /* CopyOutResponse */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyOut(combiner);
				return RESPONSE_COPY;
			case 'd': /* CopyOutDataRow */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyDataRow(combiner, msg, msg_len);
				break;
			case 'E':			/* ErrorResponse */
				HandleError(combiner, msg, msg_len);
				/*
				 * Do not return with an error, we still need to consume Z,
				 * ready-for-query
				 */
				break;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
			case 'S':			/* SetCommandComplete */
				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
			{
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				int result = suspended ? RESPONSE_SUSPENDED : RESPONSE_COMPLETE;
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif
				return result;
			}

#ifdef PGXC
			case 'b':
				{
					conn->state = DN_CONNECTION_STATE_IDLE;
					return RESPONSE_BARRIER_OK;
				}
#endif

			case 'I':			/* EmptyQuery */
			default:
				/* sync lost? */
				elog(WARNING, "Received unsupported message type: %c", msg_type);
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				/* stop reading */
				return RESPONSE_COMPLETE;
		}
	}
	/* never happen, but keep compiler quiet */
	return RESPONSE_EOF;
}


/*
 * Has the data node sent Ready For Query
 */

bool
is_data_node_ready(PGXCNodeHandle * conn)
{
	char		*msg;
	int		msg_len;
	char		msg_type;
	bool		suspended = false;

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return true;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return false;

		msg_type = get_message(conn, &msg_len, &msg);
		switch (msg_type)
		{
			case 's':			/* PortalSuspended */
				suspended = true;
				break;

			case 'Z':			/* ReadyForQuery */
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->combiner = NULL;
				return true;
		}
	}
	/* never happen, but keep compiler quiet */
	return false;
}

/*
 * Deparse the node string list obtained from GTM
 * and fill in Datanode and Coordinator lists.
 */
static bool
analyze_node_string(char *nodestring,
					List **datanodelist,
					List **coordlist)
{
	char *rawstring;
	List *elemlist;
	ListCell *item;
	bool	is_local_coord = false;

	*datanodelist = NIL;
	*coordlist = NIL;

	if (!nodestring)
		return is_local_coord;

	rawstring = pstrdup(nodestring);

	if (!SplitIdentifierString(rawstring, ',', &elemlist))
		/* syntax error in list */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax for \"data_node_hosts\"")));

	/* Fill in Coordinator and Datanode list */
	foreach(item, elemlist)
	{
		char *nodename = (char *) lfirst(item);
		Oid nodeoid = get_pgxc_nodeoid((const char *) nodename);

		if (!OidIsValid(nodeoid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined",
							nodename)));

		if (get_pgxc_nodetype(nodeoid) == PGXC_NODE_DATANODE_MASTER)
		{
			int nodeid = PGXCNodeGetNodeId(nodeoid, PGXC_NODE_DATANODE_MASTER);
			*datanodelist = lappend_int(*datanodelist, nodeid);
		}
		else if (get_pgxc_nodetype(nodeoid) == PGXC_NODE_COORD_MASTER)
		{
			int nodeid = PGXCNodeGetNodeId(nodeoid, PGXC_NODE_COORD_MASTER);
			/* Local Coordinator has been found, so commit it */
			if (nodeid == PGXCNodeId - 1)
				is_local_coord = true;
			else
				*coordlist = lappend_int(*coordlist, nodeid);
		}
	}
	pfree(rawstring);

	return is_local_coord;
}


/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses
 */
static int
pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
				GlobalTransactionId gxid)
{
	int			i;
	struct timeval *timeout = NULL;
	RemoteQueryState *combiner;
	TimestampTz timestamp = GetCurrentGTMStartTimestamp();

	/* Send BEGIN */
	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);

		if (GlobalTransactionIdIsValid(gxid) && pgxc_node_send_gxid(connections[i], gxid))
			return EOF;

		if (GlobalTimestampIsValid(timestamp) && pgxc_node_send_timestamp(connections[i], timestamp))
			return EOF;

		if (begin_string)
		{
			if (pgxc_node_send_query(connections[i], begin_string))
				return EOF;
		}
		else
		{
			if (pgxc_node_send_query(connections[i], "BEGIN"))
				return EOF;
		}
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	if (pgxc_node_receive_responses(conn_count, connections, timeout, combiner))
		return EOF;

	/* Verify status */
	return ValidateAndCloseCombiner(combiner) ? 0 : EOF;
}

/* Clears the write node list */
static void
clear_write_node_list()
{
	/* we just malloc once and use counter */
	if (write_node_list == NULL)
	{
		write_node_list = (PGXCNodeHandle **) malloc(NumDataNodes * sizeof(PGXCNodeHandle *));
	}
	write_node_count = 0;
}


/*
 * Switch autocommit mode off, so all subsequent statements will be in the same transaction
 */
void
PGXCNodeBegin(void)
{
	autocommit = false;
	clear_write_node_list();
}

void
PGXCNodeSetBeginQuery(char *query_string)
{
	int len;

	if (!query_string)
		return;

	len = strlen(query_string);
	/*
	 * This query string is sent to backend nodes,
	 * it contains serializable and read options
	 */
	begin_string = (char *)malloc(len + 1);
	begin_string = memcpy(begin_string, query_string, len + 1);
}

/*
 * Error messages for PREPARE
 */
#define ERROR_DATANODES_PREPARE	-1
#define ERROR_ALREADY_PREPARE	-2

/*
 * Prepare transaction on Datanodes and Coordinators involved in current transaction.
 * GXID associated to current transaction has to be committed on GTM.
 */
bool
PGXCNodePrepare(char *gid)
{
	int         res = 0;
	int         tran_count;
	PGXCNodeAllHandles *pgxc_connections;
	bool local_operation = false;

	pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	/* DDL involved in transaction, so make a local prepare too */
	if (is_ddl)
		local_operation = true;

	/*
	 * If no connections have been gathered for Coordinators,
	 * it means that no DDL has been involved in this transaction.
	 * And so this transaction is not prepared on Coordinators.
	 * It is only on Datanodes that data is involved.
	 */
	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * If we do not have open transactions we have nothing to prepare just
	 * report success
	 */
	if (tran_count == 0 && !is_ddl)
	{
		elog(DEBUG1, "Nothing to PREPARE on Datanodes and Coordinators, gid is not used");
		goto finish;
	}

	res = pgxc_node_prepare(pgxc_connections, gid);

finish:
	/*
	 * The transaction is just prepared, but Datanodes have reset,
	 * so we'll need a new gxid for commit prepared or rollback prepared
	 * Application is responsible for delivering the correct gid.
	 * Release the connections for the moment.
	 */
	if (!autocommit)
		stat_transaction(pgxc_connections->dn_conn_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);

	if (res != 0)
	{

		/* In case transaction has operated on temporary objects */
		if (temp_object_included)
		{
			/* Reset temporary object flag */
			temp_object_included = false;
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("cannot PREPARE a transaction that has operated on temporary tables")));
		}

		if (res == ERROR_ALREADY_PREPARE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("transaction identifier \"%s\" is already in use", gid)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not prepare transaction on data nodes")));
	}

	/* Reset temporary object flag */
	temp_object_included = false;

	return local_operation;
}


/*
 * Prepare transaction on dedicated nodes with gid received from application
 */
static int
pgxc_node_prepare(PGXCNodeAllHandles *pgxc_handles, char *gid)
{
	int			result = 0;
	int			co_conn_count = pgxc_handles->co_conn_count;
	int			dn_conn_count = pgxc_handles->dn_conn_count;
	char			*buffer = (char *) palloc0(22 + strlen(gid) + 1);
	GlobalTransactionId	gxid = InvalidGlobalTransactionId;
	char			*nodestring = NULL;
	bool			gtm_error = false;

	gxid = GetCurrentGlobalTransactionId();

	/*
	 * Now that the transaction has been prepared on the nodes,
	 * Initialize to make the business on GTM.
	 * We also had the Coordinator we are on in the prepared state.
	 */
	if (dn_conn_count != 0)
		nodestring = collect_pgxcnode_names(nodestring,
											dn_conn_count,
											pgxc_handles->datanode_handles,
											REMOTE_CONN_DATANODE);
	/*
	 * Local Coordinator is saved in the list sent to GTM
	 * only when a DDL is involved in the transaction.
	 * So we don't need to complete the list of Coordinators sent to GTM
	 * when number of connections to Coordinator is zero (no DDL).
	 */
	if (co_conn_count != 0)
		nodestring = collect_pgxcnode_names(nodestring,
											co_conn_count,
											pgxc_handles->coord_handles,
											REMOTE_CONN_COORD);
	/*
	 * This is the case of a single Coordinator
	 * involved in a transaction using DDL.
	 */
	if (is_ddl && co_conn_count == 0 && PGXCNodeId >= 0)
		nodestring = collect_localnode_name(nodestring);

	result = StartPreparedTranGTM(gxid, gid, nodestring);
	if (result < 0)
	{
		gtm_error = true;
		goto finish;
	}

	sprintf(buffer, "PREPARE TRANSACTION '%s'", gid);

	/* Continue even after an error here, to consume the messages */
	result = pgxc_all_handles_send_query(pgxc_handles, buffer, true);

	/* Receive and Combine results from Datanodes and Coordinators */
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);

	if (result)
		goto finish;

	/*
	 * Prepare the transaction on GTM after everything is done.
	 * GXID associated with PREPARE state is considered as used on Nodes,
	 * but is still present in Snapshot.
	 * This GXID will be discarded from Snapshot when commit prepared is
	 * issued from another node.
	 */
	result = PrepareTranGTM(gxid);

finish:
	/*
	 * An error has happened on a Datanode,
	 * It is necessary to rollback the transaction on already prepared nodes.
	 * But not on nodes where the error occurred.
	 */
	if (result)
	{
		GlobalTransactionId rollback_xid = InvalidGlobalTransactionId;
		result = 0;

		if (gtm_error)
		{
			buffer = (char *) repalloc(buffer, 9);
			sprintf(buffer, "ROLLBACK");
		}
		else
		{
			buffer = (char *) repalloc(buffer, 20 + strlen(gid) + 1);
			sprintf(buffer, "ROLLBACK PREPARED '%s'", gid);

			rollback_xid = BeginTranGTM(NULL);
		}

		/*
		 * Send xid and rollback prepared down to Datanodes and Coordinators
		 * Even if we get an error on one, we try and send to the others
		 * Only query is sent down to nodes if error occured on GTM.
		 */
		if (!gtm_error)
			if (pgxc_all_handles_send_gxid(pgxc_handles, rollback_xid, false))
				result = ERROR_DATANODES_PREPARE;

		if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
			result = ERROR_DATANODES_PREPARE;

		result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
		result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);

		/*
		 * Don't forget to rollback also on GTM if error happened on Datanodes
		 * Both GXIDs used for PREPARE and COMMIT PREPARED are discarded from GTM snapshot here.
		 */
		if (!gtm_error)
			CommitPreparedTranGTM(gxid, rollback_xid);

		if (gtm_error)
			return ERROR_ALREADY_PREPARE;
		else
			return ERROR_DATANODES_PREPARE;
	}

	return result;
}

/*
 * Prepare all the nodes involved in this implicit Prepare
 * Abort transaction if this is not done correctly
 */
int
PGXCNodeImplicitPrepare(GlobalTransactionId prepare_xid, char *gid)
{
	int         res = 0;
	int         tran_count;
	PGXCNodeAllHandles *pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	if (!pgxc_connections)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not prepare connection implicitely")));

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * This should not happen because an implicit 2PC is always using other nodes,
	 * but it is better to check.
	 */
	if (tran_count == 0)
	{
		goto finish;
	}

	res = pgxc_node_implicit_prepare(prepare_xid, pgxc_connections, gid);

finish:
	if (!autocommit)
		stat_transaction(pgxc_connections->dn_conn_count);

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);

	return res;
}

/*
 * Prepare transaction on dedicated nodes for Implicit 2PC
 * This is done inside a Transaction commit if multiple nodes are involved in write operations
 * Implicit prepare in done internally on Coordinator, so this does not interact with GTM.
 */
static int
pgxc_node_implicit_prepare(GlobalTransactionId prepare_xid,
						   PGXCNodeAllHandles *pgxc_handles,
						   char *gid)
{
	int		result = 0;
	int		co_conn_count = pgxc_handles->co_conn_count;
	int		dn_conn_count = pgxc_handles->dn_conn_count;
	char	buffer[256];

	sprintf(buffer, "PREPARE TRANSACTION '%s'", gid);

	/* Continue even after an error here, to consume the messages */
	result = pgxc_all_handles_send_query(pgxc_handles, buffer, true);

	/* Receive and Combine results from Datanodes and Coordinators */
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);

	return result;
}

/*
 * Commit all the nodes involved in this Implicit Commit.
 * Prepared XID is committed at the same time as Commit XID on GTM.
 */
void
PGXCNodeImplicitCommitPrepared(GlobalTransactionId prepare_xid,
							   GlobalTransactionId commit_xid,
							   char *gid,
							   bool is_commit)
{
	int         res = 0;
	int         tran_count;
	PGXCNodeAllHandles *pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_IDLE);

	if (!pgxc_connections)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit prepared transaction implicitly")));

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * This should not happen because an implicit 2PC is always using other nodes,
	 * but it is better to check.
	 */
	if (tran_count == 0)
	{
		elog(WARNING, "Nothing to PREPARE on Datanodes and Coordinators");
		goto finish;
	}

	/*
	 * Barrier:
	 *
	 * We should acquire the BarrierLock in SHARE mode here to ensure that
	 * there are no in-progress barrier at this point. This mechanism would
	 * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
	 * requester
	 */
	LWLockAcquire(BarrierLock, LW_SHARED);

	res = pgxc_node_implicit_commit_prepared(prepare_xid, commit_xid,
								pgxc_connections, gid, is_commit);

	/*
	 * Release the BarrierLock.
	 */
	LWLockRelease(BarrierLock);

finish:
	/* Clear nodes, signals are clear */
	if (!autocommit)
		stat_transaction(pgxc_connections->dn_conn_count);

	/*
	 * If an error happened, do not release handles yet. This is done when transaction
	 * is aborted after the list of nodes in error state has been saved to be sent to GTM
	 */
	if (!PersistentConnections && res == 0)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

	/* Reset temporary object flag */
	temp_object_included = false;

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);

	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit prepared transaction implicitly")));

	/*
	 * Commit on GTM is made once we are sure that Nodes are not only partially committed
	 * If an error happens on a Datanode during implicit COMMIT PREPARED, a special handling
	 * is made in AbortTransaction().
	 * The list of datanodes is saved on GTM and the partially committed transaction can be committed
	 * with a COMMIT PREPARED delivered directly from application.
	 * This permits to keep the gxid alive in snapshot and avoids other transactions to see only
	 * partially committed results.
	 */
	CommitPreparedTranGTM(prepare_xid, commit_xid);
}

/*
 * Commit a transaction implicitely transaction on all nodes
 * Prepared transaction with this gid has reset the datanodes,
 * so we need a new gxid.
 *
 * GXID used for Prepare and Commit are committed at the same time on GTM.
 * This saves Network ressource a bit.
 */
static int
pgxc_node_implicit_commit_prepared(GlobalTransactionId prepare_xid,
								   GlobalTransactionId commit_xid,
								   PGXCNodeAllHandles *pgxc_handles,
								   char *gid,
								   bool is_commit)
{
	char	buffer[256];
	int		result = 0;
	int		co_conn_count = pgxc_handles->co_conn_count;
	int		dn_conn_count = pgxc_handles->dn_conn_count;

	if (is_commit)
		sprintf(buffer, "COMMIT PREPARED '%s'", gid);
	else
		sprintf(buffer, "ROLLBACK PREPARED '%s'", gid);

	if (pgxc_all_handles_send_gxid(pgxc_handles, commit_xid, true))
	{
		result = EOF;
		goto finish;
	}

	/* Send COMMIT to all handles */
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

	/* Receive and Combine results from Datanodes and Coordinators */
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
finish:
	return result;
}

/*
 * Commit prepared transaction on Datanodes and Coordinators (as necessary)
 * where it has been prepared.
 * Connection to backends has been cut when transaction has been prepared,
 * So it is necessary to send the COMMIT PREPARE message to all the nodes.
 * We are not sure if the transaction prepared has involved all the datanodes
 * or not but send the message to all of them.
 * This avoid to have any additional interaction with GTM when making a 2PC transaction.
 */
void
PGXCNodeCommitPrepared(char *gid)
{
	int			res = 0;
	int			res_gtm = 0;
	PGXCNodeAllHandles	*pgxc_handles = NULL;
	List			*datanodelist = NIL;
	List			*coordlist = NIL;
	int			tran_count;
	char			**datanodes = NULL;
	char			**coordinators = NULL;
	int			coordcnt = 0;
	int			datanodecnt = 0;
	GlobalTransactionId	gxid, prepared_gxid;
	/* This flag tracks if the transaction has to be committed locally */
	bool			operation_local = false;
	char		   *nodestring = NULL;

	res_gtm = GetGIDDataGTM(gid, &gxid, &prepared_gxid, &nodestring);

	/* Analyze string obtained and get all node informations */
	operation_local = analyze_node_string(nodestring, &datanodelist, &coordlist);
	coordcnt = list_length(coordlist);
	datanodecnt = list_length(datanodelist);

	tran_count = datanodecnt + coordcnt;
	if (tran_count == 0 || res_gtm < 0)
		goto finish;

	autocommit = false;

	/* Get connections */
	if (coordcnt > 0 && datanodecnt == 0)
		pgxc_handles = get_handles(datanodelist, coordlist, true);
	else
		pgxc_handles = get_handles(datanodelist, coordlist, false);

	/*
	 * Commit here the prepared transaction to all Datanodes and Coordinators
	 * If necessary, local Coordinator Commit is performed after this DataNodeCommitPrepared.
	 *
	 * BARRIER:
	 *
	 * Take the BarrierLock in SHARE mode to synchronize on in-progress
	 * barriers. We should hold on to the lock until the local prepared
	 * transaction is also committed
	 */
	LWLockAcquire(BarrierLock, LW_SHARED);

	res = pgxc_node_commit_prepared(gxid, prepared_gxid, pgxc_handles, gid);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

	/* Reset temporary object flag */
	temp_object_included = false;

	/* Free node list taken from GTM */
	if (datanodes && datanodecnt != 0)
		free(datanodes);

	if (coordinators && coordcnt != 0)
		free(coordinators);

	pfree_pgxc_all_handles(pgxc_handles);

	if (res_gtm < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						gid)));
	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit prepared transaction on data nodes")));

	/*
	 * A local Coordinator always commits if involved in Prepare.
	 * 2PC file is created and flushed if a DDL has been involved in the transaction.
	 * If remote connection is a Coordinator type, the commit prepared has to be done locally
	 * if and only if the Coordinator number was in the node list received from GTM.
	 */
	if (operation_local)
		FinishPreparedTransaction(gid, true);

	LWLockRelease(BarrierLock);
	return;
}

/*
 * Commit a prepared transaction on all nodes
 * Prepared transaction with this gid has reset the datanodes,
 * so we need a new gxid.
 * An error is returned to the application only if all the Datanodes
 * and Coordinator do not know about the gxid proposed.
 * This permits to avoid interactions with GTM.
 */
static int
pgxc_node_commit_prepared(GlobalTransactionId gxid,
						  GlobalTransactionId prepared_gxid,
						  PGXCNodeAllHandles *pgxc_handles,
						  char *gid)
{
	int result = 0;
	int co_conn_count = pgxc_handles->co_conn_count;
	int dn_conn_count = pgxc_handles->dn_conn_count;
	char        *buffer = (char *) palloc0(18 + strlen(gid) + 1);

	/* GXID has been piggybacked when gid data has been received from GTM */
	sprintf(buffer, "COMMIT PREPARED '%s'", gid);

	/* Send gxid and COMMIT PREPARED message to all the Datanodes */
	if (pgxc_all_handles_send_gxid(pgxc_handles, gxid, true))
		goto finish;

	/* Continue and receive responses even if there is an error */
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

	result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);

finish:
	/* Both GXIDs used for PREPARE and COMMIT PREPARED are discarded from GTM snapshot here */
	CommitPreparedTranGTM(gxid, prepared_gxid);

	return result;
}

/*
 * Rollback prepared transaction on Datanodes involved in the current transaction
 *
 * Return whether or not a local operation required.
 */
bool
PGXCNodeRollbackPrepared(char *gid)
{
	int			res = 0;
	int			res_gtm = 0;
	PGXCNodeAllHandles	*pgxc_handles = NULL;
	List			*datanodelist = NIL;
	List			*coordlist = NIL;
	int			tran_count;
	int			coordcnt = 0;
	int			datanodecnt = 0;
	GlobalTransactionId	gxid, prepared_gxid;
	char		*nodestring = NULL;
	/* This flag tracks if the transaction has to be rolled back locally */
	bool			operation_local = false;

	res_gtm = GetGIDDataGTM(gid, &gxid, &prepared_gxid, &nodestring);

	/* Analyze string obtained and get all node informations */
	operation_local = analyze_node_string(nodestring, &datanodelist, &coordlist);
	coordcnt = list_length(coordlist);
	datanodecnt = list_length(datanodelist);

	tran_count = datanodecnt + coordcnt;
	if (tran_count == 0 || res_gtm < 0 )
		goto finish;

	autocommit = false;

	/* Get connections */
	if (coordcnt > 0 && datanodecnt == 0)
		pgxc_handles = get_handles(datanodelist, coordlist, true);
	else
		pgxc_handles = get_handles(datanodelist, coordlist, false);

	/* Here do the real rollback to Datanodes and Coordinators */
	res = pgxc_node_rollback_prepared(gxid, prepared_gxid, pgxc_handles, gid);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

	/* Reset temporary object flag */
	temp_object_included = false;

	/* Free node list taken from GTM */
	if (nodestring)
		free(nodestring);

	pfree_pgxc_all_handles(pgxc_handles);
	if (res_gtm < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						gid)));
	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not rollback prepared transaction on Datanodes")));

	return operation_local;
}


/*
 * Rollback prepared transaction
 * We first get the prepared informations from GTM and then do the treatment
 * At the end both prepared GXID and GXID are committed.
 */
static int
pgxc_node_rollback_prepared(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
							PGXCNodeAllHandles *pgxc_handles, char *gid)
{
	int result = 0;
	int dn_conn_count = pgxc_handles->dn_conn_count;
	int co_conn_count = pgxc_handles->co_conn_count;
	char		*buffer = (char *) palloc0(20 + strlen(gid) + 1);

	/* Datanodes have reset after prepared state, so get a new gxid */
	gxid = BeginTranGTM(NULL);

	sprintf(buffer, "ROLLBACK PREPARED '%s'", gid);

	/* Send gxid and ROLLBACK PREPARED message to all the Datanodes */
	if (pgxc_all_handles_send_gxid(pgxc_handles, gxid, false))
		result = EOF;
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

	result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);

	/* Both GXIDs used for PREPARE and COMMIT PREPARED are discarded from GTM snapshot here */
	CommitPreparedTranGTM(gxid, prepared_gxid);

	return result;
}


/*
 * Commit current transaction on data nodes where it has been started
 * This function is called when no 2PC is involved implicitely.
 * So only send a commit to the involved nodes.
 */
void
PGXCNodeCommit(bool bReleaseHandles)
{
	int			res = 0;
	int			tran_count;
	PGXCNodeAllHandles *pgxc_connections;

	pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * If we do not have open transactions we have nothing to commit, just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	res = pgxc_node_commit(pgxc_connections);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections && bReleaseHandles)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

	/* Reset temporary object flag */
	temp_object_included = false;

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);
	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit (or autocommit) data node connection")));
}


/*
 * Commit transaction on specified data node connections, use two-phase commit
 * if more then on one node data have been modified during the transactioon.
 */
static int
pgxc_node_commit(PGXCNodeAllHandles *pgxc_handles)
{
	char		buffer[256];
	int			result = 0;
	int			co_conn_count = pgxc_handles->co_conn_count;
	int			dn_conn_count = pgxc_handles->dn_conn_count;

	strcpy(buffer, "COMMIT");

	/* Send COMMIT to all handles */
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

	/* Receive and Combine results from Datanodes and Coordinators */
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);

	return result;
}


/*
 * Rollback current transaction
 * This will happen
 */
int
PGXCNodeRollback(void)
{
	int			res = 0;
	int			tran_count;
	PGXCNodeAllHandles *pgxc_connections;

	pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * If we do not have open transactions we have nothing to rollback just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	res = pgxc_node_rollback(pgxc_connections);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

	/* Reset temporary object flag */
	temp_object_included = false;

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);
	return res;
}


/*
 * Send ROLLBACK command down to Datanodes and Coordinators and handle responses
 */
static int
pgxc_node_rollback(PGXCNodeAllHandles *pgxc_handles)
{
	int			result = 0;
	int			co_conn_count = pgxc_handles->co_conn_count;
	int			dn_conn_count = pgxc_handles->dn_conn_count;

	/* Send ROLLBACK to all handles */
	if (pgxc_all_handles_send_query(pgxc_handles, "ROLLBACK", false))
		result = EOF;

	/* Receive and Combine results from Datanodes and Coordinators */
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);

	return result;
}


/*
 * Begin COPY command
 * The copy_connections array must have room for NumDataNodes items
 */
PGXCNodeHandle**
DataNodeCopyBegin(const char *query, List *nodelist, Snapshot snapshot, bool is_from)
{
	int i, j;
	int conn_count = list_length(nodelist) == 0 ? NumDataNodes : list_length(nodelist);
	struct timeval *timeout = NULL;
	PGXCNodeAllHandles *pgxc_handles;
	PGXCNodeHandle **connections;
	PGXCNodeHandle **copy_connections;
	PGXCNodeHandle *newConnections[conn_count];
	int new_count = 0;
	ListCell *nodeitem;
	bool need_tran;
	GlobalTransactionId gxid;
	RemoteQueryState *combiner;
	TimestampTz timestamp = GetCurrentGTMStartTimestamp();

	if (conn_count == 0)
		return NULL;
	/* Get needed datanode connections */
	pgxc_handles = get_handles(nodelist, NULL, false);
	connections = pgxc_handles->datanode_handles;

	if (!connections)
		return NULL;

	need_tran = !autocommit || conn_count > 1;

	elog(DEBUG1, "autocommit = %s, conn_count = %d, need_tran = %s", autocommit ? "true" : "false", conn_count, need_tran ? "true" : "false");

	/*
	 * We need to be able quickly find a connection handle for specified node number,
	 * So store connections in an array where index is node-1.
	 * Unused items in the array should be NULL
	 */
	copy_connections = (PGXCNodeHandle **) palloc0(NumDataNodes * sizeof(PGXCNodeHandle *));
	i = 0;
	foreach(nodeitem, nodelist)
		copy_connections[lfirst_int(nodeitem)] = connections[i++];

	/* Gather statistics */
	stat_statement();
	if (autocommit)
		stat_transaction(conn_count);

	/* We normally clear for transactions, but if autocommit, clear here, too */
	if (autocommit)
	{
		clear_write_node_list();
	}

	/* Check status of connections */
	/* We want to track new "write" nodes, and new nodes in the current transaction
	 * whether or not they are write nodes. */
	if (write_node_count < NumDataNodes)
	{
		for (i = 0; i < conn_count; i++)
		{
			bool found = false;
			for (j=0; j<write_node_count && !found; j++)
			{
				if (write_node_list[j] == connections[i])
					found = true;
			}
			if (!found)
			{
				/*
				 * Add to transaction wide-list if COPY FROM
				 * CopyOut (COPY TO) is not a write operation, no need to update
				 */
				if (is_from)
					write_node_list[write_node_count++] = connections[i];
				/* Add to current statement list */
				newConnections[new_count++] = connections[i];
			}
		}
		// Check connection state is DN_CONNECTION_STATE_IDLE
	}

	gxid = GetCurrentGlobalTransactionId();

	/* elog(DEBUG1, "Current gxid = %d", gxid); */

	if (!GlobalTransactionIdIsValid(gxid))
	{
		pfree_pgxc_all_handles(pgxc_handles);
		pfree(copy_connections);
		return NULL;
	}
	if (new_count > 0 && need_tran)
	{
		/* Start transaction on connections where it is not started */
		if (pgxc_node_begin(new_count, newConnections, gxid))
		{
			pfree_pgxc_all_handles(pgxc_handles);
			pfree(copy_connections);
			return NULL;
		}
	}

	/* Send query to nodes */
	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		/* If explicit transaction is needed gxid is already sent */
		if (!need_tran && pgxc_node_send_gxid(connections[i], gxid))
		{
			add_error_message(connections[i], "Can not send request");
			pfree_pgxc_all_handles(pgxc_handles);
			pfree(copy_connections);
			return NULL;
		}
		if (conn_count == 1 && pgxc_node_send_timestamp(connections[i], timestamp))
		{
			/*
			 * If a transaction involves multiple connections timestamp, is
			 * always sent down to Datanodes with pgxc_node_begin.
			 * An autocommit transaction needs the global timestamp also,
			 * so handle this case here.
			 */
			add_error_message(connections[i], "Can not send request");
			pfree_pgxc_all_handles(pgxc_handles);
			pfree(copy_connections);
			return NULL;
		}
		if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot))
		{
			add_error_message(connections[i], "Can not send request");
			pfree_pgxc_all_handles(pgxc_handles);
			pfree(copy_connections);
			return NULL;
		}
		if (pgxc_node_send_query(connections[i], query) != 0)
		{
			add_error_message(connections[i], "Can not send request");
			pfree_pgxc_all_handles(pgxc_handles);
			pfree(copy_connections);
			return NULL;
		}
	}

	/*
	 * We are expecting CopyIn response, but do not want to send it to client,
	 * caller should take care about this, because here we do not know if
	 * client runs console or file copy
	 */
	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	if (pgxc_node_receive_responses(conn_count, connections, timeout, combiner)
			|| !ValidateAndCloseCombiner(combiner))
	{
		if (autocommit)
		{
			if (need_tran)
				DataNodeCopyFinish(connections, -1, COMBINE_TYPE_NONE);
			else if (!PersistentConnections)
				release_handles();
		}

		pfree(connections);
		pfree(copy_connections);
		return NULL;
	}
	pfree(connections);
	return copy_connections;
}

/*
 * Send a data row to the specified nodes
 */
int
DataNodeCopyIn(char *data_row, int len, ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections)
{
	PGXCNodeHandle *primary_handle = NULL;
	ListCell *nodeitem;
	/* size + data row + \n */
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	if (exec_nodes->primarynodelist)
	{
		primary_handle = copy_connections[lfirst_int(list_head(exec_nodes->primarynodelist))];
	}

	if (primary_handle)
	{
		if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* precalculate to speed up access */
			int bytes_needed = primary_handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if (bytes_needed > COPY_BUFFER_SIZE)
			{
				/* First look if data node has sent a error message */
				int read_status = pgxc_node_read_data(primary_handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(primary_handle, "failed to read data from data node");
					return EOF;
				}

				if (primary_handle->inStart < primary_handle->inEnd)
				{
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					handle_response(primary_handle, combiner);
					if (!ValidateAndCloseCombiner(combiner))
						return EOF;
				}

				if (DN_CONNECTION_STATE_ERROR(primary_handle))
					return EOF;

				if (send_some(primary_handle, primary_handle->outEnd) < 0)
				{
					add_error_message(primary_handle, "failed to send data to data node");
					return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, primary_handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			primary_handle->outBuffer[primary_handle->outEnd++] = 'd';
			memcpy(primary_handle->outBuffer + primary_handle->outEnd, &nLen, 4);
			primary_handle->outEnd += 4;
			memcpy(primary_handle->outBuffer + primary_handle->outEnd, data_row, len);
			primary_handle->outEnd += len;
			primary_handle->outBuffer[primary_handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(primary_handle, "Invalid data node connection");
			return EOF;
		}
	}

	foreach(nodeitem, exec_nodes->nodeList)
	{
		PGXCNodeHandle *handle = copy_connections[lfirst_int(nodeitem)];
		if (handle && handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* precalculate to speed up access */
			int bytes_needed = handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if ((primary_handle && bytes_needed > PRIMARY_NODE_WRITEAHEAD)
					|| (!primary_handle && bytes_needed > COPY_BUFFER_SIZE))
			{
				int to_send = handle->outEnd;

				/* First look if data node has sent a error message */
				int read_status = pgxc_node_read_data(handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(handle, "failed to read data from data node");
					return EOF;
				}

				if (handle->inStart < handle->inEnd)
				{
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					handle_response(handle, combiner);
					if (!ValidateAndCloseCombiner(combiner))
						return EOF;
				}

				if (DN_CONNECTION_STATE_ERROR(handle))
					return EOF;

				/*
				 * Allow primary node to write out data before others.
				 * If primary node was blocked it would not accept copy data.
				 * So buffer at least PRIMARY_NODE_WRITEAHEAD at the other nodes.
				 * If primary node is blocked and is buffering, other buffers will
				 * grow accordingly.
				 */
				if (primary_handle)
				{
					if (primary_handle->outEnd + PRIMARY_NODE_WRITEAHEAD < handle->outEnd)
						to_send = handle->outEnd - primary_handle->outEnd - PRIMARY_NODE_WRITEAHEAD;
					else
						to_send = 0;
				}

				/*
				 * Try to send down buffered data if we have
				 */
				if (to_send && send_some(handle, to_send) < 0)
				{
					add_error_message(handle, "failed to send data to data node");
					return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, data_row, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(handle, "Invalid data node connection");
			return EOF;
		}
	}
	return 0;
}

uint64
DataNodeCopyOut(ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections, FILE* copy_file)
{
	RemoteQueryState *combiner;
	int 		conn_count = list_length(exec_nodes->nodeList) == 0 ? NumDataNodes : list_length(exec_nodes->nodeList);
	int 		count = 0;
	bool 		need_tran;
	List		*nodelist;
	ListCell	*nodeitem;
	uint64		processed;

	nodelist = exec_nodes->nodeList;
	need_tran = !autocommit || conn_count > 1;

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_SUM);
	combiner->processed = 0;
	/* If there is an existing file where to copy data, pass it to combiner */
	if (copy_file)
		combiner->copy_file = copy_file;

	foreach(nodeitem, exec_nodes->nodeList)
	{
		PGXCNodeHandle *handle = copy_connections[count];
		count++;

		if (handle && handle->state == DN_CONNECTION_STATE_COPY_OUT)
		{
			int read_status = 0;
			/* H message has been consumed, continue to manage data row messages */
			while (read_status >= 0 && handle->state == DN_CONNECTION_STATE_COPY_OUT) /* continue to read as long as there is data */
			{
				if (handle_response(handle,combiner) == RESPONSE_EOF)
				{
					/* read some extra-data */
					read_status = pgxc_node_read_data(handle, true);
					if (read_status < 0)
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on datanode connection")));
					else
						/*
						 * Set proper connection status - handle_response
						 * has changed it to DN_CONNECTION_STATE_QUERY
						 */
						handle->state = DN_CONNECTION_STATE_COPY_OUT;
				}
				/* There is no more data that can be read from connection */
			}
		}
	}

	processed = combiner->processed;

	if (!ValidateAndCloseCombiner(combiner))
	{
		if (autocommit && !PersistentConnections)
			release_handles();
		pfree(copy_connections);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner->request_type)));
	}

	return processed;
}

/*
 * Finish copy process on all connections
 */
void
DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int primary_dn_index, CombineType combine_type)
{
	int		i;
	RemoteQueryState *combiner = NULL;
	bool 		need_tran;
	bool 		error = false;
	struct timeval *timeout = NULL; /* wait forever */
	PGXCNodeHandle *connections[NumDataNodes];
	PGXCNodeHandle *primary_handle = NULL;
	int 		conn_count = 0;

	for (i = 0; i < NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];

		if (!handle)
			continue;

		if (i == primary_dn_index)
			primary_handle = handle;
		else
			connections[conn_count++] = handle;
	}

	if (primary_handle)
	{
		error = true;
		if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN || primary_handle->state == DN_CONNECTION_STATE_COPY_OUT)
			error = DataNodeCopyEnd(primary_handle, false);

		combiner = CreateResponseCombiner(conn_count + 1, combine_type);
		error = (pgxc_node_receive_responses(1, &primary_handle, timeout, combiner) != 0) || error;
	}

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];

		error = true;
		if (handle->state == DN_CONNECTION_STATE_COPY_IN || handle->state == DN_CONNECTION_STATE_COPY_OUT)
			error = DataNodeCopyEnd(handle, false);
	}

	need_tran = !autocommit || primary_handle || conn_count > 1;

	if (!combiner)
		combiner = CreateResponseCombiner(conn_count, combine_type);
	error = (pgxc_node_receive_responses(conn_count, connections, timeout, combiner) != 0) || error;

	if (!ValidateAndCloseCombiner(combiner) || error)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Error while running COPY")));
}

/*
 * End copy process on a connection
 */
bool
DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error)
{
	int 		nLen = htonl(4);

	if (handle == NULL)
		return true;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + 4, handle) != 0)
		return true;

	if (is_error)
		handle->outBuffer[handle->outEnd++] = 'f';
	else
		handle->outBuffer[handle->outEnd++] = 'c';

	memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
	handle->outEnd += 4;

	/* We need response right away, so send immediately */
	if (pgxc_node_flush(handle) < 0)
		return true;

	return false;
}

RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *remotestate;

	remotestate = CreateResponseCombiner(0, node->combine_type);
	remotestate->ss.ps.plan = (Plan *) node;
	remotestate->ss.ps.state = estate;

	remotestate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) remotestate);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK)));

	/* Extract the eflags bits that are relevant for tuplestorestate */
	remotestate->eflags = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD));

	/* We anyways have to support REWIND for ReScan */
	remotestate->eflags |= EXEC_FLAG_REWIND;

	remotestate->eof_underlying = false;
	remotestate->tuplestorestate = NULL;

	ExecInitResultTupleSlot(estate, &remotestate->ss.ps);
	if (node->scan.plan.targetlist)
	{
		TupleDesc typeInfo = ExecCleanTypeFromTL(node->scan.plan.targetlist, false);
		ExecSetSlotDescriptor(remotestate->ss.ps.ps_ResultTupleSlot, typeInfo);
	}
	else
	{
		/* In case there is no target list, force its creation */
		ExecAssignResultTypeFromTL(&remotestate->ss.ps);
	}

	ExecInitScanTupleSlot(estate, &remotestate->ss);

	remotestate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Tuple description for the scan slot will be set on runtime from
	 * a RowDescription message
	 */

	if (node->distinct)
	{
		/* prepare equate functions */
		remotestate->eqfunctions =
			execTuplesMatchPrepare(node->distinct->numCols,
								   node->distinct->eqOperators);
		/* create memory context for execTuplesMatch */
		remotestate->tmp_ctx =
			AllocSetContextCreate(CurrentMemoryContext,
								  "RemoteUnique",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
	}

	/*
	 * If there are parameters supplied, get them into a form to be sent to the
	 * datanodes with bind message. We should not have had done this before.
	 */
	if (estate->es_param_list_info)
	{
		Assert(!remotestate->paramval_data);
		remotestate->paramval_len = ParamListToDataRow(estate->es_param_list_info,
												&remotestate->paramval_data);
	}

	/* We need expression context to evaluate */
	if (node->exec_nodes && node->exec_nodes->en_expr)
	{
		Expr *expr = node->exec_nodes->en_expr;

		if (IsA(expr, Var) && ((Var *) expr)->vartype == TIDOID)
		{
			/* Special case if expression does not need to be evaluated */
		}
		else
		{
			/*
			 * Inner plan provides parameter values and may be needed
			 * to determine target nodes. In this case expression is evaluated
			 * and we should made values available for evaluator.
			 * So allocate storage for the values.
			 */
			if (innerPlan(node))
			{
				int nParams = list_length(node->scan.plan.targetlist);
				estate->es_param_exec_vals = (ParamExecData *) palloc0(
						nParams * sizeof(ParamExecData));
			}
			/* prepare expression evaluation */
			ExecAssignExprContext(estate, &remotestate->ss.ps);
		}
	}
	else if (remotestate->ss.ps.qual)
		ExecAssignExprContext(estate, &remotestate->ss.ps);

	if (innerPlan(node))
		innerPlanState(remotestate) = ExecInitNode(innerPlan(node), estate, eflags);

	if (outerPlan(node))
		outerPlanState(remotestate) = ExecInitNode(outerPlan(node), estate, eflags);

	return remotestate;
}


static void
copy_slot(RemoteQueryState *node, TupleTableSlot *src, TupleTableSlot *dst)
{
	if (src->tts_dataRow
			&& dst->tts_tupleDescriptor->natts == src->tts_tupleDescriptor->natts)
	{
		if (src->tts_mcxt == dst->tts_mcxt)
		{
			/* now dst slot controls the backing message */
			ExecStoreDataRowTuple(src->tts_dataRow, src->tts_dataLen,
								  src->tts_dataNodeIndex, dst,
								  src->tts_shouldFreeRow);
			src->tts_shouldFreeRow = false;
		}
		else
		{
			/* have to make a copy */
			MemoryContext	oldcontext = MemoryContextSwitchTo(dst->tts_mcxt);
			int 		len = src->tts_dataLen;
			char		*msg = (char *) palloc(len);

			memcpy(msg, src->tts_dataRow, len);
			ExecStoreDataRowTuple(msg, len, src->tts_dataNodeIndex, dst, true);
			MemoryContextSwitchTo(oldcontext);
		}
	}
	else
	{
		int i;

		/*
		 * Data node may be sending junk columns which are always at the end,
		 * but it must not be shorter then result slot.
		 */
		Assert(dst->tts_tupleDescriptor->natts <= src->tts_tupleDescriptor->natts);
		ExecClearTuple(dst);
		slot_getallattrs(src);
		/*
		 * PGXCTODO revisit: if it is correct to copy Datums using assignment?
		 */
		for (i = 0; i < dst->tts_tupleDescriptor->natts; i++)
		{
			dst->tts_values[i] = src->tts_values[i];
			dst->tts_isnull[i] = src->tts_isnull[i];
		}
		ExecStoreVirtualTuple(dst);
	}
}

/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 */
static PGXCNodeAllHandles *
get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type)
{
	List 	   *nodelist = NIL;
	List 	   *primarynode = NIL;
	List	   *coordlist = NIL;
	PGXCNodeHandle *primaryconnection;
	int			co_conn_count, dn_conn_count;
	bool		is_query_coord_only = false;
	PGXCNodeAllHandles *pgxc_handles = NULL;

	/*
	 * If query is launched only on Coordinators, we have to inform get_handles
	 * not to ask for Datanode connections even if list of Datanodes is NIL.
	 */
	if (exec_type == EXEC_ON_COORDS)
		is_query_coord_only = true;

	if (exec_nodes)
	{
		if (exec_nodes->en_expr)
		{
			/*
			 * Special case (argh, another one): if expression data type is TID
			 * the ctid value is specific to the node from which it has been
			 * returned.
			 * So try and determine originating node and execute command on
			 * that node only
			 */
			if (IsA(exec_nodes->en_expr, Var) && ((Var *) exec_nodes->en_expr)->vartype == TIDOID)
			{
				Var 	   *ctid = (Var *) exec_nodes->en_expr;
				PlanState  *source = (PlanState *) planstate;
				TupleTableSlot *slot;

				/* Find originating RemoteQueryState */
				if (ctid->varno == INNER)
					source = innerPlanState(source);
				else if (ctid->varno == OUTER)
					source = outerPlanState(source);

				while (!IsA(source, RemoteQueryState))
				{
					TargetEntry *tle = list_nth(source->plan->targetlist,
												ctid->varattno - 1);
					Assert(IsA(tle->expr, Var));
					ctid = (Var *) tle->expr;
					if (ctid->varno == INNER)
						source = innerPlanState(source);
					else if (ctid->varno == OUTER)
						source = outerPlanState(source);
					else
						elog(ERROR, "failed to determine target node");
				}

				slot = source->ps_ResultTupleSlot;
				/* The slot should be of type DataRow */
				Assert(!TupIsNull(slot) && slot->tts_dataRow);

				nodelist = list_make1_int(slot->tts_dataNodeIndex);
				primarynode = NIL;
			}
			else
			{
				/* execution time determining of target data nodes */
				bool isnull;
				ExprState *estate = ExecInitExpr(exec_nodes->en_expr,
												 (PlanState *) planstate);
				Datum partvalue = ExecEvalExpr(estate,
											   planstate->ss.ps.ps_ExprContext,
											   &isnull,
											   NULL);
				if (!isnull)
				{
					RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
					ExecNodes  *nodes = GetRelationNodes(rel_loc_info,
														 partvalue,
														 exprType((Node *) exec_nodes->en_expr),
														 exec_nodes->accesstype);
					if (nodes)
					{
						nodelist = nodes->nodeList;
						primarynode = nodes->primarynodelist;
						pfree(nodes);
					}
					FreeRelationLocInfo(rel_loc_info);
				}
			}
		}
		else
		{
			if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				nodelist = exec_nodes->nodeList;
			else if (exec_type == EXEC_ON_COORDS)
				coordlist = exec_nodes->nodeList;

			primarynode = exec_nodes->primarynodelist;
		}
	}

	/* Set node list and DN number */
	if (list_length(nodelist) == 0 &&
		(exec_type == EXEC_ON_ALL_NODES ||
		 exec_type == EXEC_ON_DATANODES))
	{
		/* Primary connection is included in this number of connections if it exists */
		dn_conn_count = NumDataNodes;
	}
	else
	{
		if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
		{
			if (primarynode)
				dn_conn_count = list_length(nodelist) + 1;
			else
				dn_conn_count = list_length(nodelist);
		}
		else
			dn_conn_count = 0;
	}

	/* Set Coordinator list and coordinator number */
	if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES) ||
		(list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS))
	{
		co_conn_count = NumCoords;
		coordlist = GetAllCoordNodes();
	}
	else
	{
		if (exec_type == EXEC_ON_COORDS)
			co_conn_count = list_length(coordlist);
		else
			co_conn_count = 0;
	}

	/* Get other connections (non-primary) */
	pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only);
	if (!pgxc_handles)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not obtain connection from pool")));

	/* Get connection for primary node, if used */
	if (primarynode)
	{
		/* Let's assume primary connection is always a datanode connection for the moment */
		PGXCNodeAllHandles *pgxc_conn_res;
		pgxc_conn_res = get_handles(primarynode, NULL, false);

		/* primary connection is unique */
		primaryconnection = pgxc_conn_res->datanode_handles[0];

		pfree(pgxc_conn_res);

		if (!primaryconnection)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not obtain connection from pool")));
		pgxc_handles->primary_handle = primaryconnection;
	}

	/* Depending on the execution type, we still need to save the initial node counts */
	pgxc_handles->dn_conn_count = dn_conn_count;
	pgxc_handles->co_conn_count = co_conn_count;

	return pgxc_handles;
}

/*
 * We would want to run 2PC if current transaction modified more then
 * one node. So optimize little bit and do not look further if we
 * already have more then one write nodes.
 */
static void
register_write_nodes(int conn_count, PGXCNodeHandle **connections)
{
	int 		i, j;

	for (i = 0; i < conn_count && write_node_count < 2; i++)
	{
		bool found = false;

		for (j = 0; j < write_node_count && !found; j++)
		{
			if (write_node_list[j] == connections[i])
				found = true;
		}
		if (!found)
		{
			/* Add to transaction wide-list */
			write_node_list[write_node_count++] = connections[i];
		}
	}
}

static bool
pgxc_start_command_on_connection(PGXCNodeHandle *connection, bool need_tran,
									GlobalTransactionId gxid, TimestampTz timestamp,
									RemoteQueryState *remotestate, int total_conn_count,
									Snapshot snapshot)
{
	RemoteQuery	*step = (RemoteQuery *) remotestate->ss.ps.plan;
	if (connection->state == DN_CONNECTION_STATE_QUERY)
		BufferConnection(connection);

	/* If explicit transaction is needed gxid is already sent */
	if (!need_tran && pgxc_node_send_gxid(connection, gxid))
		return false;
	if (total_conn_count == 1 && pgxc_node_send_timestamp(connection, timestamp))
		return false;
	if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
		return false;
	if (step->statement || step->cursor || step->param_types)
	{
		/* need to use Extended Query Protocol */
		int	fetch = 0;
		bool	prepared = false;

		/* if prepared statement is referenced see if it is already exist */
		if (step->statement)
			prepared = ActivateDatanodeStatementOnNode(step->statement,
													   PGXCNodeGetNodeId(connection->nodeoid,
																		 PGXC_NODE_DATANODE_MASTER));
		/*
		 * execute and fetch rows only if they will be consumed
		 * immediately by the sorter
		 */
		if (step->cursor)
			fetch = 1;

		if (pgxc_node_send_query_extended(connection,
							prepared ? NULL : step->sql_statement,
							step->statement,
							step->cursor,
							step->num_params,
							step->param_types,
							remotestate->paramval_len,
							remotestate->paramval_data,
							step->read_only,
							fetch) != 0)
			return false;
	}
	else
	{
		if (pgxc_node_send_query(connection, step->sql_statement) != 0)
			return false;
	}
	return true;
}

static void
do_query(RemoteQueryState *node)
{
	RemoteQuery		*step = (RemoteQuery *) node->ss.ps.plan;
	TupleTableSlot		*scanslot = node->ss.ss_ScanTupleSlot;
	bool			force_autocommit = step->force_autocommit;
	bool			is_read_only = step->read_only;
	GlobalTransactionId	gxid = InvalidGlobalTransactionId;
	Snapshot		snapshot = GetActiveSnapshot();
	TimestampTz		timestamp = GetCurrentGTMStartTimestamp();
	PGXCNodeHandle		**connections = NULL;
	PGXCNodeHandle		*primaryconnection = NULL;
	int			i;
	int			regular_conn_count;
	int			total_conn_count;
	bool			need_tran;
	PGXCNodeAllHandles	*pgxc_connections;

	/* Be sure to set temporary object flag if necessary */
	if (step->is_temp)
		temp_object_included = true;

	/*
	 * Get connections for Datanodes only, utilities and DDLs
	 * are launched in ExecRemoteUtility
	 */
	pgxc_connections = get_exec_connections(node, step->exec_nodes, step->exec_type);

	if (step->exec_type == EXEC_ON_DATANODES)
	{
		connections = pgxc_connections->datanode_handles;
		total_conn_count = regular_conn_count = pgxc_connections->dn_conn_count;
	}
	else if (step->exec_type == EXEC_ON_COORDS)
	{
		connections = pgxc_connections->coord_handles;
		total_conn_count = regular_conn_count = pgxc_connections->co_conn_count;
	}

	primaryconnection = pgxc_connections->primary_handle;

	/*
	 * Primary connection is counted separately but is included in total_conn_count if used.
	 */
	if (primaryconnection)
		regular_conn_count--;

	pfree(pgxc_connections);

	/*
	 * We save only regular connections, at the time we exit the function
	 * we finish with the primary connection and deal only with regular
	 * connections on subsequent invocations
	 */
	node->node_count = regular_conn_count;

	if (force_autocommit)
		need_tran = false;
	else
		need_tran = !autocommit || (!is_read_only && total_conn_count > 1);

	elog(DEBUG1, "autocommit = %s, has primary = %s, regular_conn_count = %d, need_tran = %s", autocommit ? "true" : "false", primaryconnection ? "true" : "false", regular_conn_count, need_tran ? "true" : "false");

	stat_statement();
	if (autocommit)
	{
		stat_transaction(total_conn_count);
		/* We normally clear for transactions, but if autocommit, clear here, too */
		clear_write_node_list();
	}

	if (!is_read_only)
	{
		if (primaryconnection)
			register_write_nodes(1, &primaryconnection);
		register_write_nodes(regular_conn_count, connections);
	}

	gxid = GetCurrentGlobalTransactionId();

	if (!GlobalTransactionIdIsValid(gxid))
	{
		if (primaryconnection)
			pfree(primaryconnection);
		pfree(connections);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get next transaction ID")));
	}

	if (need_tran)
	{
		/*
		 * Check if data node connections are in transaction and start
		 * transactions on nodes where it is not started
		 */
		PGXCNodeHandle *new_connections[total_conn_count];
		int 		new_count = 0;

		if (primaryconnection && primaryconnection->transaction_status != 'T')
			new_connections[new_count++] = primaryconnection;
		for (i = 0; i < regular_conn_count; i++)
			if (connections[i]->transaction_status != 'T')
				new_connections[new_count++] = connections[i];

		if (new_count && pgxc_node_begin(new_count, new_connections, gxid))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on data nodes.")));
	}

	/* See if we have a primary node, execute on it first before the others */
	if (primaryconnection)
	{
		if (!pgxc_start_command_on_connection(primaryconnection, need_tran, gxid,
												timestamp, node, total_conn_count, snapshot))
		{
			pfree(connections);
			pfree(primaryconnection);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to data nodes")));
		}
		Assert(node->combine_type == COMBINE_TYPE_SAME);

		/* Make sure the command is completed on the primary node */
		while (true)
		{
			int res;
			if (pgxc_node_receive(1, &primaryconnection, NULL))
				break;

			res = handle_response(primaryconnection, node);
			if (res == RESPONSE_COMPLETE)
				break;
			else if (res == RESPONSE_EOF)
				continue;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Unexpected response from data node")));
		}
		if (node->errorMessage)
		{
			char *code = node->errorCode;
			if (node->errorDetail != NULL)
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", node->errorMessage), errdetail("%s", node->errorDetail) ));
			else
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", node->errorMessage)));
		}
	}

	for (i = 0; i < regular_conn_count; i++)
	{
		if (!pgxc_start_command_on_connection(connections[i], need_tran, gxid,
												timestamp, node, total_conn_count, snapshot))
		{
			pfree(connections);
			if (primaryconnection)
				pfree(primaryconnection);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to data nodes")));
		}
		connections[i]->combiner = node;
	}

	if (step->cursor)
	{
		node->cursor = step->cursor;
		node->cursor_count = regular_conn_count;
		node->cursor_connections = (PGXCNodeHandle **) palloc(regular_conn_count * sizeof(PGXCNodeHandle *));
		memcpy(node->cursor_connections, connections, regular_conn_count * sizeof(PGXCNodeHandle *));
	}

	/*
	 * Stop if all commands are completed or we got a data row and
	 * initialized state node for subsequent invocations
	 */
	while (regular_conn_count > 0 && node->connections == NULL)
	{
		int i = 0;

		if (pgxc_node_receive(regular_conn_count, connections, NULL))
		{
			pfree(connections);
			if (primaryconnection)
				pfree(primaryconnection);
			if (node->cursor_connections)
				pfree(node->cursor_connections);

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to read response from data nodes")));
		}
		/*
		 * Handle input from the data nodes.
		 * If we got a RESPONSE_DATAROW we can break handling to wrap
		 * it into a tuple and return. Handling will be continued upon
		 * subsequent invocations.
		 * If we got 0, we exclude connection from the list. We do not
		 * expect more input from it. In case of non-SELECT query we quit
		 * the loop when all nodes finish their work and send ReadyForQuery
		 * with empty connections array.
		 * If we got EOF, move to the next connection, will receive more
		 * data on the next iteration.
		 */
		while (i < regular_conn_count)
		{
			int res = handle_response(connections[i], node);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (i < --regular_conn_count)
					connections[i] = connections[regular_conn_count];
			}
			else if (res == RESPONSE_TUPDESC)
			{
				ExecSetSlotDescriptor(scanslot, node->tuple_desc);
				/*
				 * Now tuple table slot is responsible for freeing the
				 * descriptor
				 */
				node->tuple_desc = NULL;
				if (step->sort)
				{
					SimpleSort *sort = step->sort;

					node->connections = connections;
					node->conn_count = regular_conn_count;
					/*
					 * First message is already in the buffer
					 * Further fetch will be under tuplesort control
					 * If query does not produce rows tuplesort will not
					 * be initialized
					 */
					node->tuplesortstate = tuplesort_begin_merge(
										   scanslot->tts_tupleDescriptor,
										   sort->numCols,
										   sort->sortColIdx,
										   sort->sortOperators,
										   sort->nullsFirst,
										   node,
										   work_mem);
					/*
					 * Break the loop, do not wait for first row.
					 * Tuplesort module want to control node it is
					 * fetching rows from, while in this loop first
					 * row would be got from random node
					 */
					break;
				}
			}
			else if (res == RESPONSE_DATAROW)
			{
				/*
				 * Got first data row, quit the loop
				 */
				node->connections = connections;
				node->conn_count = regular_conn_count;
				node->current_conn = i;
				break;
			}
		}
	}

	if (node->cursor_count)
	{
		node->conn_count = node->cursor_count;
		memcpy(connections, node->cursor_connections, node->cursor_count * sizeof(PGXCNodeHandle *));
		node->connections = connections;
	}
}

/*
 * ExecRemoteQueryInnerPlan
 * Executes the inner plan of a RemoteQuery. It returns false if the inner plan
 * does not return any row, otherwise it returns true.
 */
static bool
ExecRemoteQueryInnerPlan(RemoteQueryState *node)
{
	EState		   *estate = node->ss.ps.state;
	TupleTableSlot *innerSlot = ExecProcNode(innerPlanState(node));
	/*
	 * Use data row returned by the previus step as a parameters for
	 * the main query.
	 */
	if (!TupIsNull(innerSlot))
	{
		node->paramval_len = ExecCopySlotDatarow(innerSlot,
												 &node->paramval_data);

		/* Needed for expression evaluation */
		if (estate->es_param_exec_vals)
		{
			int i;
			int natts = innerSlot->tts_tupleDescriptor->natts;

			slot_getallattrs(innerSlot);
			for (i = 0; i < natts; i++)
				estate->es_param_exec_vals[i].value = slot_getattr(
						innerSlot,
						i+1,
						&estate->es_param_exec_vals[i].isnull);
		}
		return true;
	}
	return false;
}

/*
 * apply_qual
 * Applies the qual of the query, and returns the same slot if
 * the qual returns true, else returns NULL
 */
static TupleTableSlot *
apply_qual(TupleTableSlot *slot, RemoteQueryState *node)
{
   List *qual = node->ss.ps.qual;
   if (qual)
   {
	   ExprContext *econtext = node->ss.ps.ps_ExprContext;
	   econtext->ecxt_scantuple = slot;
	   if (!ExecQual(qual, econtext, false))
		   return NULL;
   }

   return slot;
}

/*
 * ExecRemoteQuery
 * Wrapper around the main RemoteQueryNext() function. This
 * wrapper provides materialization of the result returned by
 * RemoteQueryNext
 */

TupleTableSlot *
ExecRemoteQuery(RemoteQueryState *node)
{
	TupleTableSlot *resultslot = node->ss.ps.ps_ResultTupleSlot;
	Tuplestorestate *tuplestorestate;
	bool		eof_tuplestore;
	bool forward = ScanDirectionIsForward(node->ss.ps.state->es_direction);
	TupleTableSlot *slot;

	/*
	 * If sorting is needed, then tuplesortstate takes care of
	 * materialization
	 */
	if (((RemoteQuery *) node->ss.ps.plan)->sort)
		return RemoteQueryNext(node);


	tuplestorestate = node->tuplestorestate;

	if (tuplestorestate == NULL)
	{
		tuplestorestate = tuplestore_begin_heap(true, false, work_mem);
		tuplestore_set_eflags(tuplestorestate, node->eflags);
		node->tuplestorestate = tuplestorestate;
	}

	/*
	 * If we are not at the end of the tuplestore, or are going backwards, try
	 * to fetch a tuple from tuplestore.
	 */
	eof_tuplestore = (tuplestorestate == NULL) ||
		tuplestore_ateof(tuplestorestate);

	if (!forward && eof_tuplestore)
	{
		if (!node->eof_underlying)
		{
			/*
			 * When reversing direction at tuplestore EOF, the first
			 * gettupleslot call will fetch the last-added tuple; but we want
			 * to return the one before that, if possible. So do an extra
			 * fetch.
			 */
			if (!tuplestore_advance(tuplestorestate, forward))
				return NULL;	/* the tuplestore must be empty */
		}
		eof_tuplestore = false;
	}

	/*
	 * If we can fetch another tuple from the tuplestore, return it.
	 */
	slot = node->ss.ps.ps_ResultTupleSlot;
	if (!eof_tuplestore)
	{
		/* Look for the first tuple that matches qual */
		while (tuplestore_gettupleslot(tuplestorestate, forward, false, slot))
		{
			if (apply_qual(slot, node))
				return slot;
		}
		/* Not found */
		if (forward)
			eof_tuplestore = true;
	}


	/*
	 * If tuplestore has reached its end but the underlying RemoteQueryNext() hasn't
	 * finished yet, try to fetch another row.
	 */
	if (eof_tuplestore && !node->eof_underlying)
	{
		TupleTableSlot *outerslot;

		while (1)
		{
			/*
			 * We can only get here with forward==true, so no need to worry about
			 * which direction the subplan will go.
			 */
			outerslot = RemoteQueryNext(node);
			if (TupIsNull(outerslot))
			{
				node->eof_underlying = true;
				return NULL;
			}

			/*
			 * Append a copy of the returned tuple to tuplestore.  NOTE: because
			 * the tuplestore is certainly in EOF state, its read position will
			 * move forward over the added tuple.  This is what we want.
			 */
			if (tuplestorestate)
				tuplestore_puttupleslot(tuplestorestate, outerslot);

			/*
			 * If qual returns true, return the fetched tuple, else continue for
			 * next tuple
			 */
			if (apply_qual(outerslot, node))
				return outerslot;
		}

	}

	return ExecClearTuple(resultslot);
}

/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the data nodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
static TupleTableSlot *
RemoteQueryNext(RemoteQueryState *node)
{
	RemoteQuery    *step = (RemoteQuery *) node->ss.ps.plan;
	TupleTableSlot *resultslot = node->ss.ps.ps_ResultTupleSlot;
	TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;
	bool have_tuple = false;
	List			*qual = node->ss.ps.qual;
	ExprContext		*econtext = node->ss.ps.ps_ExprContext;

	if (!node->query_Done)
	{
		/*
		 * Inner plan for RemoteQuery supplies parameters.
		 * We execute inner plan to get a tuple and use values of the tuple as
		 * parameter values when executing this remote query.
		 * If returned slot contains NULL tuple break execution.
		 * TODO there is a problem how to handle the case if both inner and
		 * outer plans exist. We can decide later, since it is never used now.
		 */
		if (innerPlanState(node))
		{
			if (!ExecRemoteQueryInnerPlan(node))
			{
				/* no parameters, exit */
				return NULL;
			}
		}
		do_query(node);

		node->query_Done = true;
	}

	if (node->update_cursor)
	{
		PGXCNodeAllHandles *all_dn_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
		close_node_cursors(all_dn_handles->datanode_handles,
						  all_dn_handles->dn_conn_count,
						  node->update_cursor);
		pfree(node->update_cursor);
		node->update_cursor = NULL;
		pfree_pgxc_all_handles(all_dn_handles);
	}

handle_results:
	if (node->tuplesortstate)
	{
		while (tuplesort_gettupleslot((Tuplesortstate *) node->tuplesortstate,
									  true, scanslot))
		{
			if (qual)
				econtext->ecxt_scantuple = scanslot;
			if (!qual || ExecQual(qual, econtext, false))
				have_tuple = true;
			else
			{
				have_tuple = false;
				continue;
			}
			/*
			 * If DISTINCT is specified and current tuple matches to
			 * previous skip it and get next one.
			 * Othervise return current tuple
			 */
			if (step->distinct)
			{
				/*
				 * Always receive very first tuple and
				 * skip to next if scan slot match to previous (result slot)
				 */
				if (!TupIsNull(resultslot) &&
						execTuplesMatch(scanslot,
										resultslot,
										step->distinct->numCols,
										step->distinct->uniqColIdx,
										node->eqfunctions,
										node->tmp_ctx))
				{
					have_tuple = false;
					continue;
				}
			}
			copy_slot(node, scanslot, resultslot);
			break;
		}
		if (!have_tuple)
			ExecClearTuple(resultslot);
	}
	else
	{
		if (FetchTuple(node, scanslot) && !TupIsNull(scanslot))
		{
			if (qual)
				econtext->ecxt_scantuple = scanslot;
			copy_slot(node, scanslot, resultslot);
		}
		else
			ExecClearTuple(resultslot);
	}

	if (node->errorMessage)
	{
		char *code = node->errorCode;
		if (node->errorDetail != NULL)
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", node->errorMessage), errdetail("%s", node->errorDetail) ));
		else
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", node->errorMessage)));
	}

	/*
	 * While we are emitting rows we ignore outer plan
	 */
	if (!TupIsNull(resultslot))
		return resultslot;

	/*
	 * We can not use recursion here. We can run out of the stack memory if
	 * inner node returns long result set and this node does not returns rows
	 * (like INSERT ... SELECT)
	 */
	if (innerPlanState(node))
	{
		if (ExecRemoteQueryInnerPlan(node))
		{
			do_query(node);
			goto handle_results;
		}
	}

	/*
	 * Execute outer plan if specified
	 */
	if (outerPlanState(node))
	{
		TupleTableSlot *slot = ExecProcNode(outerPlanState(node));
		if (!TupIsNull(slot))
			return slot;
	}

	/*
	 * OK, we have nothing to return, so return NULL
	 */
	return NULL;
}

/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	ListCell *lc;

	/*
	 * shut down the subplan
	 */
	if (innerPlanState(node))
		ExecEndNode(innerPlanState(node));

	/* clean up the buffer */
	foreach(lc, node->rowBuffer)
	{
		RemoteDataRow dataRow = (RemoteDataRow) lfirst(lc);
		pfree(dataRow->msg);
	}
	list_free_deep(node->rowBuffer);

	node->current_conn = 0;
	while (node->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = node->connections[node->current_conn];

		/* throw away message */
		if (node->currentRow.msg)
		{
			pfree(node->currentRow.msg);
			node->currentRow.msg = NULL;
		}

		if (conn == NULL)
		{
			node->conn_count--;
			continue;
		}

		/* no data is expected */
		if (conn->state == DN_CONNECTION_STATE_IDLE ||
				conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
		{
			if (node->current_conn < --node->conn_count)
				node->connections[node->current_conn] = node->connections[node->conn_count];
			continue;
		}
		res = handle_response(conn, node);
		if (res == RESPONSE_EOF)
		{
			struct timeval timeout;
			timeout.tv_sec = END_QUERY_TIMEOUT;
			timeout.tv_usec = 0;

			if (pgxc_node_receive(1, &conn, &timeout))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to read response from data nodes when ending query")));
		}
	}

	if (node->tuplesortstate != NULL || node->tuplestorestate != NULL)
		ExecClearTuple(node->ss.ss_ScanTupleSlot);
	/*
	 * Release tuplesort resources
	 */
	if (node->tuplesortstate != NULL)
		tuplesort_end((Tuplesortstate *) node->tuplesortstate);
	node->tuplesortstate = NULL;
	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * If there are active cursors close them
	 */
	if (node->cursor || node->update_cursor)
	{
		PGXCNodeAllHandles *all_handles = NULL;
		PGXCNodeHandle    **cur_handles;
		bool bFree = false;
		int nCount;
		int i;
	
		cur_handles = node->cursor_connections;
		nCount = node->cursor_count;
	
		for(i=0;i<node->cursor_count;i++)
		{
			if (node->cursor_connections == NULL || node->cursor_connections[i]->sock == -1)
			{
				bFree = true;
				all_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
				cur_handles = all_handles->datanode_handles;
				nCount = all_handles->dn_conn_count;
				break;
			}
		}
	
		if (node->cursor)
		{
			close_node_cursors(cur_handles, nCount, node->cursor);
			pfree(node->cursor);
			node->cursor = NULL;
		}
	
		if (node->update_cursor)
		{
			close_node_cursors(cur_handles, nCount, node->update_cursor);
			pfree(node->update_cursor);
			node->update_cursor = NULL;
		}
	
		if (bFree)
			pfree_pgxc_all_handles(all_handles);
	}

	/*
	 * Clean up parameters if they were set
	 */
	if (node->paramval_data)
	{
		pfree(node->paramval_data);
		node->paramval_data = NULL;
		node->paramval_len = 0;
	}

	/*
	 * shut down the subplan
	 */
	if (outerPlanState(node))
		ExecEndNode(outerPlanState(node));

	if (node->ss.ss_currentRelation)
		ExecCloseScanRelation(node->ss.ss_currentRelation);

	if (node->tmp_ctx)
		MemoryContextDelete(node->tmp_ctx);

	CloseCombiner(node);
}

static void
close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor)
{
	int i;
	RemoteQueryState *combiner;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], false, cursor) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node cursor")));
		if (pgxc_node_send_sync(connections[i]) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node cursor")));
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node cursor")));
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				// Unexpected response, ignore?
			}
		}
	}

	ValidateAndCloseCombiner(combiner);
}


/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to data nodes.
 * The buffer to store encoded value is palloc'ed and returned as the result
 * parameter. Function returns size of the result
 */
int
ParamListToDataRow(ParamListInfo params, char** result)
{
	StringInfoData buf;
	uint16 n16;
	int i;
	int real_num_params = params->numParams;

	/*
	 * It is necessary to fetch parameters
	 * before looking at the output value.
	 */
	for (i = 0; i < params->numParams; i++)
	{
		ParamExternData *param;

		param = &params->params[i];

		if (!OidIsValid(param->ptype) && params->paramFetch != NULL)
			(*params->paramFetch) (params, i + 1);

		/*
		 * In case parameter type is not defined, it is not necessary to include
		 * it in message sent to backend nodes.
		 */
		if (!OidIsValid(param->ptype))
			real_num_params--;
	}

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(real_num_params);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < params->numParams; i++)
	{
		ParamExternData *param = &params->params[i];
		uint32 n32;

		/* If parameter has no type defined it is not necessary to include it in message */
		if (!OidIsValid(param->ptype))
			continue;

		if (param->isnull)
		{
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
		else
		{
			Oid		typOutput;
			bool	typIsVarlena;
			Datum	pval;
			char   *pstring;
			int		len;

			/* Get info needed to output the value */
			getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
			else
				pval = param->value;

			/* Convert Datum to string */
			pstring = OidOutputFunctionCall(typOutput, pval);

			/* copy data to the buffer */
			len = strlen(pstring);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, pstring, len);
		}
	}

	/* Take data from the buffer */
	*result = palloc(buf.len);
	memcpy(*result, buf.data, buf.len);
	pfree(buf.data);
	return buf.len;
}


/* ----------------------------------------------------------------
 *		ExecRemoteQueryReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt)
{
	/*
	 * If the materialized store is not empty, just rewind the stored output.
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (((RemoteQuery *) node->ss.ps.plan)->sort)
	{
		if (!node->tuplesortstate)
			return;

		tuplesort_rescan(node->tuplesortstate);
	}
	else
	{
		if (!node->tuplestorestate)
			return;

		tuplestore_rescan(node->tuplestorestate);
	}

}


/*
 * Execute utility statement on multiple data nodes
 * It does approximately the same as
 *
 * RemoteQueryState *state = ExecInitRemoteQuery(plan, estate, flags);
 * Assert(TupIsNull(ExecRemoteQuery(state));
 * ExecEndRemoteQuery(state)
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void
ExecRemoteUtility(RemoteQuery *node)
{
	RemoteQueryState *remotestate;
	bool		force_autocommit = node->force_autocommit;
	bool		is_read_only = node->read_only;
	RemoteQueryExecType exec_type = node->exec_type;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot snapshot = GetActiveSnapshot();
	PGXCNodeAllHandles *pgxc_connections;
	int			total_conn_count;
	int			co_conn_count;
	int			dn_conn_count;
	bool		need_tran;
	ExecDirectType		exec_direct_type = node->exec_direct_type;
	int			i;

	if (!force_autocommit)
		is_ddl = true;

	implicit_force_autocommit = force_autocommit;

	/*
	 * A transaction using temporary objects cannot use 2PC.
	 * It is possible to invoke create table with inheritance on
	 * temporary objects, so in this case temp_object_included flag
	 * is already assigned when analyzing inner relations.
	 */
	if (!temp_object_included)
		temp_object_included = node->is_temp;

	remotestate = CreateResponseCombiner(0, node->combine_type);

	pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type);

	dn_conn_count = pgxc_connections->dn_conn_count;

	/*
	 * EXECUTE DIRECT can only be launched on a single node
	 * but we have to count local node also here.
	 */
	if (exec_direct_type != EXEC_DIRECT_NONE && exec_type == EXEC_ON_COORDS)
		co_conn_count = 2;
	else
		co_conn_count = pgxc_connections->co_conn_count;

	/* Registering new connections needs the sum of Connections to Datanodes AND to Coordinators */
	total_conn_count = dn_conn_count + co_conn_count;

	if (force_autocommit)
		need_tran = false;
	else if (exec_type == EXEC_ON_ALL_NODES ||
			 exec_type == EXEC_ON_COORDS)
		need_tran = true;
	else
		need_tran = !autocommit || total_conn_count > 1;

	/* Commands launched through EXECUTE DIRECT do not need start a transaction */
	if (exec_direct_type == EXEC_DIRECT_UTILITY)
	{
		need_tran = false;

		/* This check is not done when analyzing to limit dependencies */
		if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
	}

	if (!is_read_only)
	{
		register_write_nodes(dn_conn_count, pgxc_connections->datanode_handles);
	}

	gxid = GetCurrentGlobalTransactionId();
	if (!GlobalTransactionIdIsValid(gxid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get next transaction ID")));
	}

	if (need_tran)
	{
		/*
		 * Check if data node connections are in transaction and start
		 * transactions on nodes where it is not started
		 */
		PGXCNodeHandle *new_connections[total_conn_count];
		int 		new_count = 0;

		/* Check for Datanodes */
		for (i = 0; i < dn_conn_count; i++)
			if (pgxc_connections->datanode_handles[i]->transaction_status != 'T')
				new_connections[new_count++] = pgxc_connections->datanode_handles[i];

		if (exec_type == EXEC_ON_ALL_NODES ||
			exec_type == EXEC_ON_DATANODES)
		{
			if (new_count && pgxc_node_begin(new_count, new_connections, gxid))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Could not begin transaction on data nodes")));
		}

		/* Check Coordinators also and begin there if necessary */
		new_count = 0;
		if (exec_type == EXEC_ON_ALL_NODES ||
			exec_type == EXEC_ON_COORDS)
		{
			/* Important not to count the connection of local coordinator! */
			for (i = 0; i < co_conn_count - 1; i++)
				if (pgxc_connections->coord_handles[i]->transaction_status != 'T')
					new_connections[new_count++] = pgxc_connections->coord_handles[i];

			if (new_count && pgxc_node_begin(new_count, new_connections, gxid))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Could not begin transaction on Coordinators")));
		}
	}

	/* Send query down to Datanodes */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_DATANODES)
	{
		for (i = 0; i < dn_conn_count; i++)
		{
			PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];

			if (conn->state == DN_CONNECTION_STATE_QUERY)
				BufferConnection(conn);
			/* If explicit transaction is needed gxid is already sent */
			if (!need_tran && pgxc_node_send_gxid(conn, gxid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (snapshot && pgxc_node_send_snapshot(conn, snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (pgxc_node_send_query(conn, node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
		}
	}

	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_COORDS)
	{
		/* Now send it to Coordinators if necessary */
		for (i = 0; i < co_conn_count - 1; i++)
		{
			/* If explicit transaction is needed gxid is already sent */
			if (!need_tran && pgxc_node_send_gxid(pgxc_connections->coord_handles[i], gxid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (snapshot && pgxc_node_send_snapshot(pgxc_connections->coord_handles[i], snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (pgxc_node_send_query(pgxc_connections->coord_handles[i], node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
		}
	}

	/*
	 * Stop if all commands are completed or we got a data row and
	 * initialized state node for subsequent invocations
	 */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_DATANODES)
	{
		while (dn_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL))
				break;
			/*
			 * Handle input from the data nodes.
			 * We do not expect data nodes returning tuples when running utility
			 * command.
			 * If we got EOF, move to the next connection, will receive more
			 * data on the next iteration.
			 */
			while (i < dn_conn_count)
			{
				PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];
				int res = handle_response(conn, remotestate);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (i < --dn_conn_count)
						pgxc_connections->datanode_handles[i] =
							pgxc_connections->datanode_handles[dn_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from data node")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from data node")));
				}
			}
		}
	}

	/* Make the same for Coordinators */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_COORDS)
	{
		/* For local Coordinator */
		co_conn_count--;
		while (co_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(co_conn_count, pgxc_connections->coord_handles, NULL))
				break;

			while (i < co_conn_count)
			{
				int res = handle_response(pgxc_connections->coord_handles[i], remotestate);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (i < --co_conn_count)
						pgxc_connections->coord_handles[i] =
							 pgxc_connections->coord_handles[co_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
			}
		}
	}
	/*
	 * We have processed all responses from nodes and if we have
	 * error message pending we can report it. All connections should be in
	 * consistent state now and so they can be released to the pool after ROLLBACK.
	 */
	if (remotestate->errorMessage)
	{
		char *code = remotestate->errorCode;
		if (remotestate->errorDetail != NULL)
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", remotestate->errorMessage), errdetail("%s", remotestate->errorDetail) ));
		else
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", remotestate->errorMessage)));
	}
}


/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{
	/* Rollback on Data Nodes */
	if (IsTransactionState())
	{
		PGXCNodeRollback();

		/* Rollback on GTM if transaction id opened. */
		RollbackTranGTM((GlobalTransactionId) GetCurrentTransactionIdIfAny());
	}

	/* Clean up prepared transactions before releasing connections */
	DropAllPreparedStatements();

	/* Release data node connections */
	release_handles();

	/* Disconnect from Pooler */
	PoolManagerDisconnect();

	/* Close connection with GTM */
	CloseGTM();

	/* Dump collected statistics to the log */
	stat_log();
}


/*
 * Create combiner, receive results from connections and validate combiner.
 * Works for Coordinator or Datanodes.
 */
static int
pgxc_node_receive_and_validate(const int conn_count, PGXCNodeHandle ** handles, bool reset_combiner)
{
	struct timeval *timeout = NULL;
	int result = 0;	
	RemoteQueryState *combiner = NULL;

	if (conn_count == 0)
		return result;

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	result = pgxc_node_receive_responses(conn_count, handles, timeout, combiner);

	if (result)
		goto finish;

	if (reset_combiner)
		result = ValidateAndResetCombiner(combiner) ? result : EOF;
	else
		result = ValidateAndCloseCombiner(combiner) ? result : EOF;

finish:
	return result;
}

/*
 * Get all connections for which we have an open transaction,
 * for both data nodes and coordinators
 */
static PGXCNodeAllHandles *
pgxc_get_all_transaction_nodes(PGXCNode_HandleRequested status_requested)
{
	PGXCNodeAllHandles *pgxc_connections;

	pgxc_connections = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
	if (!pgxc_connections)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	pgxc_connections->datanode_handles = (PGXCNodeHandle **)
				palloc(NumDataNodes * sizeof(PGXCNodeHandle *));
	pgxc_connections->coord_handles = (PGXCNodeHandle **)
				palloc(NumCoords * sizeof(PGXCNodeHandle *));
	if (!pgxc_connections->datanode_handles || !pgxc_connections->coord_handles)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* gather needed connections */
	pgxc_connections->dn_conn_count = get_transaction_nodes(
							pgxc_connections->datanode_handles,
							REMOTE_CONN_DATANODE,
							status_requested);
	pgxc_connections->co_conn_count = get_transaction_nodes(
							pgxc_connections->coord_handles,
							REMOTE_CONN_COORD,
							status_requested);

	return pgxc_connections;
}

void
ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle	  **connections;
	RemoteQueryState   *combiner;
	int					conn_count;
	int 				i;

	/* Exit if nodelist is empty */
	if (list_length(nodelist) == 0)
		return;

	/* get needed data node connections */
	all_handles = get_handles(nodelist, NIL, false);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
		{
			/*
			 * statements are not affected by statement end, so consider
			 * unclosed statement on the datanode as a fatal issue and
			 * force connection is discarded
			 */
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node statemrnt")));
		}
		if (pgxc_node_send_sync(connections[i]) != 0)
		{
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node statement")));
		}
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
		{
			for (i = 0; i <= conn_count; i++)
				connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node statement")));
		}
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			}
		}
	}

	ValidateAndCloseCombiner(combiner);
	pfree_pgxc_all_handles(all_handles);
}


/*
 * Check if an Implicit 2PC is necessary for this transaction.
 * Check also if it is necessary to prepare transaction locally.
 */
bool
PGXCNodeIsImplicit2PC(bool *prepare_local_coord)
{
	PGXCNodeAllHandles *pgxc_handles = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);
	int co_conn_count = pgxc_handles->co_conn_count;
	int total_count = pgxc_handles->co_conn_count + pgxc_handles->dn_conn_count;

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_handles);

	/*
	 * Prepare Local Coord only if DDL is involved.
	 * Even 1Co/1Dn cluster needs 2PC as more than 1 node is involved.
	 */
	*prepare_local_coord = is_ddl && total_count != 0;

	/*
	 * In case of an autocommit or forced autocommit transaction, 2PC is not involved
	 * This case happens for Utilities using force autocommit (CREATE DATABASE, VACUUM...).
	 * For a transaction using temporary objects, 2PC is not authorized.
	 */
	if (implicit_force_autocommit || temp_object_included)
	{
		*prepare_local_coord = false;
		implicit_force_autocommit = false;
		temp_object_included = false;
		return false;
	}

	/*
	 * 2PC is necessary at other Nodes if one Datanode or one Coordinator
	 * other than the local one has been involved in a write operation.
	 */
	return (write_node_count > 1 || co_conn_count > 0 || total_count > 0);
}

/*
 * Return the list of active nodes
 */
char *
PGXCNodeGetNodeList(char *nodestring)
{
	PGXCNodeAllHandles *pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_ERROR);

	if (pgxc_connections->dn_conn_count != 0)
		nodestring = collect_pgxcnode_names(nodestring,
											pgxc_connections->dn_conn_count,
											pgxc_connections->datanode_handles,
											REMOTE_CONN_DATANODE);

	if (pgxc_connections->co_conn_count != 0)
		nodestring = collect_pgxcnode_names(nodestring,
											pgxc_connections->co_conn_count,
											pgxc_connections->coord_handles,
											REMOTE_CONN_COORD);

	/* Case of a single Coordinator */
	if (is_ddl && pgxc_connections->co_conn_count == 0 && PGXCNodeId >= 0)
		nodestring = collect_localnode_name(nodestring);

	/*
	 * Now release handles properly, the list of handles in error state has been saved
	 * and will be sent to GTM.
	 */
	if (!PersistentConnections)
		release_handles();

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);

	return nodestring;
}

/*
 * DataNodeCopyInBinaryForAll
 *
 * In a COPY TO, send to all datanodes PG_HEADER for a COPY TO in binary mode.
 */
int DataNodeCopyInBinaryForAll(char *msg_buf, int len, PGXCNodeHandle** copy_connections)
{
	int 		i;
	int 		conn_count = 0;
	PGXCNodeHandle *connections[NumDataNodes];
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	for (i = 0; i < NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];

		if (!handle)
			continue;

		connections[conn_count++] = handle;
	}

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];
		if (handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* msgType + msgLen */
			if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, msg_buf, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(handle, "Invalid data node connection");
			return EOF;
		}
	}

	return 0;
}

/*
 * ExecSetTempObjectIncluded
 *
 * Set Temp object flag on the fly for transactions
 * This flag will be reinitialized at commit.
 */
void
ExecSetTempObjectIncluded(void)
{
	temp_object_included = true;
}
