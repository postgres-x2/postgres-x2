/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote data nodes
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
#include "access/gtm.h"
#include "access/xact.h"
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"

/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD 1024 * 1024

static bool autocommit = true;
static DataNodeHandle **write_node_list = NULL;
static int	write_node_count = 0;

static int	data_node_begin(int conn_count, DataNodeHandle ** connections,
				GlobalTransactionId gxid);
static int	data_node_commit(int conn_count, DataNodeHandle ** connections);
static int	data_node_rollback(int conn_count, DataNodeHandle ** connections);

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
	combiner->dest = NULL;
	combiner->command_complete_count = 0;
	combiner->row_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->tuple_desc = NULL;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->errorMessage = NULL;
	combiner->query_Done = false;
	combiner->completionTag = NULL;
	combiner->msg = NULL;
	combiner->msglen = 0;
	combiner->initAggregates = true;
	combiner->simple_aggregates = NULL;
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
 * Initialize the collection value, when agregation is first set up, or for a
 * new group (grouping support is not implemented yet)
 */
static void
initialize_collect_aggregates(SimpleAgg  *simple_agg)
{
	if (simple_agg->initValueIsNull)
		simple_agg->collectValue = simple_agg->initValue;
	else
		simple_agg->collectValue = datumCopy(simple_agg->initValue,
		                                     simple_agg->transtypeByVal,
		                                     simple_agg->transtypeLen);
	simple_agg->noCollectValue = simple_agg->initValueIsNull;
	simple_agg->collectValueNull = simple_agg->initValueIsNull;
}

/*
 * Finalize the aggregate after current group or entire relation is processed
 * (grouping support is not implemented yet)
 */
static void
finalize_collect_aggregates(SimpleAgg  *simple_agg, Datum *resultVal, bool *resultIsNull)
{
	/*
	 * Apply the agg's finalfn if one is provided, else return collectValue.
	 */
	if (OidIsValid(simple_agg->finalfn_oid))
	{
		FunctionCallInfoData fcinfo;

		InitFunctionCallInfoData(fcinfo, &(simple_agg->finalfn), 1,
		                         (void *) simple_agg, NULL);
		fcinfo.arg[0] = simple_agg->collectValue;
		fcinfo.argnull[0] = simple_agg->collectValueNull;
		if (fcinfo.flinfo->fn_strict && simple_agg->collectValueNull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			*resultVal = FunctionCallInvoke(&fcinfo);
			*resultIsNull = fcinfo.isnull;
		}
	}
	else
	{
		*resultVal = simple_agg->collectValue;
		*resultIsNull = simple_agg->collectValueNull;
	}
}

/*
 * Given new input value(s), advance the transition function of an aggregate.
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in fcinfo, so that we needn't copy them again to pass to the
 * collection function.  No other fields of fcinfo are assumed valid.
 *
 * It doesn't matter which memory context this is called in.
 */
static void
advance_collect_function(SimpleAgg  *simple_agg, FunctionCallInfoData *fcinfo)
{
	Datum		newVal;

	if (simple_agg->transfn.fn_strict)
	{
		/*
		 * For a strict transfn, nothing happens when there's a NULL input; we
		 * just keep the prior transValue.
		 */
		if (fcinfo->argnull[1])
			return;
		if (simple_agg->noCollectValue)
		{
			/*
			 * result has not been initialized
			 * We must copy the datum into result if it is pass-by-ref. We
			 * do not need to pfree the old result, since it's NULL.
			 */
			simple_agg->collectValue = datumCopy(fcinfo->arg[1],
			                                     simple_agg->transtypeByVal,
			                                     simple_agg->transtypeLen);
			simple_agg->collectValueNull = false;
			simple_agg->noCollectValue = false;
			return;
		}
		if (simple_agg->collectValueNull)
		{
			/*
			 * Don't call a strict function with NULL inputs.  Note it is
			 * possible to get here despite the above tests, if the transfn is
			 * strict *and* returned a NULL on a prior cycle. If that happens
			 * we will propagate the NULL all the way to the end.
			 */
			return;
		}
	}

	/*
	 * OK to call the transition function
	 */
	InitFunctionCallInfoData(*fcinfo, &(simple_agg->transfn), 2, (void *) simple_agg, NULL);
	fcinfo->arg[0] = simple_agg->collectValue;
	fcinfo->argnull[0] = simple_agg->collectValueNull;
	newVal = FunctionCallInvoke(fcinfo);

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * pfree the prior transValue.	But if transfn returned a pointer to its
	 * first input, we don't need to do anything.
	 */
	if (!simple_agg->transtypeByVal &&
	        DatumGetPointer(newVal) != DatumGetPointer(simple_agg->collectValue))
	{
		if (!fcinfo->isnull)
		{
			newVal = datumCopy(newVal,
			                   simple_agg->transtypeByVal,
			                   simple_agg->transtypeLen);
		}
		if (!simple_agg->collectValueNull)
			pfree(DatumGetPointer(simple_agg->collectValue));
	}

	simple_agg->collectValue = newVal;
	simple_agg->collectValueNull = fcinfo->isnull;
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
		char 	   *attname;
		Oid 		oidtypeid;
		int32 		typmod;

		uint32		n32;

		attnum = (AttrNumber) i;

		/* attribute name */
		attname = msg_body;
		msg_body += strlen(attname) + 1;

		/* table OID, ignored */
		msg_body += 4;

		/* column no, ignored */
		msg_body += 2;

		/* data type */
		memcpy(&n32, msg_body, 4);
		oidtypeid = ntohl(n32);
		msg_body += 4;

		/* type len, ignored */
		msg_body += 2;

		/* type mod */
		memcpy(&n32, msg_body, 4);
		typmod = ntohl(n32);
		msg_body += 4;

		/* PGXCTODO text/binary flag? */
		msg_body += 2;

		TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
	}
	return result;
}

static void
exec_simple_aggregates(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	ListCell   *lc;

	Assert(combiner->simple_aggregates);
	Assert(!TupIsNull(slot));

	if (combiner->initAggregates)
	{
		foreach (lc, combiner->simple_aggregates)
			initialize_collect_aggregates((SimpleAgg *) lfirst(lc));

		combiner->initAggregates = false;
	}

	foreach (lc, combiner->simple_aggregates)
	{
		SimpleAgg  *simple_agg = (SimpleAgg *) lfirst(lc);
		FunctionCallInfoData fcinfo;
		int attr = simple_agg->column_pos;

		slot_getsomeattrs(slot, attr + 1);
		fcinfo.arg[1] = slot->tts_values[attr];
		fcinfo.argnull[1] = slot->tts_isnull[attr];

		advance_collect_function(simple_agg, &fcinfo);
	}
}

static void
finish_simple_aggregates(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	ListCell   *lc;
	ExecClearTuple(slot);

	/*
	 * Aggregates may not been initialized if no rows has been received
	 * from the data nodes because of HAVING clause.
	 * In this case finish_simple_aggregates() should return empty slot
	 */
	if (!combiner->initAggregates)
	{
		foreach (lc, combiner->simple_aggregates)
		{
			SimpleAgg  *simple_agg = (SimpleAgg *) lfirst(lc);
			int attr = simple_agg->column_pos;

			finalize_collect_aggregates(simple_agg,
										slot->tts_values + attr,
										slot->tts_isnull + attr);
		}
		ExecStoreVirtualTuple(slot);
		/* To prevent aggregates get finalized again */
		combiner->initAggregates = true;
	}
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
	int 		digits = 0;

	/*
	 * If we did not receive description we are having rowcount or OK response
	 */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;
	/* Extract rowcount */
	if (combiner->combine_type != COMBINE_TYPE_NONE)
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
					if (rowcount != combiner->row_count)
						/* There is a consistency issue in the database with the replicated table */
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("Write to replicated table returned different results from the data nodes")));
				}
				else
					/* first result */
					combiner->row_count  = rowcount;
			}
			else
				combiner->row_count += rowcount;
		}
		else
			combiner->combine_type = COMBINE_TYPE_NONE;
	}
	if (++combiner->command_complete_count == combiner->node_count)
	{
		if (combiner->completionTag)
		{
			if (combiner->combine_type == COMBINE_TYPE_NONE)
			{
				/* ensure we do not go beyond buffer bounds */
				if (len > COMPLETION_TAG_BUFSIZE)
					len = COMPLETION_TAG_BUFSIZE;
				memcpy(combiner->completionTag, msg_body, len);
			}
			else
			{
				/* Truncate msg_body to get base string */
				msg_body[len - digits - 1] = '\0';
				snprintf(combiner->completionTag,
						 COMPLETION_TAG_BUFSIZE,
						 "%s" UINT64_FORMAT,
						 msg_body,
						 combiner->row_count);
			}
		}
	}
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
HandleDataRow(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	/* We expect previous message is consumed */
	Assert(combiner->msg == NULL);

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
	combiner->msg = (char *) palloc(len);
	memcpy(combiner->msg, msg_body, len);
	combiner->msglen = len;
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
		memcpy(combiner->errorCode, code, 5);
	}

	/*
	 * If data node have sent ErrorResponse it will never send CommandComplete.
	 * Increment the counter to prevent endless waiting for it.
	 */
	combiner->command_complete_count++;
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

	if (combiner->connections)
		pfree(combiner->connections);
	if (combiner->tuple_desc)
		FreeTupleDesc(combiner->tuple_desc);
	if (combiner->msg)
		pfree(combiner->msg);
	if (combiner->errorMessage)
		pfree(combiner->errorMessage);

	combiner->command_complete_count = 0;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->row_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->tuple_desc = NULL;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->errorMessage = NULL;
	combiner->query_Done = false;
	combiner->msg = NULL;
	combiner->msglen = 0;
	combiner->simple_aggregates = NULL;
	combiner->copy_file = NULL;

	return valid;
}

/*
 * Get next data row from the combiner's buffer into provided slot
 * Just clear slot and return false if buffer is empty, that means more data
 * should be read
 */
bool
FetchTuple(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	/* have messages in the buffer, consume them */
	if (combiner->msg)
	{
		ExecStoreDataRowTuple(combiner->msg, combiner->msglen, slot, true);
		combiner->msg = NULL;
		combiner->msglen = 0;
		return true;
	}
	/* inform caller that buffer is empty */
	ExecClearTuple(slot);
	return false;
}


/*
 * Handle responses from the Data node connections
 */
static int
data_node_receive_responses(const int conn_count, DataNodeHandle ** connections,
						 struct timeval * timeout, RemoteQueryState *combiner)
{
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
		int i = 0;

		if (data_node_receive(count, to_receive, timeout))
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
 * EOF - need to receive more data for the connection
 * 0 - done with the connection
 * 1 - got data row
 * 2 - got copy response
 */
int
handle_response(DataNodeHandle * conn, RemoteQueryState *combiner)
{
	char	   *msg;
	int			msg_len;

	for (;;)
	{
		/* No data available, exit */
		if (conn->state == DN_CONNECTION_STATE_QUERY)
			return RESPONSE_EOF;

		/* 
		 * If we are in the process of shutting down, we 
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
		{
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
			return RESPONSE_EOF;
		}

		/* TODO handle other possible responses */
		switch (get_message(conn, &msg_len, &msg))
		{
			case '\0':			/* Not enough data in the buffer */
				conn->state = DN_CONNECTION_STATE_QUERY;
				return RESPONSE_EOF;
			case 'c':			/* CopyToCommandComplete */
				conn->state = DN_CONNECTION_STATE_COMPLETED;
				HandleCopyOutComplete(combiner);
				break;
			case 'C':			/* CommandComplete */
				conn->state = DN_CONNECTION_STATE_COMPLETED;
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
				HandleDataRow(combiner, msg, msg_len);
				return RESPONSE_DATAROW;
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
				conn->state = DN_CONNECTION_STATE_ERROR_NOT_READY;
				/*
				 * Do not return with an error, we still need to consume Z,
				 * ready-for-query
				 */
				break;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif
				return RESPONSE_COMPLETE;
			case 'I':			/* EmptyQuery */
			default:
				/* sync lost? */
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				return RESPONSE_EOF;
		}
	}
	/* Keep compiler quiet */
	return RESPONSE_EOF;
}

/*
 * Send BEGIN command to the Data nodes and receive responses
 */
static int
data_node_begin(int conn_count, DataNodeHandle ** connections,
				GlobalTransactionId gxid)
{
	int			i;
	struct timeval *timeout = NULL;
	RemoteQueryState *combiner;

	/* Send BEGIN */
	for (i = 0; i < conn_count; i++)
	{
		if (GlobalTransactionIdIsValid(gxid) && data_node_send_gxid(connections[i], gxid))
			return EOF;

		if (data_node_send_query(connections[i], "BEGIN"))
			return EOF;
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);
	combiner->dest = None_Receiver;

	/* Receive responses */
	if (data_node_receive_responses(conn_count, connections, timeout, combiner))
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
 * Commit current transaction on data nodes where it has been started
 */
void
DataNodeCommit(void)
{
	int			res = 0;
	int			tran_count;
	DataNodeHandle *connections[NumDataNodes];

	/* gather connections to commit */
	tran_count = get_transaction_nodes(connections);

	/*
	 * If we do not have open transactions we have nothing to commit, just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	res = data_node_commit(tran_count, connections);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	clear_write_node_list();
	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit connection on data nodes")));
}


/*
 * Commit transaction on specified data node connections, use two-phase commit
 * if more then on one node data have been modified during the transactioon.
 */
static int
data_node_commit(int conn_count, DataNodeHandle ** connections)
{
	int			i;
	struct timeval *timeout = NULL;
	char		buffer[256];
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	int			result = 0;
	RemoteQueryState *combiner = NULL;


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
		gxid = GetCurrentGlobalTransactionId();

		sprintf(buffer, "PREPARE TRANSACTION 'T%d'", gxid);
		/* Send PREPARE */
		for (i = 0; i < conn_count; i++)
		{
			if (data_node_send_query(connections[i], buffer))
				return EOF;
		}

		combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);
		combiner->dest = None_Receiver;
		/* Receive responses */
		if (data_node_receive_responses(conn_count, connections, timeout, combiner))
			result = EOF;

		/* Reset combiner */
		if (!ValidateAndResetCombiner(combiner))
			result = EOF;
	}

	if (!do2PC)
		strcpy(buffer, "COMMIT");
	else
	{
		if (result)
			sprintf(buffer, "ROLLBACK PREPARED 'T%d'", gxid);
		else
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

	if (!combiner)
	{
		combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);
		combiner->dest = None_Receiver;
	}
	/* Receive responses */
	if (data_node_receive_responses(conn_count, connections, timeout, combiner))
		result = EOF;
	result = ValidateAndCloseCombiner(combiner) ? result : EOF;

finish:
	if (do2PC)
		CommitTranGTM((GlobalTransactionId) two_phase_xid);

	return result;
}


/*
 * Rollback current transaction
 */
int
DataNodeRollback(void)
{
	int			res = 0;
	int			tran_count;
	DataNodeHandle *connections[NumDataNodes];

	/* gather connections to rollback */
	tran_count = get_transaction_nodes(connections);

	/*
	 * If we do not have open transactions we have nothing to rollback just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	res = data_node_rollback(tran_count, connections);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	clear_write_node_list();
	return res;
}


/*
 * Send ROLLBACK command down to the Data nodes and handle responses
 */
static int
data_node_rollback(int conn_count, DataNodeHandle ** connections)
{
	int			i;
	struct timeval *timeout = NULL;
	RemoteQueryState *combiner;

	/* Send ROLLBACK - */
	for (i = 0; i < conn_count; i++)
	{
		data_node_send_query(connections[i], "ROLLBACK");
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);
	combiner->dest = None_Receiver;
	/* Receive responses */
	if (data_node_receive_responses(conn_count, connections, timeout, combiner))
		return EOF;

	/* Verify status */
	return ValidateAndCloseCombiner(combiner) ? 0 : EOF;
}


/*
 * Begin COPY command
 * The copy_connections array must have room for NumDataNodes items
 */
DataNodeHandle**
DataNodeCopyBegin(const char *query, List *nodelist, Snapshot snapshot, bool is_from)
{
	int i, j;
	int conn_count = list_length(nodelist) == 0 ? NumDataNodes : list_length(nodelist);
	struct timeval *timeout = NULL;
	DataNodeHandle **connections;
	DataNodeHandle **copy_connections;
	DataNodeHandle *newConnections[conn_count];
	int new_count = 0;
	ListCell *nodeitem;
	bool need_tran;
	GlobalTransactionId gxid;
	RemoteQueryState *combiner;

	if (conn_count == 0)
		return NULL;

	/* Get needed datanode connections */
	connections = get_handles(nodelist);
	if (!connections)
		return NULL;

	need_tran = !autocommit || conn_count > 1;

	elog(DEBUG1, "autocommit = %s, conn_count = %d, need_tran = %s", autocommit ? "true" : "false", conn_count, need_tran ? "true" : "false");

	/*
	 * We need to be able quickly find a connection handle for specified node number,
	 * So store connections in an array where index is node-1.
	 * Unused items in the array should be NULL
	 */
	copy_connections = (DataNodeHandle **) palloc0(NumDataNodes * sizeof(DataNodeHandle *));
	i = 0;
	foreach(nodeitem, nodelist)
		copy_connections[lfirst_int(nodeitem) - 1] = connections[i++];

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
		pfree(connections);
		pfree(copy_connections);
		return NULL;
	}
	if (new_count > 0 && need_tran)
	{
		/* Start transaction on connections where it is not started */
		if (data_node_begin(new_count, newConnections, gxid))
		{
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
	}

	/* Send query to nodes */
	for (i = 0; i < conn_count; i++)
	{
		/* If explicit transaction is needed gxid is already sent */
		if (!need_tran && data_node_send_gxid(connections[i], gxid))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
		if (snapshot && data_node_send_snapshot(connections[i], snapshot))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
		if (data_node_send_query(connections[i], query) != 0)
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
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
	combiner->dest = None_Receiver;

	/* Receive responses */
	if (data_node_receive_responses(conn_count, connections, timeout, combiner)
			|| !ValidateAndCloseCombiner(combiner))
	{
		if (autocommit)
		{
			if (need_tran)
				DataNodeCopyFinish(connections, 0, COMBINE_TYPE_NONE);
			else
				if (!PersistentConnections) release_handles();
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
DataNodeCopyIn(char *data_row, int len, Exec_Nodes *exec_nodes, DataNodeHandle** copy_connections)
{
	DataNodeHandle *primary_handle = NULL;
	ListCell *nodeitem;
	/* size + data row + \n */
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	if (exec_nodes->primarynodelist)
	{
		primary_handle = copy_connections[lfirst_int(list_head(exec_nodes->primarynodelist)) - 1];
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
				int read_status = data_node_read_data(primary_handle);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(primary_handle, "failed to read data from data node");
					return EOF;
				}

				if (primary_handle->inStart < primary_handle->inEnd)
				{
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					combiner->dest = None_Receiver;
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

	foreach(nodeitem, exec_nodes->nodelist)
	{
		DataNodeHandle *handle = copy_connections[lfirst_int(nodeitem) - 1];
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
				int read_status = data_node_read_data(handle);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(handle, "failed to read data from data node");
					return EOF;
				}

				if (handle->inStart < handle->inEnd)
				{
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					combiner->dest = None_Receiver;
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
DataNodeCopyOut(Exec_Nodes *exec_nodes, DataNodeHandle** copy_connections, FILE* copy_file)
{
	RemoteQueryState *combiner;
	int 		conn_count = list_length(exec_nodes->nodelist) == 0 ? NumDataNodes : list_length(exec_nodes->nodelist);
	int 		count = 0;
	bool 		need_tran;
	List 	   *nodelist;
	ListCell   *nodeitem;
	uint64		processed = 0;

	nodelist = exec_nodes->nodelist;
	need_tran = !autocommit || conn_count > 1;

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_SUM);
	combiner->dest = None_Receiver;
	/* If there is an existing file where to copy data, pass it to combiner */
	if (copy_file)
		combiner->copy_file = copy_file;

	foreach(nodeitem, exec_nodes->nodelist)
	{
		DataNodeHandle *handle = copy_connections[count];
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
					read_status = data_node_read_data(handle);
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

	processed = combiner->row_count;

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
uint64
DataNodeCopyFinish(DataNodeHandle** copy_connections, int primary_data_node,
		CombineType combine_type)
{
	int 		i;
	int 		nLen = htonl(4);
	RemoteQueryState *combiner = NULL;
	bool 		need_tran;
	bool 		error = false;
	struct timeval *timeout = NULL; /* wait forever */
	DataNodeHandle *connections[NumDataNodes];
	DataNodeHandle *primary_handle = NULL;
	int 		conn_count = 0;
	uint64		processed;

	for (i = 0; i < NumDataNodes; i++)
	{
		DataNodeHandle *handle = copy_connections[i];

		if (!handle)
			continue;

		if (i == primary_data_node - 1)
			primary_handle = handle;
		else
			connections[conn_count++] = handle;
	}

	if (primary_handle)
	{
		if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN || primary_handle->state == DN_CONNECTION_STATE_COPY_OUT)
		{
			/* msgType + msgLen */
			if (ensure_out_buffer_capacity(primary_handle->outEnd + 1 + 4, primary_handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			primary_handle->outBuffer[primary_handle->outEnd++] = 'c';
			memcpy(primary_handle->outBuffer + primary_handle->outEnd, &nLen, 4);
			primary_handle->outEnd += 4;

			/* We need response right away, so send immediately */
			if (data_node_flush(primary_handle) < 0)
			{
				error = true;
			}
		}
		else
		{
			error = true;
		}

		combiner = CreateResponseCombiner(conn_count + 1, combine_type);
		combiner->dest = None_Receiver;
		error = data_node_receive_responses(1, &primary_handle, timeout, combiner) || error;
	}

	for (i = 0; i < conn_count; i++)
	{
		DataNodeHandle *handle = connections[i];

		if (handle->state == DN_CONNECTION_STATE_COPY_IN || handle->state == DN_CONNECTION_STATE_COPY_OUT)
		{
			/* msgType + msgLen */
			if (ensure_out_buffer_capacity(handle->outEnd + 1 + 4, handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'c';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;

			/* We need response right away, so send immediately */
			if (data_node_flush(handle) < 0)
			{
				error = true;
			}
		}
		else
		{
			error = true;
		}
	}

	need_tran = !autocommit || primary_handle || conn_count > 1;

	if (!combiner)
	{
		combiner = CreateResponseCombiner(conn_count, combine_type);
		combiner->dest = None_Receiver;
	}
	error = (data_node_receive_responses(conn_count, connections, timeout, combiner) != 0) || error;

	processed = combiner->row_count;

	if (!ValidateAndCloseCombiner(combiner) || error)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Error while running COPY")));

	return processed;
}

RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState *remotestate;

	remotestate = CreateResponseCombiner(0, node->combine_type);
	remotestate->ss.ps.plan = (Plan *) node;
	remotestate->ss.ps.state = estate;
	remotestate->simple_aggregates = node->simple_aggregates;

	ExecInitResultTupleSlot(estate, &remotestate->ss.ps);
	if (node->plan.targetlist)
	{
		TupleDesc typeInfo = ExecCleanTypeFromTL(node->plan.targetlist, false);
		ExecSetSlotDescriptor(remotestate->ss.ps.ps_ResultTupleSlot, typeInfo);
	}

	ExecInitScanTupleSlot(estate, &remotestate->ss);
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
			ExecStoreDataRowTuple(src->tts_dataRow, src->tts_dataLen, dst, src->tts_shouldFreeRow);
			src->tts_shouldFreeRow = false;
		}
		else
		{
			/* have to make a copy */
			MemoryContext	oldcontext = MemoryContextSwitchTo(dst->tts_mcxt);
			int 			len = src->tts_dataLen;
			char		   *msg = (char *) palloc(len);

			memcpy(msg, src->tts_dataRow, len);
			ExecStoreDataRowTuple(msg, len, dst, true);
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
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the data nodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
TupleTableSlot *
ExecRemoteQuery(RemoteQueryState *node)
{
	RemoteQuery    *step = (RemoteQuery *) node->ss.ps.plan;
	EState		   *estate = node->ss.ps.state;
	TupleTableSlot *resultslot = node->ss.ps.ps_ResultTupleSlot;
	TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;
	bool have_tuple = false;


	if (!node->query_Done)
	{
		/* First invocation, initialize */
		Exec_Nodes *exec_nodes = step->exec_nodes;
		bool		force_autocommit = step->force_autocommit;
		bool		is_read_only = step->read_only;
		GlobalTransactionId gxid = InvalidGlobalTransactionId;
		Snapshot snapshot = GetActiveSnapshot();
		DataNodeHandle **connections = NULL;
		DataNodeHandle **primaryconnection = NULL;
		List 	   *nodelist = NIL;
		List 	   *primarynode = NIL;
		int			i;
		int			j;
		int			regular_conn_count;
		int			total_conn_count;
		bool		need_tran;

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
		node->node_count = total_conn_count;

		/* Get connection for primary node, if used */
		if (primarynode)
		{
			primaryconnection = get_handles(primarynode);
			if (!primaryconnection)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Could not obtain connection from pool")));
			total_conn_count++;
		}

		/* Get other connections (non-primary) */
		connections = get_handles(nodelist);
		if (!connections)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not obtain connection from pool")));

		if (force_autocommit)
			need_tran = false;
		else
			need_tran = !autocommit || total_conn_count > 1;

		elog(DEBUG1, "autocommit = %s, has primary = %s, regular_conn_count = %d, statement_need_tran = %s", autocommit ? "true" : "false", primarynode ? "true" : "false", regular_conn_count, need_tran ? "true" : "false");

		stat_statement();
		if (autocommit)
		{
			stat_transaction(total_conn_count);
			/* We normally clear for transactions, but if autocommit, clear here, too */
			clear_write_node_list();
		}

		/* Check status of connections */
		/*
		 * We would want to run 2PC if current transaction modified more then
		 * one node. So optimize little bit and do not look further if we
		 * already have two.
		 */
		if (!is_read_only && write_node_count < 2)
		{
			bool found;

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
				}
			}
			for (i = 0; i < regular_conn_count && write_node_count < 2; i++)
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
				}
			}
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
			DataNodeHandle *new_connections[total_conn_count];
			int 		new_count = 0;

			if (primaryconnection && primaryconnection[0]->transaction_status != 'T')
				new_connections[new_count++] = primaryconnection[0];
			for (i = 0; i < regular_conn_count; i++)
				if (connections[i]->transaction_status != 'T')
					new_connections[new_count++] = connections[i];

			if (new_count)
				data_node_begin(new_count, new_connections, gxid);
		}

		/* See if we have a primary nodes, execute on it first before the others */
		if (primaryconnection)
		{
			/* If explicit transaction is needed gxid is already sent */
			if (!need_tran && data_node_send_gxid(primaryconnection[0], gxid))
			{
				pfree(connections);
				pfree(primaryconnection);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (snapshot && data_node_send_snapshot(primaryconnection[0], snapshot))
			{
				pfree(connections);
				pfree(primaryconnection);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (data_node_send_query(primaryconnection[0], step->sql_statement) != 0)
			{
				pfree(connections);
				pfree(primaryconnection);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}

			Assert(node->combine_type == COMBINE_TYPE_SAME);

			while (node->command_complete_count < 1)
			{
				if (data_node_receive(1, primaryconnection, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to read response from data nodes")));
				while (handle_response(primaryconnection[0], node) == RESPONSE_EOF)
					if (data_node_receive(1, primaryconnection, NULL))
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Failed to read response from data nodes")));
				if (node->errorMessage)
				{
					char *code = node->errorCode;
					ereport(ERROR,
							(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
							 errmsg("%s", node->errorMessage)));
				}
			}
			pfree(primaryconnection);
		}

		for (i = 0; i < regular_conn_count; i++)
		{
			/* If explicit transaction is needed gxid is already sent */
			if (!need_tran && data_node_send_gxid(connections[i], gxid))
			{
				pfree(connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (snapshot && data_node_send_snapshot(connections[i], snapshot))
			{
				pfree(connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (data_node_send_query(connections[i], step->sql_statement) != 0)
			{
				pfree(connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
		}

			/*
			 * Stop if all commands are completed or we got a data row and
			 * initialized state node for subsequent invocations
			 */
			while (regular_conn_count > 0 && node->connections == NULL)
			{
				int i = 0;

				if (data_node_receive(regular_conn_count, connections, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to read response from data nodes")));
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
						 * we should send to client not the tuple_desc we just
						 * received, but tuple_desc from the planner.
						 * Data node may be sending junk columns for sorting
						 */
						(*node->dest->rStartup) (node->dest, CMD_SELECT,
												 resultslot->tts_tupleDescriptor);
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
												   node->tuple_desc,
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

		node->query_Done = true;
	}

	if (node->tuplesortstate)
	{
		while (tuplesort_gettupleslot((Tuplesortstate *) node->tuplesortstate,
									  true, scanslot))
		{
			have_tuple = true;
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
			(*node->dest->receiveSlot) (resultslot, node->dest);
			break;
		}
		if (!have_tuple)
			ExecClearTuple(resultslot);
	}
	else
	{
		while (node->conn_count > 0 && !have_tuple)
		{
			int i;

			/*
			 * If combiner already has tuple go ahead and return it
			 * otherwise tuple will be cleared
			 */
			if (FetchTuple(node, scanslot) && !TupIsNull(scanslot))
			{
				if (node->simple_aggregates)
				{
					/*
					 * Advance aggregate functions and allow to read up next
					 * data row message and get tuple in the same slot on
					 * next iteration
					 */
					exec_simple_aggregates(node, scanslot);
				}
				else
				{
					/*
					 * Receive current slot and read up next data row
					 * message before exiting the loop. Next time when this
					 * function is invoked we will have either data row
					 * message ready or EOF
					 */
					copy_slot(node, scanslot, resultslot);
					(*node->dest->receiveSlot) (resultslot, node->dest);
					have_tuple = true;
				}
			}

			/*
			 * Handle input to get next row or ensure command is completed,
			 * starting from connection next after current. If connection
			 * does not
			 */
			if ((i = node->current_conn + 1) == node->conn_count)
				i = 0;

			for (;;)
			{
				int res = handle_response(node->connections[i], node);
				if (res == RESPONSE_EOF)
				{
					/* go to next connection */
					if (++i == node->conn_count)
						i = 0;
					/* if we cycled over all connections we need to receive more */
					if (i == node->current_conn)
						if (data_node_receive(node->conn_count, node->connections, NULL))
							ereport(ERROR,
									(errcode(ERRCODE_INTERNAL_ERROR),
									 errmsg("Failed to read response from data nodes")));
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (--node->conn_count == 0)
						break;
					if (i == node->conn_count)
						i = 0;
					else
						node->connections[i] = node->connections[node->conn_count];
					if (node->current_conn == node->conn_count)
						node->current_conn = i;
				}
				else if (res == RESPONSE_DATAROW)
				{
					node->current_conn = i;
					break;
				}
			}
		}

		/*
		 * We may need to finalize aggregates
		 */
		if (!have_tuple && node->simple_aggregates)
		{
			finish_simple_aggregates(node, resultslot);
			if (!TupIsNull(resultslot))
			{
				(*node->dest->receiveSlot) (resultslot, node->dest);
				have_tuple = true;
			}
		}

		if (!have_tuple) /* report end of scan */
			ExecClearTuple(resultslot);

	}

	if (node->errorMessage)
	{
		char *code = node->errorCode;
		ereport(ERROR,
				(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
				 errmsg("%s", node->errorMessage)));
	}

	return resultslot;
}

void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	(*node->dest->rShutdown) (node->dest);
	if (node->tmp_ctx)
		MemoryContextDelete(node->tmp_ctx);
	CloseCombiner(node);
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
		DataNodeRollback();

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

