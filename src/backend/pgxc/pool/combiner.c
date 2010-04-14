/*-------------------------------------------------------------------------
 *
 * combiner.c
 *
 *	  Combine responses from multiple Data Nodes
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgxc/combiner.h"
#include "pgxc/planner.h"
#include "catalog/pg_type.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"


/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
ResponseCombiner
CreateResponseCombiner(int node_count, CombineType combine_type,
					   CommandDest dest)
{
	ResponseCombiner combiner;

	/* ResponseComber is a typedef for pointer to ResponseCombinerData */
	combiner = (ResponseCombiner) palloc(sizeof(ResponseCombinerData));
	if (combiner == NULL)
	{
		/* Out of memory */
		return combiner;
	}

	combiner->node_count = node_count;
	combiner->combine_type = combine_type;
	combiner->dest = dest;
	combiner->command_complete_count = 0;
	combiner->row_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->description_count = 0;
	combiner->simple_aggregates = NULL;

	return combiner;
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, int *rowcount)
{
	int			digits = 0;

	*rowcount = 0;
	/* skip \0 string terminator */
	len--;
	while (len-- > 0 && message[len] >= '0' && message[len] <= '9')
	{
		*rowcount = *rowcount * 10 + message[len] - '0';
		digits++;
	}
	return digits;
}

/*
 * Extract the aggregate element result
 * returns a boolean indicating whether or not it was a short message
 */
static unsigned long
parse_aggregate_value(SimpleAgg * simple_agg, char *msg_body, size_t len)
{
	char	   *valstr;

	Assert(len >= 7);

	/* PGXCTODO - handle pos (position) */
	/* PGXCTODO - handle other types like TEXT */

	/* skip first 2 bytes */
	if (simple_agg->data_len == 0)
		memcpy(&(simple_agg->data_len), &(msg_body[2]), 4);

	valstr = (char *) palloc(simple_agg->data_len + 1);
	strncpy(valstr, &(msg_body[6]), simple_agg->data_len);
	valstr[simple_agg->data_len - 1] = '\0';

	return atol(valstr);
}


/*
 * Process a result from a node for the aggregate function
 * returns a boolean indicating whether or not it was a short message
 */
static void
process_aggregate_element(List *simple_aggregates, char *msg_body, size_t len)
{
	ListCell   *lc;

	foreach(lc, simple_aggregates)
	{
		unsigned long col_value;
		SimpleAgg  *simple_agg = (SimpleAgg *) lfirst(lc);

		/* PGXCTODO may need to support numeric, too. */
		col_value = parse_aggregate_value(simple_agg, msg_body, len);

		switch (simple_agg->agg_type)
		{
			case AGG_TYPE_MAX:
				/* If it is the first one, take it */
				if (simple_agg->response_count == 0)
				{
					/* PGXCTODO - type checking */
					simple_agg->ulong_value = col_value;
				}
				else
				{
					if (col_value > simple_agg->ulong_value)
						simple_agg->ulong_value = col_value;
				}
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Unknown aggregate type: %d",
								simple_agg->agg_type)));
		}

	}
}


/*
 * Handle response message and update combiner's state.
 * This function contains main combiner logic
 */
int
CombineResponse(ResponseCombiner combiner, char msg_type, char *msg_body, size_t len)
{
	int			rowcount;
	int			digits = 0;

	switch (msg_type)
	{
		case 'C':				/* CommandComplete */
			/*
			 * If we did not receive description we are having rowcount or OK
			 * response
			 */
			if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
				combiner->request_type = REQUEST_TYPE_COMMAND;
			/* Extract rowcount */
			if (combiner->combine_type != COMBINE_TYPE_NONE)
			{
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
									 errmsg("Write to replicated table returned different results from the data nodes"
											)));
						}
						else
							/* first result */
							combiner->row_count  = rowcount;
					} else 
						combiner->row_count += rowcount;
				}
				else
					combiner->combine_type = COMBINE_TYPE_NONE;
			}
			if (++combiner->command_complete_count == combiner->node_count)
			{

				if (combiner->dest == DestRemote
					|| combiner->dest == DestRemoteExecute)
				{
					if (combiner->combine_type == COMBINE_TYPE_NONE)
					{
						pq_putmessage(msg_type, msg_body, len);
					}
					else
					{
						char		command_complete_buffer[256];

						rowcount = combiner->combine_type == COMBINE_TYPE_SUM ?
							combiner->row_count :
							combiner->row_count / combiner->node_count;
						/* Truncate msg_body to get base string */
						msg_body[len - digits - 1] = '\0';
						len = sprintf(command_complete_buffer, "%s%d", msg_body, rowcount) + 1;
						pq_putmessage(msg_type, command_complete_buffer, len);
					}
				}
			}
			break;
		case 'T':				/* RowDescription */
			if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
				combiner->request_type = REQUEST_TYPE_QUERY;
			if (combiner->request_type != REQUEST_TYPE_QUERY)
			{
				/* Inconsistent responses */
				return EOF;
			}
			/* Proxy first */
			if (combiner->description_count++ == 0)
			{
				if (combiner->dest == DestRemote
					|| combiner->dest == DestRemoteExecute)
					pq_putmessage(msg_type, msg_body, len);
			}
			break;
		case 'G':				/* CopyInResponse */
			if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
				combiner->request_type = REQUEST_TYPE_COPY_IN;
			if (combiner->request_type != REQUEST_TYPE_COPY_IN)
			{
				/* Inconsistent responses */
				return EOF;
			}
			/* Proxy first */
			if (combiner->description_count++ == 0)
			{
				if (combiner->dest == DestRemote
					|| combiner->dest == DestRemoteExecute)
					pq_putmessage(msg_type, msg_body, len);
			}
			break;
		case 'H':				/* CopyOutResponse */
			if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
				combiner->request_type = REQUEST_TYPE_COPY_OUT;
			if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
			{
				/* Inconsistent responses */
				return EOF;
			}
			/* Proxy first */
			if (combiner->description_count++ == 0)
			{
				if (combiner->dest == DestRemote
					|| combiner->dest == DestRemoteExecute)
					pq_putmessage(msg_type, msg_body, len);
			}
			break;
		case 'D':				/* DataRow */
			if (combiner->simple_aggregates == NULL)
			{
				if (combiner->dest == DestRemote
					|| combiner->dest == DestRemoteExecute)
					pq_putmessage(msg_type, msg_body, len);
			}
			else
			{
				SimpleAgg  *simple_agg = (SimpleAgg *) linitial(combiner->simple_aggregates);

				/* Handle aggregates */
				/* Process single node result */
				process_aggregate_element(
										  combiner->simple_aggregates,
										  msg_body, len);

				/*
				 * See if we are done with all nodes. Only then do we send one
				 * DataRow result.
				 */

				if (++simple_agg->response_count
					== combiner->node_count)
				{
					char		longstr[21];
					int			longlen;

					StringInfo	data_buffer;

					data_buffer = makeStringInfo();

					/*
					 * longlen = sprintf(longstr, "%lu",
					 * simple_agg->ulong_value);
					 */

					pg_ltoa(simple_agg->ulong_value, longstr);
					longlen = strlen(longstr);

					pq_beginmessage(data_buffer, 'D');
					pq_sendbyte(data_buffer, msg_body[0]);
					pq_sendbyte(data_buffer, msg_body[1]);
					pq_sendint(data_buffer, longlen, 4);
					pq_sendtext(data_buffer, longstr, longlen);
					pq_putmessage(msg_type,
								  data_buffer->data,
								  data_buffer->len);

					pfree(data_buffer->data);
					pfree(data_buffer);
				}
			}
			break;
		case 'E':				/* ErrorResponse */
		case 'A':				/* NotificationResponse */
		case 'N':				/* NoticeResponse */
			/* Proxy error message back if specified, 
			 * or if doing internal primary copy 
			 */
			if (combiner->dest == DestRemote
				|| combiner->dest == DestRemoteExecute
				|| combiner->combine_type == COMBINE_TYPE_SAME)
				pq_putmessage(msg_type, msg_body, len);
			break;
		case 'I':				/* EmptyQuery */
		default:
			/* Unexpected message */
			return EOF;
	}
	return 0;
}

/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
static bool
validate_combiner(ResponseCombiner combiner)
{
	/* Check all nodes completed */
	if (combiner->command_complete_count != combiner->node_count)
		return false;

	/* Check count of description responses */
	if (combiner->request_type != REQUEST_TYPE_COMMAND
		&& combiner->description_count != combiner->node_count)
		return false;

	/* Add other checks here as needed */

	/* All is good if we are here */
	return true;
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
bool
ValidateAndCloseCombiner(ResponseCombiner combiner)
{
	bool		valid = validate_combiner(combiner);

	pfree(combiner);

	return valid;
}

/*
 * Validate combiner and reset storage
 */
bool
ValidateAndResetCombiner(ResponseCombiner combiner)
{
	bool		valid = validate_combiner(combiner);

	combiner->command_complete_count = 0;
	combiner->row_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->description_count = 0;
	combiner->simple_aggregates = NULL;

	return valid;
}

/*
 * Assign combiner aggregates
 */
void
AssignCombinerAggregates(ResponseCombiner combiner, List *simple_aggregates)
{
	combiner->simple_aggregates = simple_aggregates;
}
