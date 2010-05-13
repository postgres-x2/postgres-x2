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
#include "utils/datum.h"


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
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->inErrorState = false;
	combiner->initAggregates = true;
	combiner->simple_aggregates = NULL;
	combiner->copy_file = NULL;

	return combiner;
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, int *rowcount)
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
 * Extract a transition value from data row. Invoke the Input Function
 * associated with the transition data type to represent value as a Datum.
 * Output parameters value and val_null, receive extracted value and indicate
 * whether it is null.
 */
static void
parse_aggregate_value(SimpleAgg *simple_agg, char *col_data, size_t datalen, Datum *value, bool *val_null)
{
	/* Check NULL */
	if (datalen == -1)
	{
		*value = (Datum) 0;
		*val_null = true;
	}
	else
	{
		resetStringInfo(&simple_agg->valuebuf);
		appendBinaryStringInfo(&simple_agg->valuebuf, col_data, datalen);
		*value = InputFunctionCall(&simple_agg->arginputfn, simple_agg->valuebuf.data, simple_agg->argioparam, -1);
		*val_null = false;
	}
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
 * Handle response message and update combiner's state.
 * This function contains main combiner logic
 */
int
CombineResponse(ResponseCombiner combiner, char msg_type, char *msg_body, size_t len)
{
	int digits = 0;

	/* Ignore anything if we have encountered error */
	if (combiner->inErrorState)
		return EOF;

	switch (msg_type)
	{
		case 'c':				/* CopyOutCommandComplete */
			if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
				combiner->request_type = REQUEST_TYPE_COPY_OUT;
			if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
				/* Inconsistent responses */
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("Unexpected response from the data nodes")));
			/* Just do nothing, close message is managed by the coordinator */
			combiner->copy_out_count++;
			break;
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
				int	rowcount;
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

				if (combiner->simple_aggregates
						/*
						 * Aggregates has not been initialized - that means
						 * no rows received from data nodes, nothing to send
						 * It is possible if HAVING clause is present
						 */
						&& !combiner->initAggregates)
				{
					/* Build up and send a datarow with aggregates */
					StringInfo	dataRowBuffer = makeStringInfo();
					ListCell   *lc;

					/* Number of fields */
					pq_sendint(dataRowBuffer, list_length(combiner->simple_aggregates), 2);

					foreach (lc, combiner->simple_aggregates)
					{
						SimpleAgg  *simple_agg = (SimpleAgg *) lfirst(lc);
						Datum 		resultVal;
						bool		resultIsNull;

						finalize_collect_aggregates(simple_agg, &resultVal, &resultIsNull);
						/* Aggregation result */
						if (resultIsNull)
						{
							pq_sendint(dataRowBuffer, -1, 4);
						}
						else
						{
							char   *text = OutputFunctionCall(&simple_agg->resoutputfn, resultVal);
							size_t 	len = strlen(text);
							pq_sendint(dataRowBuffer, len, 4);
							pq_sendtext(dataRowBuffer, text, len);
						}
					}
					pq_putmessage('D', dataRowBuffer->data, dataRowBuffer->len);
					pfree(dataRowBuffer->data);
					pfree(dataRowBuffer);
				}
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

						/* Truncate msg_body to get base string */
						msg_body[len - digits - 1] = '\0';
						len = sprintf(command_complete_buffer, "%s%d", msg_body, combiner->row_count) + 1;
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
				ereport(ERROR,
				        (errcode(ERRCODE_DATA_CORRUPTED),
				         errmsg("Unexpected response from the data nodes")));
			}
			/* Proxy first */
			if (combiner->description_count++ == 0)
			{
				if (combiner->dest == DestRemote
				        || combiner->dest == DestRemoteExecute)
					pq_putmessage(msg_type, msg_body, len);
			}
			break;
		case 'S':				/* ParameterStatus (SET command) */
			if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
				combiner->request_type = REQUEST_TYPE_QUERY;
			if (combiner->request_type != REQUEST_TYPE_QUERY)
			{
				/* Inconsistent responses */
				ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Unexpected response from the data nodes")));
			}
			/* Proxy last */
			if (++combiner->description_count == combiner->node_count)
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
				ereport(ERROR,
				        (errcode(ERRCODE_DATA_CORRUPTED),
				         errmsg("Unexpected response from the data nodes")));
			}
			/* Proxy first */
			if (combiner->copy_in_count++ == 0)
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
				ereport(ERROR,
				        (errcode(ERRCODE_DATA_CORRUPTED),
				         errmsg("Unexpected response from the data nodes")));
			}
			/*
			 * The normal PG code will output an H message when it runs in the
			 * coordinator, so do not proxy message here, just count it.
			 */
			combiner->copy_out_count++;
			break;
		case 'd':			/* CopyOutDataRow */
			if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
				combiner->request_type = REQUEST_TYPE_COPY_OUT;

			/* Inconsistent responses */
			if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("Unexpected response from the data nodes")));

			/* If there is a copy file, data has to be sent to the local file */
			if (combiner->copy_file)
			{
				/* write data to the copy file */
				char *data_row;
				data_row = (char *) palloc0(len);
				memcpy(data_row, msg_body, len);

				fwrite(data_row, 1, len, combiner->copy_file);
				break;
			}
			/*
			 * In this case data is sent back to the client
			 */
			if (combiner->dest == DestRemote
				|| combiner->dest == DestRemoteExecute)
			{
				StringInfo  data_buffer;

				data_buffer = makeStringInfo();

				pq_sendtext(data_buffer, msg_body, len);
				pq_putmessage(msg_type,
							  data_buffer->data,
							  data_buffer->len);

				pfree(data_buffer->data);
				pfree(data_buffer);
			}
			break;
		case 'D':				/* DataRow */
			if (!combiner->simple_aggregates)
			{
				if (combiner->dest == DestRemote
				        || combiner->dest == DestRemoteExecute)
					pq_putmessage(msg_type, msg_body, len);
			}
			else
			{
				ListCell   *lc;
				char	  **col_values;
				int		   *col_value_len;
				uint16		col_count;
				int			i, cur = 0;

				/* Get values from the data row into array to speed up access */
				memcpy(&col_count, msg_body, 2);
				col_count = ntohs(col_count);
				cur += 2;

				col_values = (char **) palloc0(col_count * sizeof(char *));
				col_value_len = (int *) palloc0(col_count * sizeof(int));
				for (i = 0; i < col_count; i++)
				{
					int n32;

					memcpy(&n32, msg_body + cur, 4);
					col_value_len[i] = ntohl(n32);
					cur += 4;

					if (col_value_len[i] != -1)
					{
						col_values[i] = msg_body + cur;
						cur += col_value_len[i];
					}
				}

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

					parse_aggregate_value(simple_agg,
					                      col_values[simple_agg->column_pos],
					                      col_value_len[simple_agg->column_pos],
					                      fcinfo.arg + 1,
					                      fcinfo.argnull + 1);

					advance_collect_function(simple_agg, &fcinfo);
				}
				pfree(col_values);
				pfree(col_value_len);
			}
			break;
		case 'E':				/* ErrorResponse */
			combiner->inErrorState = true;
			/* fallthru */
		case 'A':				/* NotificationResponse */
		case 'N':				/* NoticeResponse */
			/* Proxy error message back if specified,
			 * or if doing internal primary copy
			 */
			if (combiner->dest == DestRemote
			        || combiner->dest == DestRemoteExecute)
				pq_putmessage(msg_type, msg_body, len);
			break;
		case 'I':				/* EmptyQuery */
		default:
			/* Unexpected message */
			ereport(ERROR,
			        (errcode(ERRCODE_DATA_CORRUPTED),
			         errmsg("Unexpected response from the data nodes")));
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
	/* There was error message while combining */
	if (combiner->inErrorState)
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
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->inErrorState = false;
	combiner->simple_aggregates = NULL;
	combiner->copy_file = NULL;

	return valid;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
void
CloseCombiner(ResponseCombiner combiner)
{
	if (combiner)
		pfree(combiner);
}

/*
 * Assign combiner aggregates
 */
void
AssignCombinerAggregates(ResponseCombiner combiner, List *simple_aggregates)
{
	combiner->simple_aggregates = simple_aggregates;
}
