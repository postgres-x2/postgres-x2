/*-------------------------------------------------------------------------
 *
 * combiner.h
 *
 *	  Combine responses from multiple Data Nodes
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

#ifndef COMBINER_H
#define COMBINER_H

#include "postgres.h"
#include "tcop/dest.h"

typedef enum
{
	COMBINE_TYPE_NONE,			/* it is known that no row count, do not parse */
	COMBINE_TYPE_SUM,			/* sum row counts (partitioned, round robin) */
	COMBINE_TYPE_SAME			/* expect all row counts to be the same (replicated write) */
}	CombineType;

typedef enum
{
	REQUEST_TYPE_NOT_DEFINED,	/* not determined yet */
	REQUEST_TYPE_COMMAND,		/* OK or row count response */
	REQUEST_TYPE_QUERY,			/* Row description response */
	REQUEST_TYPE_COPY_IN,		/* Copy In response */
	REQUEST_TYPE_COPY_OUT		/* Copy Out response */
}	RequestType;


typedef struct
{
	int			node_count;
	CombineType combine_type;
	CommandDest dest;
	int			command_complete_count;
	int			row_count;
	RequestType request_type;
	int			description_count;
	List	   *simple_aggregates;
}	ResponseCombinerData;


typedef ResponseCombinerData *ResponseCombiner;

extern ResponseCombiner CreateResponseCombiner(int node_count,
					   CombineType combine_type, CommandDest dest);
extern int CombineResponse(ResponseCombiner combiner, char msg_type,
				char *msg_body, size_t len);
extern bool ValidateAndCloseCombiner(ResponseCombiner combiner);
extern bool ValidateAndResetCombiner(ResponseCombiner combiner);
extern void AssignCombinerAggregates(ResponseCombiner combiner, List *simple_aggregates);

#endif   /* COMBINER_H */
