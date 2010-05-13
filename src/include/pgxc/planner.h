/*-------------------------------------------------------------------------
 *
 * planner.h
 *		Externally declared locator functions
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
#ifndef PGXCPLANNER_H
#define PGXCPLANNER_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "pgxc/combiner.h"


/* for Query_Plan.exec_loc_type can have these OR'ed*/
#define EXEC_ON_COORD 0x1
#define EXEC_ON_DATA_NODES 0x2

/* Contains instructions on processing a step of a query.
 * In the prototype this will be simple, but it will eventually
 * evolve into a GridSQL-style QueryStep.
 */
typedef struct
{
	char	   *sql_statement;
	Exec_Nodes *exec_nodes;
	CombineType combine_type;
	List	   *simple_aggregates;		/* simple aggregate to combine on this
										 * step */
} Query_Step;


/*
 * The PGXC plan to execute.
 * In the prototype this will be simple, and queryStepList will
 * contain just one step.
 */
typedef struct
{
	int			exec_loc_type;
	bool		force_autocommit;		/* For CREATE DATABASE */
	List	   *query_step_list;	/* List of QuerySteps */
} Query_Plan;


/* For handling simple aggregates (no group by present)
 * For now, only MAX will be supported.
 */
typedef enum
{
	AGG_TYPE_MAX,
	AGG_TYPE_MIN,
	AGG_TYPE_COUNT,
	AGG_TYPE_SUM,
	AGG_TYPE_AVG
} SimpleAggType;


/* For handling simple aggregates */
/* For now, only support int/long types */
typedef struct
{
	int			column_pos;		/* Only use 1 for now */
	Aggref	   *aggref;
	Oid			transfn_oid;
	Oid			finalfn_oid;

	/* Input Functions, to parse arguments coming from the data nodes */
	FmgrInfo	arginputfn;
	Oid			argioparam;

	/* Output Function, to encode result to present to client */
	FmgrInfo	resoutputfn;

	/*
	 * fmgr lookup data for transfer functions --- only valid when
	 * corresponding oid is not InvalidOid.  Note in particular that fn_strict
	 * flags are kept here.
	 */
	FmgrInfo	transfn;
	FmgrInfo	finalfn;

	/*
	 * initial value from pg_aggregate entry
	 */
	Datum		initValue;
	bool		initValueIsNull;

	/*
	 * We need the len and byval info for the agg's input, result, and
	 * transition data types in order to know how to copy/delete values.
	 */
	int16		inputtypeLen,
				resulttypeLen,
				transtypeLen;
	bool		inputtypeByVal,
				resulttypeByVal,
				transtypeByVal;

	/*
	 * State of current group
	 */
	bool		noCollectValue;
	Datum		collectValue;
	bool		collectValueNull;

	/* a value buffer to avoid multiple allocations */
	StringInfoData valuebuf;
} SimpleAgg;

/* forbid SQL if unsafe, useful to turn off for development */
extern bool StrictStatementChecking;

/* forbid SELECT even multi-node ORDER BY */
extern bool StrictSelectChecking;

extern Query_Plan *GetQueryPlan(Node *parsetree, const char *sql_statement,
								List *querytree_list);
extern void FreeQueryPlan(Query_Plan *query_plan);
extern bool IsHashDistributable(Oid col_type);

#endif   /* PGXCPLANNER_H */
