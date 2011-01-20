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
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "tcop/dest.h"
#include "nodes/relation.h"


typedef enum
{
	COMBINE_TYPE_NONE,			/* it is known that no row count, do not parse */
	COMBINE_TYPE_SUM,			/* sum row counts (partitioned, round robin) */
	COMBINE_TYPE_SAME			/* expect all row counts to be the same (replicated write) */
}	CombineType;

/* For sorting within RemoteQuery handling */
/*
 * It is pretty much like Sort, but without Plan. We may use Sort later.
 */
typedef struct
{
	NodeTag		type;
	int			numCols;		/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
} SimpleSort;

/* For returning distinct results from the RemoteQuery*/
typedef struct
{
	NodeTag		type;
	int			numCols;		/* number of sort-key columns */
	AttrNumber *uniqColIdx;		/* their indexes in the target list */
	Oid		   *eqOperators;	/* OIDs of operators to equate them by */
} SimpleDistinct;

/*
 * Determines if query has to be launched
 * on Coordinators only (SEQUENCE DDL),
 * on Datanodes (normal Remote Queries),
 * or on all Postgres-XC nodes (Utilities and DDL).
 */
typedef enum
{
	EXEC_ON_DATANODES,
	EXEC_ON_COORDS,
	EXEC_ON_ALL_NODES
} RemoteQueryExecType;

/*
 * Contains instructions on processing a step of a query.
 * In the prototype this will be simple, but it will eventually
 * evolve into a GridSQL-style QueryStep.
 */
typedef struct
{
	Scan		scan;
	bool		is_single_step;		/* special case, skip extra work */
	char	   *sql_statement;
	ExecNodes  *exec_nodes;			/* List of Datanodes where to launch query */
	CombineType combine_type;
	List	   *simple_aggregates;	/* simple aggregate to combine on this step */
	SimpleSort *sort;
	SimpleDistinct *distinct;
	bool		read_only;          /* do not use 2PC when committing read only steps */
	bool		force_autocommit;	/* some commands like VACUUM require autocommit mode */
	char	   *statement;			/* if specified use it as a PreparedStatement name on data nodes */
	char	   *cursor;				/* if specified use it as a Portal name on data nodes */
	RemoteQueryExecType		exec_type;

	/* Support for parameters */
	char	   *paramval_data;		/* parameter data, format is like in BIND */
	int			paramval_len;		/* length of parameter values data */

	char	  *relname;
	bool	   remotejoin;			/* True if this is a reduced remote join  */
	bool	   partitioned_replicated;	/* True if reduced and contains replicated-partitioned join */
	int		   reduce_level;		/* in case of reduced JOIN, it's level    */
	List	  *base_tlist;			/* in case of isReduced, the base tlist   */
	char	  *outer_alias;
	char	  *inner_alias;
	int		   outer_reduce_level;
	int		   inner_reduce_level;
	Relids	   outer_relids;
	Relids	   inner_relids;
	char 	  *inner_statement;
	char	  *outer_statement;
	char	  *join_condition;
} RemoteQuery;


/*
 * For handling simple aggregates (no group by present)
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
typedef struct
{
	NodeTag		type;
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

typedef struct
{
	bool partitioned_replicated;
	ExecNodes *exec_nodes;
} JoinReduceInfo;

/* forbid SQL if unsafe, useful to turn off for development */
extern bool StrictStatementChecking;

/* forbid SELECT even multi-node ORDER BY */
extern bool StrictSelectChecking;

extern PlannedStmt *pgxc_planner(Query *query, int cursorOptions,
								 ParamListInfo boundParams);
extern bool IsHashDistributable(Oid col_type);

extern bool is_immutable_func(Oid funcid);

extern bool IsJoinReducible(RemoteQuery *innernode, RemoteQuery *outernode,
					List *rtable_list, JoinPath *join_path, JoinReduceInfo *join_info);

#endif   /* PGXCPLANNER_H */
