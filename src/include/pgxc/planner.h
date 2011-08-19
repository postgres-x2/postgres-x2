/*-------------------------------------------------------------------------
 *
 * planner.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
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
	EXEC_ON_ALL_NODES,
	EXEC_ON_NONE
} RemoteQueryExecType;

typedef enum
{
	EXEC_DIRECT_NONE,
	EXEC_DIRECT_LOCAL,
	EXEC_DIRECT_LOCAL_UTILITY,
	EXEC_DIRECT_UTILITY,
	EXEC_DIRECT_SELECT,
	EXEC_DIRECT_INSERT,
	EXEC_DIRECT_UPDATE,
	EXEC_DIRECT_DELETE
} ExecDirectType;

/*
 * Contains instructions on processing a step of a query.
 * In the prototype this will be simple, but it will eventually
 * evolve into a GridSQL-style QueryStep.
 */
typedef struct
{
	Scan		scan;
	bool		is_single_step;		/* special case, skip extra work */
	ExecDirectType	exec_direct_type;	/* track if remote query is execute direct and what type it is */
	char	   *sql_statement;
	ExecNodes  *exec_nodes;			/* List of Datanodes where to launch query */
	CombineType combine_type;
	SimpleSort *sort;
	SimpleDistinct *distinct;
	bool		read_only;          /* do not use 2PC when committing read only steps */
	bool		force_autocommit;	/* some commands like VACUUM require autocommit mode */
	char	   *statement;			/* if specified use it as a PreparedStatement name on data nodes */
	char	   *cursor;				/* if specified use it as a Portal name on data nodes */
	int			num_params;			/* number of parameters specified for Prepared statement */
	Oid		   *param_types;		/* parameter types, this pointer is shared
									 * across all the RemoteQuery nodes in the
									 * plan. So, don't change this once set.
									 */
	RemoteQueryExecType		exec_type;
	bool					is_temp; /* determine if this remote node is based
									  * on a temporary objects (no 2PC) */

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

extern bool IsJoinReducible(RemoteQuery *innernode, RemoteQuery *outernode,
					List *rtable_list, JoinPath *join_path, JoinReduceInfo *join_info);

extern List *AddRemoteQueryNode(List *stmts, const char *queryString,
								RemoteQueryExecType remoteExecType, bool is_temp);

#endif   /* PGXCPLANNER_H */
