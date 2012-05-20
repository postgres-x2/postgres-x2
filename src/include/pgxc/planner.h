/*-------------------------------------------------------------------------
 *
 * planner.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
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
	Oid		   *sortCollations;
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
} SimpleSort;

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
	Scan			scan;
	ExecDirectType		exec_direct_type;	/* track if remote query is execute direct and what type it is */
	char			*sql_statement;
	ExecNodes		*exec_nodes;		/* List of Datanodes where to launch query */
	CombineType		combine_type;
	SimpleSort		*sort;
	bool			read_only;		/* do not use 2PC when committing read only steps */
	bool			force_autocommit;	/* some commands like VACUUM require autocommit mode */
	char			*statement;		/* if specified use it as a PreparedStatement name on data nodes */
	char			*cursor;		/* if specified use it as a Portal name on data nodes */
	int			num_params;		/* number of parameters specified for Prepared statement */
	Oid			*param_types;		/* parameter types, this pointer is shared
							 * across all the RemoteQuery nodes in the
							 * plan. So, don't change this once set.
							 */
	RemoteQueryExecType	exec_type;
	bool			is_temp;		/* determine if this remote node is based
							 * on a temporary objects (no 2PC) */

	int			reduce_level;		/* in case of reduced JOIN, it's level    */
	List			*base_tlist;		/* in case of isReduced, the base tlist   */
	char			*outer_alias;
	char			*inner_alias;
	int			outer_reduce_level;
	int			inner_reduce_level;
	Relids			outer_relids;
	Relids			inner_relids;
	char			*inner_statement;
	char			*outer_statement;
	char			*join_condition;
} RemoteQuery;

/*
 * FQS_context
 * This context structure is used by the Fast Query Shipping walker, to gather
 * information during analysing query for Fast Query Shipping.
 */
typedef struct
{
	Bitmapset	*fqsc_shippability;	/* The conditions for (un)shippability of the
									 * query.
									 */
	Query	*fqsc_query;			/* the query being analysed for FQS */
	int		fqsc_query_level;		/* level of the query */
	int		fqsc_max_varlevelsup;	/* maximum upper level referred to by any
									 * variable reference in the query. If this
									 * value is greater than 0, the query is not
									 * shippable, if shipped alone.
									 */
	ExecNodes	*fqsc_exec_nodes;	/* nodes where the query should be executed */
	ExecNodes	*fqsc_subquery_en;	/* ExecNodes produced by merging the ExecNodes
									 * for individual subqueries. This gets
									 * ultimately merged with fqsc_exec_nodes.
									 */
} FQS_context;

/* enum for reasons as to why a query is not FQSable */
typedef enum
{
	FQS_UNSHIPPABLE_EXPR = 0,		/* it has unshippable expression */
	FQS_SINGLENODE_EXPR,			/* it has single node expression, like
									 * aggregates, ORDER BY etc. */
	FQS_NEEDS_COORD,				/* the query needs coordinator */
	FQS_VARLEVEL,					/* one of its subqueries has a VAR
									 * referencing an upper level query
									 * relation */
	FQS_NO_NODES,					/* no suitable nodes can be found to ship
									 * the query */
	FQS_UNSUPPORTED_EXPR			/* it has expressions currently unsupported
									 * by FQS, but such expressions might be
									 * supported by FQS in future */
} FQS_shippability;

/* global variable corresponding to the GUC with same name */
extern bool enable_fast_query_shipping;
/* forbid SQL if unsafe, useful to turn off for development */
extern bool StrictStatementChecking;

/* forbid SELECT even multi-node ORDER BY */
extern bool StrictSelectChecking;

extern PlannedStmt *pgxc_planner(Query *query, int cursorOptions,
								 ParamListInfo boundParams);
extern bool IsHashDistributable(Oid col_type);

extern ExecNodes *IsJoinReducible(RemoteQuery *innernode, RemoteQuery *outernode,
									Relids in_relids, Relids out_relids,
									Join *join, JoinPath *join_path, List *rtable);

extern List *AddRemoteQueryNode(List *stmts, const char *queryString,
								RemoteQueryExecType remoteExecType, bool is_temp);
extern bool pgxc_query_contains_temp_tables(List *queries);
extern bool pgxc_query_contains_utility(List *queries);
extern Expr *pgxc_find_distcol_expr(Index varno, PartAttrNumber partAttrNum,
												Node *quals);

#endif   /* PGXCPLANNER_H */
