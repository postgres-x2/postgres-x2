/*-------------------------------------------------------------------------
 *
 * pgxcplan.h
 *		Postgres-XC specific planner interfaces and structures.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxcplan.h
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
	bool			read_only;		/* do not use 2PC when committing read only steps */
	bool			force_autocommit;	/* some commands like VACUUM require autocommit mode */
	char			*statement;		/* if specified use it as a PreparedStatement name on Datanodes */
	char			*cursor;		/* if specified use it as a Portal name on Datanodes */
	int			remote_num_params;		/* number of parameters specified for Prepared remote statement */
	Oid			*remote_param_types;		/* parameter types, this pointer is shared
							 * across all the RemoteQuery nodes in the
							 * plan. So, don't change this once set.
							 */
	RemoteQueryExecType	exec_type;
	bool			is_temp;		/* determine if this remote node is based
									 * on a temporary objects (no 2PC) */

	bool			rq_finalise_aggs;	/* Aggregates should be finalised at the
										 * Datanode
										 */
	bool			rq_sortgroup_colno;	/* Use resno for sort group references
										 * instead of expressions
										 */
	Query			*remote_query;	/* Query structure representing the query to be
									 * sent to the datanodes
									 */
	List			*base_tlist;	/* the targetlist representing the result of
									 * the query to be sent to the datanode
									 */
	/*
	 * Reference targetlist of Vars to match the Vars in the plan nodes on
	 * coordinator to the corresponding Vars in the remote_query. These
	 * targetlists are used to while replacing/adding targetlist and quals in
	 * the remote_query.
	 */
	List			*coord_var_tlist;
	List			*query_var_tlist;
	bool			has_row_marks;		/* Did SELECT had FOR UPDATE/SHARE? */
	bool			has_ins_child_sel_parent;	/* This node is part of an INSERT SELECT that
								 * inserts into child by selecting from its parent */
} RemoteQuery;

extern PlannedStmt *pgxc_planner(Query *query, int cursorOptions,
								 ParamListInfo boundParams);
extern List *AddRemoteQueryNode(List *stmts, const char *queryString,
								RemoteQueryExecType remoteExecType, bool is_temp);
extern bool pgxc_query_contains_temp_tables(List *queries);
extern bool pgxc_query_contains_utility(List *queries);
extern void pgxc_rqplan_adjust_tlist(RemoteQuery *rqplan);

extern Plan *pgxc_make_modifytable(PlannerInfo *root, Plan *topplan);
extern ExecNodes *pgxc_is_join_reducible(ExecNodes *inner_en, ExecNodes *outer_en,
						Relids in_relids, Relids out_relids, JoinType jointype,
						List *join_quals, List *rtables);

#endif   /* PGXCPLANNER_H */
