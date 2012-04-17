/*-------------------------------------------------------------------------
 *
 * planner.c
 *
 *	  Functions for generating a PGXC style plan.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
 *
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/planner.h"
#include "pgxc/postgresql_fdw.h"
#include "tcop/pquery.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/syscache.h"
#include "utils/numeric.h"
#include "utils/memutils.h"
#include "access/hash.h"
#include "commands/tablecmds.h"
#include "utils/timestamp.h"
#include "utils/date.h"

/* Forbid unsafe SQL statements */
bool		StrictStatementChecking = true;
/* fast query shipping is enabled by default */
bool		enable_fast_query_shipping = true;

static RemoteQuery *makeRemoteQuery(void);
static void validate_part_col_updatable(const Query *query);
static bool contains_temp_tables(List *rtable);
static bool contains_only_pg_catalog(List *rtable);
static void pgxc_handle_unsupported_stmts(Query *query);
static PlannedStmt *pgxc_FQS_planner(Query *query, int cursorOptions,
										ParamListInfo boundParams);
static bool pgxc_query_needs_coord(Query *query);
static ExecNodes *pgxc_is_query_shippable(Query *query, int query_level);
static bool pgxc_FQS_walker(Node *node, FQS_context *fqs_context);
static void pgxc_FQS_find_datanodes(FQS_context *fqs_context);
static ExecNodes *pgxc_merge_exec_nodes(ExecNodes *exec_nodes1,
										ExecNodes *exec_nodes2,
										bool merge_dist_equijoin,
										bool merge_replicated_only);
static PlannedStmt *pgxc_handle_exec_direct(Query *query, int cursorOptions,
												ParamListInfo boundParams);
static RemoteQuery *pgxc_FQS_create_remote_plan(Query *query,
												ExecNodes *exec_nodes,
												bool is_exec_direct);
static ExecNodes *pgxc_FQS_get_relation_nodes(RangeTblEntry *rte, Index varno,
												Query *query);
static bool pgxc_qual_hash_dist_equijoin(Relids varnos_1, Relids varnos_2,
											Oid distcol_type, Node *quals,
											List *rtable);
static bool VarAttrIsPartAttr(Var *var, List *rtable);
static void pgxc_FQS_set_reason(FQS_context *context, FQS_shippability reason);
static bool pgxc_FQS_test_reason(FQS_context *context, FQS_shippability reason);

/*
 * make_ctid_col_ref
 *
 * creates a Var for a column referring to ctid
 */

static Var *
make_ctid_col_ref(Query *qry)
{
	ListCell		*lc1, *lc2;
	RangeTblEntry		*rte1, *rte2;
	int			tableRTEs, firstTableRTENumber;
	RangeTblEntry		*rte_in_query;
	AttrNumber		attnum;
	Oid			vartypeid;
	int32			type_mod;
	Oid			varcollid;

	/* If the query has more than 1 table RTEs where both are different, we can not add ctid to the query target list 
	 * We should in this case skip adding it to the target list and a WHERE CURRENT OF should then 
	 * fail saying the query is not a simply update able scan of table
	 */

	tableRTEs = 0;
	foreach(lc1, qry->rtable)
	{
		rte1 = (RangeTblEntry *) lfirst(lc1);

		if (rte1->rtekind == RTE_RELATION)
		{
			tableRTEs++;
			if (tableRTEs > 1)
			{
				/* See if we get two RTEs in case we have two references 
				* to the same table with different aliases
				*/
				foreach(lc2, qry->rtable)
				{
					rte2 = (RangeTblEntry *) lfirst(lc2);
		
					if (rte2->rtekind == RTE_RELATION)
					{
						if (rte2->relid != rte1->relid)
						{
							return NULL;
						}
					}
				}
				continue;
			}
			rte_in_query = rte1;
		}
	}

	if (tableRTEs > 1)
	{
		firstTableRTENumber = 0;
		foreach(lc1, qry->rtable)
		{
			rte1 = (RangeTblEntry *) lfirst(lc1);
			firstTableRTENumber++;
			if (rte1->rtekind == RTE_RELATION)
			{
				break;
			}
		}
	}
	else
	{
		firstTableRTENumber = 1;
	}

	attnum = specialAttNum("ctid");
	get_rte_attribute_type(rte_in_query, attnum, &vartypeid, &type_mod, &varcollid);
	return makeVar(firstTableRTENumber, attnum, vartypeid, type_mod, varcollid, 0);
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * only contain pg_catalog entries.
 */
static bool
contains_only_pg_catalog(List *rtable)
{
	ListCell *item;

	/* May be complicated. Before giving up, just check for pg_catalog usage */
	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

		if (rte->rtekind == RTE_RELATION)
		{
			if (get_rel_namespace(rte->relid) != PG_CATALOG_NAMESPACE)
				return false;
		}
		else if (rte->rtekind == RTE_SUBQUERY &&
				 !contains_only_pg_catalog(rte->subquery->rtable))
			return false;
	}
	return true;
}


/*
 * Returns true if at least one temporary table is in use
 * in query (and its subqueries)
 */
static bool
contains_temp_tables(List *rtable)
{
	ListCell *item;

	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

		if (rte->rtekind == RTE_RELATION)
		{
			if (IsTempTable(rte->relid))
				return true;
		}
		else if (rte->rtekind == RTE_SUBQUERY &&
				 contains_temp_tables(rte->subquery->rtable))
			return true;
	}

	return false;
}

/*
 * Create an instance of RemoteQuery and initialize fields
 */
static RemoteQuery *
makeRemoteQuery(void)
{
	RemoteQuery *result = makeNode(RemoteQuery);
	result->combine_type = COMBINE_TYPE_NONE;
	result->exec_type = EXEC_ON_DATANODES;
	result->exec_direct_type = EXEC_DIRECT_NONE;

	return result;
}

/*
 * get_plan_combine_type - determine combine type
 *
 * COMBINE_TYPE_SAME - for replicated updates
 * COMBINE_TYPE_SUM - for hash and round robin updates
 * COMBINE_TYPE_NONE - for operations where row_count is not applicable
 *
 * return NULL if it is not safe to be done in a single step.
 */
static CombineType
get_plan_combine_type(Query *query, char baselocatortype)
{

	switch (query->commandType)
	{
		case CMD_INSERT:
		case CMD_UPDATE:
		case CMD_DELETE:
			return baselocatortype == LOCATOR_TYPE_REPLICATED ?
					COMBINE_TYPE_SAME : COMBINE_TYPE_SUM;

		default:
			return COMBINE_TYPE_NONE;
	}
	/* quiet compiler warning */
	return COMBINE_TYPE_NONE;
}

/*
 * get oid of the function whose name is passed as argument
 */

static Oid
get_fn_oid(char *fn_name, Oid *p_rettype)
{
	Value		*fn_nm;
	List		*fn_name_list;
	FuncDetailCode	fdc;
	bool		retset;
	int		nvargs;
	Oid		*true_typeids;
	Oid		func_oid;

	fn_nm = makeString(fn_name);
	fn_name_list = list_make1(fn_nm);
	
	fdc = func_get_detail(fn_name_list,
				NULL,			/* argument expressions */
				NULL,			/* argument names */
				0,			/* argument numbers */
				NULL,			/* argument types */
				false,			/* expand variable number or args */
				false,			/* expand defaults */
				&func_oid,		/* oid of the function - returned detail*/
				p_rettype,		/* function return type - returned detail */
				&retset,		/*  - returned detail*/
				&nvargs,		/*  - returned detail*/
				&true_typeids,		/*  - returned detail */
				NULL			/* arguemnt defaults returned*/
				);

	pfree(fn_name_list);
	if (fdc == FUNCDETAIL_NORMAL)
	{
		return func_oid;
	}
	return InvalidOid;
}

/*
 * Append ctid to the field list of step queries to support update
 * WHERE CURRENT OF. The ctid is not sent down to client but used as a key
 * to find target tuple.
 * PGXCTODO: Bug
 * This function modifies the original query to add ctid
 * and nodename in the targetlist. It should rather modify the targetlist of the
 * query to be shipped by the RemoteQuery node.
 */
static void
fetch_ctid_of(Plan *subtree, Query *query)
{
	/* recursively process subnodes */
	if (innerPlan(subtree))
		fetch_ctid_of(innerPlan(subtree), query);
	if (outerPlan(subtree))
		fetch_ctid_of(outerPlan(subtree), query);

	/* we are only interested in RemoteQueries */
	if (IsA(subtree, RemoteQuery))
	{
		RemoteQuery		*step = (RemoteQuery *) subtree;
		TargetEntry		*te1;
		Query			*temp_qry;
		FuncExpr		*func_expr;
		AttrNumber		resno;
		Oid			funcid;
		Oid			rettype;
		Var			*ctid_expr;
		MemoryContext		oldcontext;
		MemoryContext		tmpcontext;

		tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
							"Temp Context",
							ALLOCSET_DEFAULT_MINSIZE,
							ALLOCSET_DEFAULT_INITSIZE,
							ALLOCSET_DEFAULT_MAXSIZE);
		oldcontext = MemoryContextSwitchTo(tmpcontext);

		/* Copy the query tree to make changes to the target list */
		temp_qry = copyObject(query);
		/* Get the number of entries in the target list */
		resno = list_length(temp_qry->targetList);

		/* Make a ctid column ref expr to add in target list */
		ctid_expr = make_ctid_col_ref(temp_qry);
		if (ctid_expr == NULL)
		{
			MemoryContextSwitchTo(oldcontext);
			MemoryContextDelete(tmpcontext);
			return;
		}

		te1 = makeTargetEntry((Expr *)ctid_expr, resno+1, NULL, false);

		/* add the target entry to the query target list */
		temp_qry->targetList = lappend(temp_qry->targetList, te1);

		/* PGXCTODO We can take this call in initialization rather than getting it always */

		/* Get the Oid of the function */
		funcid = get_fn_oid("pgxc_node_str", &rettype);
		if (OidIsValid(funcid))
		{
			StringInfoData		deparsed_qry;
			TargetEntry		*te2;

			/* create a function expression */
			func_expr = makeFuncExpr(funcid, rettype, NULL, InvalidOid, InvalidOid, COERCE_DONTCARE);
			/* make a target entry for function call */
			te2 = makeTargetEntry((Expr *)func_expr, resno+2, NULL, false);
			/* add the target entry to the query target list */
			temp_qry->targetList = lappend(temp_qry->targetList, te2);

			initStringInfo(&deparsed_qry);
			deparse_query(temp_qry, &deparsed_qry, NIL);

			MemoryContextSwitchTo(oldcontext);

			if (step->sql_statement != NULL)
				pfree(step->sql_statement);

			step->sql_statement = pstrdup(deparsed_qry.data);

			MemoryContextDelete(tmpcontext);
		}
		else
		{
			MemoryContextSwitchTo(oldcontext);
			MemoryContextDelete(tmpcontext);
		}
	}
}

/*
 * Build up a QueryPlan to execute on.
 *
 * This functions tries to find out whether
 * 1. The statement can be shipped to the datanode and coordinator is needed
 *    only as a proxy - in which case, it creates a single node plan.
 * 2. The statement can be evaluated on the coordinator completely - thus no
 *    query shipping is involved and standard_planner() is invoked to plan the
 *    statement
 * 3. The statement needs coordinator as well as datanode for evaluation -
 *    again we use standard_planner() to plan the statement.
 *
 * The plan generated in either of the above cases is returned.
 */
PlannedStmt *
pgxc_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;

	/* handle the un-supported statements, obvious errors etc. */
	pgxc_handle_unsupported_stmts(query);

	result = pgxc_handle_exec_direct(query, cursorOptions, boundParams);
	if (result)
		return result;

	/* see if can ship the query completely */
	result = pgxc_FQS_planner(query, cursorOptions, boundParams);
	if (result)
		return result;

	/* we need coordinator for evaluation, invoke standard planner */
	result = standard_planner(query, cursorOptions, boundParams);
	return result;
}

static PlannedStmt *
pgxc_handle_exec_direct(Query *query, int cursorOptions,
						ParamListInfo boundParams)
{
	PlannedStmt		*result = NULL;
	PlannerGlobal	*glob;
	PlannerInfo		*root;
	/*
	 * if the query has its utility set, it could be an EXEC_DIRECT statement,
	 * check if it needs to be executed on coordinator
	 */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, RemoteQuery))
	{
		RemoteQuery *node = (RemoteQuery *)query->utilityStmt;
		/* EXECUTE DIRECT statements on remote nodes don't need coordinator */
		if (node->exec_direct_type != EXEC_DIRECT_NONE &&
			node->exec_direct_type != EXEC_DIRECT_LOCAL &&
			node->exec_direct_type != EXEC_DIRECT_LOCAL_UTILITY)
		{
			glob = makeNode(PlannerGlobal);
			glob->boundParams = boundParams;
			/* Create a PlannerInfo data structure, usually it is done for a subquery */
			root = makeNode(PlannerInfo);
			root->parse = query;
			root->glob = glob;
			root->query_level = 1;
			root->planner_cxt = CurrentMemoryContext;
			/* build the PlannedStmt result */
			result = makeNode(PlannedStmt);
			/* Try and set what we can, rest must have been zeroed out by makeNode() */
			result->commandType = query->commandType;
			result->canSetTag = query->canSetTag;
			result->intoClause = query->intoClause;
			/* Set result relations */
			if (query->commandType != CMD_SELECT)
				result->resultRelations = list_make1_int(query->resultRelation);

			result->planTree = (Plan *)pgxc_FQS_create_remote_plan(query, NULL, true);
			result->rtable = query->rtable;
			/*
			 * We need to save plan dependencies, so that dropping objects will
			 * invalidate the cached plan if it depends on those objects. Table
			 * dependencies are available in glob->relationOids and all other
			 * dependencies are in glob->invalItems. These fields can be retrieved
			 * through set_plan_references().
			 */
			result->planTree = set_plan_references(glob, result->planTree,
			                                       query->rtable, root->rowMarks);
			result->relationOids = glob->relationOids;
			result->invalItems = glob->invalItems;
		}
	}
	return result;
}
/*
 * pgxc_handle_unsupported_stmts
 * Throw error for the statements that can not be handled in XC
 */
static void
pgxc_handle_unsupported_stmts(Query *query)
{
	/*
	 * PGXCTODO: This validation will not be removed
	 * until we support moving tuples from one node to another
	 * when the partition column of a table is updated
	 */
	if (query->commandType == CMD_UPDATE)
		validate_part_col_updatable(query);

	if (query->returningList)
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				 (errmsg("RETURNING clause not yet supported"))));
}

/*
 * pgxc_FQS_planner
 * The routine tries to see if the statement can be completely evaluated on the
 * datanodes. In such cases coordinator is not needed to evaluate the statement,
 * and just acts as a proxy. A statement can be completely shipped to the remote
 * node if every row of the result can be evaluated on a single datanode.
 * For example:
 *
 * 1. SELECT * FROM tab1; where tab1 is a distributed table - Every row of the
 * result set can be evaluated at a single datanode. Hence this statement is
 * completely shippable even though many datanodes are involved in evaluating
 * complete result set. In such case coordinator will be able to gather rows
 * arisign from individual datanodes and proxy the result to the client.
 *
 * 2. SELECT count(*) FROM tab1; where tab1 is a distributed table - there is
 * only one row in the result but it needs input from all the datanodes. Hence
 * this is not completely shippable.
 *
 * 3. SELECT count(*) FROM tab1; where tab1 is replicated table - since result
 * can be obtained from a single datanode, this is a completely shippable
 * statement.
 *
 * fqs in the name of function is acronym for fast query shipping.
 */
static PlannedStmt *
pgxc_FQS_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt		*result;
	PlannerGlobal	*glob;
	PlannerInfo		*root;
	ExecNodes		*exec_nodes;
	Plan			*top_plan;

	/* Try by-passing standard planner, if fast query shipping is enabled */
	if (!enable_fast_query_shipping)
		return NULL;

	/* Cursor options may come from caller or from DECLARE CURSOR stmt */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, DeclareCursorStmt))
		cursorOptions |= ((DeclareCursorStmt *) query->utilityStmt)->options;
	/*
	 * If the query can not be or need not be shipped to the datanodes, don't
	 * create any plan here. standard_planner() will take care of it.
	 */
	exec_nodes = pgxc_is_query_shippable(query, 0);
	if (exec_nodes == NULL)
		return NULL;

	glob = makeNode(PlannerGlobal);
	glob->boundParams = boundParams;
	/* Create a PlannerInfo data structure, usually it is done for a subquery */
	root = makeNode(PlannerInfo);
	root->parse = query;
	root->glob = glob;
	root->query_level = 1;
	root->planner_cxt = CurrentMemoryContext;

	/*
	 * We decided to ship the query to the datanode/s, create a RemoteQuery node
	 * for the same.
	 */
	top_plan = (Plan *)pgxc_FQS_create_remote_plan(query, exec_nodes, false);
	/*
	 * If creating a plan for a scrollable cursor, make sure it can run
	 * backwards on demand.  Add a Material node at the top at need.
	 */
	if (cursorOptions & CURSOR_OPT_SCROLL)
	{
		if (!ExecSupportsBackwardScan(top_plan))
			top_plan = materialize_finished_plan(top_plan);
	}

	/*
	 * Just before creating the PlannedStmt, do some final cleanup
	 * We need to save plan dependencies, so that dropping objects will
	 * invalidate the cached plan if it depends on those objects. Table
	 * dependencies are available in glob->relationOids and all other
	 * dependencies are in glob->invalItems. These fields can be retrieved
	 * through set_plan_references().
	 */
	top_plan = set_plan_references(glob, top_plan, query->rtable,
									root->rowMarks);
	/* build the PlannedStmt result */
	result = makeNode(PlannedStmt);
	/* Try and set what we can, rest must have been zeroed out by makeNode() */
	result->commandType = query->commandType;
	result->canSetTag = query->canSetTag;
	result->utilityStmt = query->utilityStmt;
	result->intoClause = query->intoClause;
	/* Set result relations */
	if (query->commandType != CMD_SELECT)
		result->resultRelations = list_make1_int(query->resultRelation);
	result->planTree = top_plan;
	result->rtable = query->rtable;
	result->relationOids = glob->relationOids;
	result->invalItems = glob->invalItems;

	/*
	 * If query is DECLARE CURSOR fetch CTIDs and node names from the remote node
	 * Use CTID as a key to update/delete tuples on remote nodes when handling
	 * WHERE CURRENT OF.
	 */
	if (query->utilityStmt && IsA(query->utilityStmt, DeclareCursorStmt))
		fetch_ctid_of(result->planTree, query);
	return result;
}

static RemoteQuery *
pgxc_FQS_create_remote_plan(Query *query, ExecNodes *exec_nodes, bool is_exec_direct)
{
	RemoteQuery *query_step;
	StringInfoData buf;
	RangeTblEntry	*dummy_rte;

	/* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
	if (is_exec_direct)
	{
		Assert(IsA(query->utilityStmt, RemoteQuery));
		query_step = (RemoteQuery *)query->utilityStmt;
		query->utilityStmt = NULL;
	}
	else
	{
		query_step = makeRemoteQuery();
		query_step->exec_nodes = exec_nodes;
	}

	Assert(query_step->exec_nodes);

	/* Datanodes should finalise the results of this query */
	query->qry_finalise_aggs = true;

	/* Deparse query tree to get step query. */
	if ( query_step->sql_statement == NULL )
	{
		initStringInfo(&buf);
		deparse_query(query, &buf, NIL);
		query_step->sql_statement = pstrdup(buf.data);
		pfree(buf.data);
	}
	/*
	 * PGXCTODO: we may route this same Query structure through
	 * standard_planner, where we don't want datanodes to finalise the results.
	 * Turn it off. At some point, we will avoid routing the same query
	 * structure through the standard_planner by modifying it only when it's not
	 * be routed through standard_planner.
	 */
	query->qry_finalise_aggs = false;
	/* Optimize multi-node handling */
	query_step->read_only = query->commandType == CMD_SELECT;
	/* Check if temporary tables are in use in query */
	/* PGXC_FQS_TODO: scanning the rtable again for the queries should not be
	 * needed. We should be able to find out if the query has a temporary object
	 * while finding nodes for the objects. But there is no way we can convey
	 * that information here. Till such a connection is available, this is it.
	 */
	if (contains_temp_tables(query->rtable))
		query_step->is_temp = true;

	/*
	 * We need to evaluate some expressions like the ExecNodes->en_expr at
	 * coordinator, prepare those for evaluation. Ideally we should call
	 * preprocess_expression, but it needs PlannerInfo structure for the same
	 */
	fix_opfuncids((Node *)(query_step->exec_nodes->en_expr));
	/*
	 * PGXCTODO
	 * When Postgres runs insert into t (a) values (1); against table
	 * defined as create table t (a int, b int); the plan is looking
	 * like insert into t (a,b) values (1,null);
	 * Later executor is verifying plan, to make sure table has not
	 * been altered since plan has been created and comparing table
	 * definition with plan target list and output error if they do
	 * not match.
	 * I could not find better way to generate targetList for pgxc plan
	 * then call standard planner and take targetList from the plan
	 * generated by Postgres.
	 */
	query_step->combine_type = get_plan_combine_type(
				query, query_step->exec_nodes->baselocatortype);

	/*
	 * Create a dummy RTE for the remote query being created. Append the dummy
	 * range table entry to the range table. Note that this modifies the master
	 * copy the caller passed us, otherwise e.g EXPLAIN VERBOSE will fail to
	 * find the rte the Vars built below refer to. Also create the tuple
	 * descriptor for the result of this query from the base_tlist (targetlist
	 * we used to generate the remote node query).
	 */
	dummy_rte = makeNode(RangeTblEntry);
	dummy_rte->rtekind = RTE_REMOTE_DUMMY;
	/* Use a dummy relname... */
	if (is_exec_direct)
		dummy_rte->relname = "__EXECUTE_DIRECT__";
	else
		dummy_rte->relname	   = "__REMOTE_FQS_QUERY__";
	dummy_rte->eref		   = makeAlias("__REMOTE_FQS_QUERY__", NIL);
	/* Rest will be zeroed out in makeNode() */

	query->rtable = lappend(query->rtable, dummy_rte);
	query_step->scan.scanrelid 	= list_length(query->rtable);
	query_step->scan.plan.targetlist = query->targetList;

	return query_step;
}

/*
 * pgxc_query_needs_coord
 * Check if the query needs coordinator for evaluation or it can be completely
 * evaluated on coordinator. Return true if so, otherwise return false.
 */
static bool
pgxc_query_needs_coord(Query *query)
{
	/*
	 * If the query is an EXEC DIRECT on the same coordinator where it's fired,
	 * it should not be shipped
	 */
	if (query->is_local)
		return true;
	/*
	 * If the query involves just the catalog tables, and is not an EXEC DIRECT
	 * statement, it can be evaluated completely on the coordinator. No need to
	 * involve datanodes.
	 */
	if (contains_only_pg_catalog(query->rtable))
		return true;


	/* Allow for override */
	if (query->commandType != CMD_SELECT &&
			query->commandType != CMD_INSERT &&
			query->commandType != CMD_UPDATE &&
			query->commandType != CMD_DELETE)
	{
		if (StrictStatementChecking)
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 (errmsg("This command is not yet supported."))));

			return true;
	}

	return false;
}

/*
 * Set the given reason in FQS_context indicating why the query can not be
 * shipped directly to the datanodes.
 */
static void
pgxc_FQS_set_reason(FQS_context *context, FQS_shippability reason)
{
	context->fqsc_shippability = bms_add_member(context->fqsc_shippability, reason);
}

/*
 * See if a given reason is why the query can not be shipped directly
 * to the datanodes.
 */
static bool
pgxc_FQS_test_reason(FQS_context *context, FQS_shippability reason)
{
	return bms_is_member(reason, context->fqsc_shippability);
}

/*
 * pgxc_is_query_shippable
 * This function calls the query walker to analyse the query to gather
 * information like  Constraints under which the query can be shippable, nodes
 * on which the query is going to be executed etc.
 * Based on the information gathered, it decides whether the query can be
 * executed on datanodes directly without involving coordinator.
 * If the query is shippable this routine also returns the nodes where the query
 * should be shipped. If the query is not shippable, it returns NULL.
 */
static ExecNodes *
pgxc_is_query_shippable(Query *query, int query_level)
{
	FQS_context fqs_context;
	ExecNodes	*exec_nodes;
	bool		canShip = true;
	Bitmapset	*shippability;

	memset(&fqs_context, 0, sizeof(fqs_context));
	/* let's assume that by default query is shippable */
	fqs_context.fqsc_query = query;
	fqs_context.fqsc_query_level = query_level;

	/*
	 * We might have already decided not to ship the query to the datanodes, but
	 * still walk it anyway to find out if there are any subqueries which can be
	 * shipped.
	 */
	pgxc_FQS_walker((Node *)query, &fqs_context);
	/*
	 * We have merged the nodelists and distributions of all subqueries seen in
	 * the query tree, merge it with the same obtained for the relations
	 * involved in the query.
	 * PGXC_FQS_TODO:
	 * Merge the subquery ExecNodes if both of them are replicated.
	 * The logic to merge node lists with other distribution
	 * strategy is not clear yet.
	 */
	exec_nodes = fqs_context.fqsc_exec_nodes;
	if (exec_nodes)
		exec_nodes = pgxc_merge_exec_nodes(exec_nodes,
											fqs_context.fqsc_subquery_en, false,
											true);

	/*
	 * Look at the information gathered by the walker in FQS_context and that
	 * in the Query structure to decide whether we should ship this query
	 * directly to the datanode or not
	 */

	/*
	 * If the planner was not able to find the datanodes to the execute the
	 * query, the query is not completely shippable. So, return NULL
	 */
	if (!exec_nodes)
		return NULL;

	/* Copy the shippability reasons. We modify the copy for easier handling.
	 * The original can be saved away */
	shippability = bms_copy(fqs_context.fqsc_shippability);

	/*
	 * If the query has an expression which renders the shippability to single
	 * node, and query needs to be shipped to more than one node, it can not be
	 * shipped
	 */
	if (bms_is_member(FQS_SINGLENODE_EXPR, shippability))
	{
		/* We handled the reason here, reset it */
		shippability = bms_del_member(shippability, FQS_SINGLENODE_EXPR);
		/* if nodeList has no nodes, it ExecNodes will have other means to know
		 * the nodes where to execute like distribution column expression. We
		 * can't tell how many nodes the query will be executed on, hence treat
		 * that as multiple nodes.
		 */
		if (list_length(exec_nodes->nodeList) != 1)
			canShip = false;
	}

	/* Can not ship the query for some reason */
	if (!bms_is_empty(shippability))
		canShip = false;

	/* Always keep this at the end before checking canShip and return */
	if (!canShip && exec_nodes)
		FreeExecNodes(&exec_nodes);
	/* If query is to be shipped, we should know where to execute the query */
	Assert (!canShip || exec_nodes);

	bms_free(shippability);
	shippability = NULL;

	return exec_nodes;
}

/*
 * pgxc_merge_exec_nodes
 * The routine combines the two exec_nodes passed such that the resultant
 * exec_node corresponds to the JOIN of respective relations.
 * If both exec_nodes can not be merged, it returns NULL.
 */
static ExecNodes *
pgxc_merge_exec_nodes(ExecNodes *en1, ExecNodes *en2, bool merge_dist_equijoin,
						bool merge_replicated_only)
{
	ExecNodes	*merged_en = makeNode(ExecNodes);
	ExecNodes	*tmp_en;

	/* If either of exec_nodes are NULL, return the copy of other one */
	if (!en1)
	{
		tmp_en = copyObject(en2);
		return tmp_en;
	}
	if (!en2)
	{
		tmp_en = copyObject(en1);
		return tmp_en;
	}

	/* Following cases are not handled in this routine */
	/* PGXC_FQS_TODO how should we handle table usage type? */
	if (en1->primarynodelist || en2->primarynodelist ||
		en1->en_expr || en2->en_expr ||
		OidIsValid(en1->en_relid) || OidIsValid(en2->en_relid) ||
		en1->accesstype != RELATION_ACCESS_READ || en2->accesstype != RELATION_ACCESS_READ)
		return NULL;

	if (IsLocatorReplicated(en1->baselocatortype) &&
		IsLocatorReplicated(en2->baselocatortype))
	{
		/*
		 * Replicated/replicated join case
		 * Check that replicated relation is not disjoint
		 * with initial relation which is also replicated.
		 * If there is a common portion of the node list between
		 * the two relations, other rtables have to be checked on
		 * this restricted list.
		 */
		merged_en->nodeList = list_intersection_int(en1->nodeList,
													en2->nodeList);
		merged_en->baselocatortype = LOCATOR_TYPE_REPLICATED;
		/* No intersection, so has to go though standard planner... */
		if (!merged_en->nodeList)
			FreeExecNodes(&merged_en);
		return merged_en;
	}

	/*
	 * We are told to merge the nodelists if both the distributions are
	 * replicated. We checked that above, so bail out
	 */
	if (merge_replicated_only)
	{
		FreeExecNodes(&merged_en);
		return merged_en;
	}

	if (IsLocatorReplicated(en1->baselocatortype) &&
			  IsLocatorColumnDistributed(en2->baselocatortype))
	{
		List	*diff_nodelist = NULL;
		/*
		 * Replicated/distributed join case.
		 * Node list of distributed table has to be included
		 * in node list of replicated table.
		 */
		diff_nodelist = list_difference_int(en2->nodeList, en1->nodeList);
		/*
		 * If the difference list is not empty, this means that node list of
		 * distributed table is not completely mapped by node list of replicated
		 * table, so go through standard planner.
		 */
		if (diff_nodelist)
			FreeExecNodes(&merged_en);
		else
		{
			merged_en->nodeList = list_copy(en2->nodeList);
			merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
		}
		return merged_en;
	}

	if (IsLocatorColumnDistributed(en1->baselocatortype) &&
			  IsLocatorReplicated(en2->baselocatortype))
	{
		List *diff_nodelist = NULL;
		/*
		 * Distributed/replicated join case.
		 * Node list of distributed table has to be included
		 * in node list of replicated table.
		 */
		diff_nodelist = list_difference_int(en1->nodeList, en2->nodeList);

		/*
		 * If the difference list is not empty, this means that node list of
		 * distributed table is not completely mapped by node list of replicated
		 * table, so go through standard planner.
		 */
		if (diff_nodelist)
			FreeExecNodes(&merged_en);
		else
		{
			merged_en->nodeList = list_copy(en1->nodeList);
			merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
		}
		return merged_en;
	}

	if (IsLocatorColumnDistributed(en1->baselocatortype) &&
			 IsLocatorColumnDistributed(en2->baselocatortype))
	{
		/*
		 * Distributed/distributed case
		 * If the caller has suggested that this is an equi-join between two
		 * distributed results, check if both are distributed by the same
		 * distribution strategy, and have the same nodes in the distribution
		 * node list. The caller should have made sure that distribution column
		 * type is same.
		 */
		if (merge_dist_equijoin &&
			en1->baselocatortype == en2->baselocatortype &&
			!list_difference_int(en1->nodeList, en2->nodeList) &&
			!list_difference_int(en2->nodeList, en1->nodeList))
		{
			merged_en->nodeList = list_copy(en1->nodeList);
			merged_en->baselocatortype = en1->baselocatortype;
		}
		else if (list_length(en1->nodeList) == 1 && list_length(en2->nodeList) == 1)
		{
			merged_en->nodeList = list_intersection_int(en1->nodeList,
														en2->nodeList);
			merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
		}
		else
			FreeExecNodes(&merged_en);
		return merged_en;
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Postgres-XC does not support this distribution type yet"),
			 errdetail("The feature is not currently supported")));

	/* Keep compiler happy */
	return NULL;
}

static void
pgxc_FQS_find_datanodes(FQS_context *fqs_context)
{
	Query *query = fqs_context->fqsc_query;
	ListCell   *rt;
	ExecNodes	*exec_nodes = NULL;
	bool		canShip = true;
	Index		varno = 0;

	/*
	 * For every range table entry,
	 * 1. Find out the datanodes needed for that range table
	 * 2. Merge these datanodes with the already available datanodes
	 * 3. If the merge is unsuccessful, we can not ship this query directly to
	 *    the datanode/s
	 */
	foreach(rt, query->rtable)
	{
		RangeTblEntry	*rte = (RangeTblEntry *) lfirst(rt);
		Oid				distcol_type;	/* TODO mostly this is not needed */
		Relids			dist_varnos;

		varno++;
		switch (rte->rtekind)
		{
			case RTE_RELATION:
			{
				ExecNodes	*rel_exec_nodes;
				ExecNodes	*tmp_en;
				bool		merge_dist_equijoin = false;
				/*
				 * In case of inheritance, child tables can have completely different
				 * datanode distribution than parent. To handle inheritance we need
				 * to merge the datanodes of the children table as well. The inheritance
				 * is resolved during planning(?), so we may not have the RTEs of the
				 * children here. Also, the exact method of merging datanodes of the
				 * children is not known yet. So, when inheritance is requested, query
				 * can not be shipped.
				 */
				if (rte->inh)
				{
					/*
					 * See prologue of has_subclass, we might miss on the
					 * optimization because has_subclass can return true
					 * even if there aren't any subclasses, but it's ok
					 */
					if (has_subclass(rte->relid))
					{
						canShip = false;
						break;
					}
				}

				if (rte->relkind != RELKIND_RELATION)
				{
					canShip = false;
					break;
				}
				rel_exec_nodes = pgxc_FQS_get_relation_nodes(rte,varno, query);
				if (!rel_exec_nodes)
				{
					/*
					 * No information about the location of relation in XC,
					 * a local table OR system catalog. The query can not be
					 * pushed.
					 */
					canShip = false;
					break;
				}
				if (varno == 1)
				{
					if (IsLocatorColumnDistributed(rel_exec_nodes->baselocatortype))
					{
						RelationLocInfo *rel_loc_info = GetRelationLocInfo(rte->relid);
						distcol_type = get_atttype(rte->relid,
													rel_loc_info->partAttrNum);
						dist_varnos = bms_make_singleton(varno);
					}
					else
					{
						distcol_type = InvalidOid;
						dist_varnos = NULL;
					}
				}
				if (exec_nodes &&
					IsLocatorDistributedByValue(exec_nodes->baselocatortype) &&
					OidIsValid(distcol_type) && bms_num_members(dist_varnos) > 0 &&
					exec_nodes->baselocatortype == rel_exec_nodes->baselocatortype)
				{
					/*
					 * If the already reduced JOINs is distributed the same way
					 * as the current relation, check if there exists an
					 * equi-join condition between the relations and the data type
					 * of distribution column involved is same for both the
					 * relations
					 */
					if (pgxc_qual_hash_dist_equijoin(dist_varnos,
														bms_make_singleton(varno),
														distcol_type,
														query->jointree->quals,
														query->rtable))
							merge_dist_equijoin = true;
				}

				/* Save the current exec_nodes to be freed later */
				tmp_en = exec_nodes;
				exec_nodes = pgxc_merge_exec_nodes(exec_nodes, rel_exec_nodes,
													merge_dist_equijoin,
													false);
				/*
				 * The JOIN is equijoin between distributed tables, and we could
				 * obtain the nodelist for pushing this JOIN, so add the current
				 * relation to the list of relations already JOINed in the same
				 * fashion.
				 */
				if (exec_nodes && merge_dist_equijoin)
					dist_varnos = bms_add_member(dist_varnos, varno);
				FreeExecNodes(&tmp_en);
			}
				break;

			case RTE_JOIN:
				/* Is information here useful in some or other way? */
				break;
			case RTE_CTE:
			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_VALUES:
			default:
				canShip = false;
		}

		if (!canShip || !exec_nodes)
			break;
	}

	/*
	 * If we didn't find the datanodes to ship the query to, we shouldn't ship
	 * the query :)
	 */
	if (!exec_nodes || !(exec_nodes->nodeList || exec_nodes->en_expr))
		canShip = false;

	if (canShip)
	{
		/*
		 * If relations involved in the query are such that ultimate JOIN is
		 * replicated JOIN, choose only one of them. If one of them is a
		 * preferred node choose that one, otherwise choose the first one.
		 */
		if (IsLocatorReplicated(exec_nodes->baselocatortype) &&
			exec_nodes->accesstype == RELATION_ACCESS_READ)
		{
			List		*tmp_list = exec_nodes->nodeList;
			ListCell	*item;
			int			nodeid = -1;
			foreach(item, exec_nodes->nodeList)
			{
				int cnt_nodes;
				for (cnt_nodes = 0;
						cnt_nodes < num_preferred_data_nodes && nodeid < 0;
						cnt_nodes++)
				{
					if (PGXCNodeGetNodeId(preferred_data_node[cnt_nodes],
										  PGXC_NODE_DATANODE) == lfirst_int(item))
						nodeid = lfirst_int(item);
				}
				if (nodeid >= 0)
					break;
			}
			if (nodeid < 0)
				exec_nodes->nodeList = list_make1_int(linitial_int(exec_nodes->nodeList));
			else
				exec_nodes->nodeList = list_make1_int(nodeid);
			list_free(tmp_list);
		}
		fqs_context->fqsc_exec_nodes = exec_nodes;
	}
	else if (exec_nodes)
	{
		FreeExecNodes(&exec_nodes);
	}
	return;
}

static bool
pgxc_qual_hash_dist_equijoin(Relids varnos_1, Relids varnos_2, Oid distcol_type,
								Node *quals, List *rtable)
{
	List		*lquals;
	ListCell	*qcell;

	/*
	 * Make a copy of the argument bitmaps, it will be modified by
	 * bms_first_member().
	 */
	varnos_1 = bms_copy(varnos_1);
	varnos_2 = bms_copy(varnos_2);

	lquals = make_ands_implicit((Expr *)quals);
	foreach(qcell, lquals)
	{
		Expr *qual_expr = (Expr *)lfirst(qcell);
		OpExpr *op;
		Var *lvar;
		Var *rvar;

		if (!IsA(qual_expr, OpExpr))
			continue;
		op = (OpExpr *)qual_expr;
		/* If not a binary operator, it can not be '='. */
		if (list_length(op->args) != 2)
			continue;

		/*
		 * Check if both operands are Vars, if not check next expression */
		if (IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Var))
		{
			lvar = (Var *)linitial(op->args);
			rvar = (Var *)lsecond(op->args);
		}
		else
			continue;

		/*
		 * If the data types of both the columns are not same, continue. Hash
		 * and Modulo of a the same bytes will be same if the data types are
		 * same. So, only when the data types of the columns are same, we can
		 * ship a distributed JOIN to the datanodes
		 */
		if (exprType((Node *)lvar) != exprType((Node *)rvar))
			continue;

		/* if the vars do not correspond to the required varnos, continue. */
		if ((bms_is_member(lvar->varno, varnos_1) && bms_is_member(rvar->varno, varnos_2)) ||
			(bms_is_member(lvar->varno, varnos_2) && bms_is_member(rvar->varno, varnos_1)))
		{
			if (!VarAttrIsPartAttr(lvar, rtable) ||
				!VarAttrIsPartAttr(rvar, rtable))
				continue;
		}
		else
			continue;
		/*
		 * If the operator is not an assignment operator, check next
		 * constraint. An operator is an assignment operator if it's
		 * mergejoinable or hashjoinable. Beware that not every assignment
		 * operator is mergejoinable or hashjoinable, so we might leave some
		 * oportunity. But then we have to rely on the opname which may not
		 * be something we know to be equality operator as well.
		 */
		if (!op_mergejoinable(op->opno, exprType((Node *)lvar)) &&
			!op_hashjoinable(op->opno, exprType((Node *)lvar)))
			continue;
		/* Found equi-join condition on distribution columns */
		return true;
	}
	return false;
}

/*
 * pgxc_find_distcol_expr
 * Search through the quals provided and find out an expression which will give
 * us value of distribution column if exists in the quals. Say for a table
 * tab1 (val int, val2 int) distributed by hash(val), a query "SELECT * FROM
 * tab1 WHERE val = fn(x, y, z) and val2 = 3", fn(x,y,z) is the expression which
 * decides the distribution column value in the rows qualified by this query.
 * Hence return fn(x, y, z). But for a query "SELECT * FROM tab1 WHERE val =
 * fn(x, y, z) || val2 = 3", there is no expression which decides the values
 * distribution column val can take in the qualified rows. So, in such cases
 * this function returns NULL.
 */
Expr *
pgxc_find_distcol_expr(Index varno, PartAttrNumber partAttrNum,
												Node *quals)
{
	/* Convert the qualification into list of arguments of AND */
	List *lquals = make_ands_implicit((Expr *)quals);
	ListCell *qual_cell;
	/*
	 * For every ANDed expression, check if that expression is of the form
	 * <distribution_col> = <expr>. If so return expr.
	 */
	foreach(qual_cell, lquals)
	{
		Expr *qual_expr = (Expr *)lfirst(qual_cell);
		OpExpr *op;
		Expr *lexpr;
		Expr *rexpr;
		Var *var_expr;
		Expr *distcol_expr;

		if (!IsA(qual_expr, OpExpr))
			continue;
		op = (OpExpr *)qual_expr;
		/* If not a binary operator, it can not be '='. */
		if (list_length(op->args) != 2)
			continue;

		lexpr = linitial(op->args);
		rexpr = lsecond(op->args);

		/*
		 * If either of the operands is a RelabelType, extract the Var in the RelabelType.
		 * A RelabelType represents a "dummy" type coercion between two binary compatible datatypes.
		 * If we do not handle these then our optimization does not work in case of varchar
		 * For example if col is of type varchar and is the dist key then
		 * select * from vc_tab where col = 'abcdefghijklmnopqrstuvwxyz';
		 * should be shipped to one of the nodes only
		 */
		if (IsA(lexpr, RelabelType))
			lexpr = ((RelabelType*)lexpr)->arg;
		if (IsA(rexpr, RelabelType))
			rexpr = ((RelabelType*)rexpr)->arg;

		/*
		 * If either of the operands is a Var expression, assume the other
		 * one is distribution column expression. If none is Var check next
		 * qual.
		 */
		if (IsA(lexpr, Var))
		{
			var_expr = (Var *)lexpr;
			distcol_expr = rexpr;
		}
		else if (IsA(rexpr, Var))
		{
			var_expr = (Var *)rexpr;
			distcol_expr = lexpr;
		}
		else
			continue;
		/*
		 * If Var found is not the distribution column of required relation,
		 * check next qual
		 */
		if (var_expr->varno != varno || var_expr->varattno != partAttrNum)
			continue;
		/*
		 * If the operator is not an assignment operator, check next
		 * constraint. An operator is an assignment operator if it's
		 * mergejoinable or hashjoinable. Beware that not every assignment
		 * operator is mergejoinable or hashjoinable, so we might leave some
		 * oportunity. But then we have to rely on the opname which may not
		 * be something we know to be equality operator as well.
		 */
		if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
			!op_hashjoinable(op->opno, exprType((Node *)lexpr)))
			continue;
		/* Found the distribution column expression return it */
		return distcol_expr;
	}
	/* Exhausted all quals, but no distribution column expression */
	return NULL;
}

static bool VarAttrIsPartAttr(Var *var, List *rtable)
{
	RangeTblEntry   *rte = rt_fetch(var->varno, rtable);
	RelationLocInfo	*rel_loc_info;
	/* distribution column only applies to the relations */
	if (rte->rtekind != RTE_RELATION ||
		rte->relkind != RELKIND_RELATION)
		return false;
	rel_loc_info = GetRelationLocInfo(rte->relid);
	if (!rel_loc_info)
		return false;
	if (var->varattno == rel_loc_info->partAttrNum)
		return true;
	return false;
}
/*
 * pgxc_FQS_get_relation_nodes
 * For FQS return ExecNodes structure so as to decide which datanodes the query
 * should execute on. If it is possible to set the node list directly, set it.
 * Otherwise set the appropriate distribution column expression or relid in
 * ExecNodes structure.
 */
static ExecNodes *
pgxc_FQS_get_relation_nodes(RangeTblEntry *rte, Index varno, Query *query)
{
	CmdType command_type = query->commandType;
	bool for_update = query->rowMarks ? true : false;
	ExecNodes	*rel_exec_nodes;
	RelationAccessType rel_access;
	RelationLocInfo *rel_loc_info;
	Expr			*distcol_expr = NULL;
	Datum 			distcol_value;
	bool			distcol_isnull;
	Oid				distcol_type;

	Assert(rte == rt_fetch(varno, (query->rtable)));

	switch (command_type)
	{
		case CMD_SELECT:
			if (for_update)
				rel_access = RELATION_ACCESS_READ_FOR_UPDATE;
			else
				rel_access = RELATION_ACCESS_READ;
			break;

		case CMD_UPDATE:
		case CMD_DELETE:
			rel_access = RELATION_ACCESS_UPDATE;
			break;

		case CMD_INSERT:
			rel_access = RELATION_ACCESS_INSERT;
			break;

		default:
			/* should not happen, but */
			elog(ERROR, "Unrecognised command type %d", command_type);
			break;
	}

	rel_loc_info = GetRelationLocInfo(rte->relid);
	/* If we don't know about the distribution of relation, bail out */
	if (!rel_loc_info)
		return NULL;

	/*
	 * If the table distributed by value, check if we can reduce the datanodes
	 * by looking at the qualifiers for this relation.
	 * PGXC_FQS_TODO: for now, we apply node reduction only when there is only
	 * one relation involved in the query. If there are multiple distributed
	 * tables in the query and we apply node reduction here, we may fail to ship
	 * the entire join. We should some apply node reduction transitively.
	 */
	if (IsLocatorDistributedByValue(rel_loc_info->locatorType) &&
		list_length(query->rtable) == 1)
	{
		Oid		disttype = get_atttype(rte->relid, rel_loc_info->partAttrNum);
		int32	disttypmod = get_atttypmod(rte->relid, rel_loc_info->partAttrNum);
		distcol_expr = pgxc_find_distcol_expr(varno, rel_loc_info->partAttrNum,
												query->jointree->quals);
		/*
		 * If the type of expression used to find the datanode, is not same as
		 * the distribution column type, try casting it. This is same as what
		 * will happen in case of inserting that type of expression value as the
		 * distribution column value.
		 */
		if (distcol_expr)
		{
			distcol_expr = (Expr *)coerce_to_target_type(NULL,
													(Node *)distcol_expr,
													exprType((Node *)distcol_expr),
													disttype, disttypmod,
													COERCION_ASSIGNMENT,
													COERCE_IMPLICIT_CAST, -1);
			/*
			 * PGXC_FQS_TODO: We should set the bound parameters here, but we don't have
			 * PlannerInfo struct and we don't handle them right now.
			 * Even if constant expression mutator changes the expression, it will
			 * only simplify it, keeping the semantics same
			 */
			distcol_expr = (Expr *)eval_const_expressions(NULL,
															(Node *)distcol_expr);
		}
	}

	if (distcol_expr && IsA(distcol_expr, Const))
	{
		Const *const_expr = (Const *)distcol_expr;
		distcol_value = const_expr->constvalue;
		distcol_isnull = const_expr->constisnull;
		distcol_type = const_expr->consttype;
	}
	else
	{
		distcol_value = (Datum) 0;
		distcol_isnull = true;
		distcol_type = InvalidOid;
	}

	rel_exec_nodes = GetRelationNodes(rel_loc_info, distcol_value,
										distcol_isnull, distcol_type, rel_access);

	if (!rel_exec_nodes)
		return NULL;
	rel_exec_nodes->accesstype = rel_access;
	/*
	 * If we are reading a replicated table, pick all the nodes where it
	 * resides. If the query has JOIN, it helps picking up a matching set of
	 * datanodes for that JOIN. FQS planner will ultimately pick up one node if
	 * the JOIN is replicated.
	 */
	if (rel_access == RELATION_ACCESS_READ &&
		IsLocatorReplicated(rel_loc_info->locatorType))
	{
		list_free(rel_exec_nodes->nodeList);
		rel_exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
	}
	else if (rel_access == RELATION_ACCESS_INSERT &&
				IsLocatorDistributedByValue(rel_loc_info->locatorType))
	{
		ListCell *lc;
		TargetEntry *tle;
		/*
		 * If the INSERT is happening on a table distributed by value of a
		 * column, find out the
		 * expression for distribution column in the targetlist, and stick in
		 * in ExecNodes, and clear the nodelist. Execution will find
		 * out where to insert the row.
		 */
		/* It is a partitioned table, get value by looking in targetList */
		foreach(lc, query->targetList)
		{
			tle = (TargetEntry *) lfirst(lc);

			if (tle->resjunk)
				continue;
			if (strcmp(tle->resname, rel_loc_info->partAttrName) == 0)
				break;
		}
		/* Not found, bail out */
		if (!lc)
			return NULL;

		Assert(tle);
		/* We found the TargetEntry for the partition column */
		list_free(rel_exec_nodes->primarynodelist);
		rel_exec_nodes->primarynodelist = NULL;
		list_free(rel_exec_nodes->nodeList);
		rel_exec_nodes->nodeList = NULL;
		rel_exec_nodes->en_expr = tle->expr;
		rel_exec_nodes->en_relid = rel_loc_info->relid;
	}
	return rel_exec_nodes;
}
/*
 * pgxc_FQS_walker
 * walks the query/expression tree routed at the node passed in, gathering
 * information which will help decide whether the query to which this node
 * belongs is shippable to the datanodes.
 *
 * The function should try to walk the entire tree analysing each subquery for
 * shippability. If a subquery is shippable but not the whole query, we would be
 * able to create a RemoteQuery node for that subquery, shipping it to the
 * datanode.
 *
 * Return value of this function is governed by the same rules as
 * expression_tree_walker(), see prologue of that function for details.
 */
static bool
pgxc_FQS_walker(Node *node, FQS_context *fqs_context)
{
	if (node == NULL)
		return false;

	/* Below is the list of nodes that can appear in a query, examine each
	 * kind of node and find out under what conditions query with this node can
	 * be shippable. For each node, update the context (add fields if
	 * necessary) so that decision whether to FQS the query or not can be made.
	 */
	switch(nodeTag(node))
	{
		/* Constants are always shippable */
		case T_Const:
			break;

		/*
		 * For placeholder nodes the shippability of the node, depends upon the
		 * expression which they refer to. It will be checked separately, when
		 * that expression is encountered.
		 */
		case T_CaseTestExpr:
		case T_SortGroupClause:
		case T_TargetEntry:
			break;

		/*
		 * Nodes, which are shippable if the tree rooted under these nodes is
		 * shippable
		 */
		case T_List:
		case T_CoerceToDomainValue:
			/*
			 * PGXCTODO: mostly, CoerceToDomainValue node appears in DDLs,
			 * do we handle DDLs here?
			 */
		case T_FieldSelect:
		case T_ArrayRef:
		case T_RangeTblRef:
		case T_NamedArgExpr:
		case T_BoolExpr:
			/*
			 * PGXCTODO: we might need to take into account the kind of boolean
			 * operator we have in the quals and see if the corresponding
			 * function is immutable.
			 */
		case T_RelabelType:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_ConvertRowtypeExpr:
		case T_CaseExpr:
		case T_ArrayExpr:
		case T_RowExpr:
		case T_CollateExpr:
		case T_CoalesceExpr:
		case T_XmlExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_CoerceToDomain:
			break;

		case T_FieldStore:
			/*
			 * PostgreSQL deparsing logic does not handle the FieldStore
			 * for more than one fields (see processIndirection()). So, let's
			 * handle it through standard planner, where whole row will be
			 * constructed.
			 */
			pgxc_FQS_set_reason(fqs_context, FQS_UNSUPPORTED_EXPR);
			break;

		case T_SetToDefault:
			/*
			 * PGXCTODO: we should actually check whether the default value to
			 * be substituted is shippable to the datanode. Some cases like
			 * nextval() of a sequence can not be shipped to the datanode, hence
			 * for now default values can not be shipped to the datanodes
			 */
			pgxc_FQS_set_reason(fqs_context, FQS_UNSUPPORTED_EXPR);
			break;

		case T_Var:
		{
			Var	*var = (Var *)node;
			/*
			 * if a subquery references an upper level variable, that query is
			 * not shippable, if shipped alone.
			 */
			if (var->varlevelsup > fqs_context->fqsc_max_varlevelsup)
				fqs_context->fqsc_max_varlevelsup = var->varlevelsup;
		}
		break;

		case T_Param:
		{
			Param *param = (Param *)node;
			/*
			 * PARAM_EXEC params should not appear in the tree, since we are
			 * analysing before planning. In case, they appear, we do not know
			 * what to do! Other kinds of parameters can be shipped to the
			 * remote side.
			 */
			if (param->paramkind == PARAM_EXEC)
				pgxc_FQS_set_reason(fqs_context, FQS_UNSUPPORTED_EXPR);
		}
		break;

		case T_CurrentOfExpr:
		{
			/*
			 * Ideally we should not see CurrentOf expression here, it
			 * should have been replaced by the CTID = ? expression. But
			 * still, no harm in shipping it as is.
			 */
		}
		break;

		case T_Aggref:
		{
			/*
			 * An aggregate is completely shippable to the datanode, if the
			 * whole group resides on that datanode. This will be clear when
			 * we see the GROUP BY clause.
			 * Query::hasAggs will tell that the query has aggregates.
			 * agglevelsup is minimum of variable's varlevelsup, so we will
			 * set the fqsc_max_varlevelsup when we reach the appropriate
			 * VARs in the tree.
			 * Hence nothing to set here.
			 */
		}
		break;

		case T_FuncExpr:
		{
			FuncExpr	*funcexpr = (FuncExpr *)node;
			/*
			 * PGXC_FQS_TODO: it's too restrictive not to ship non-immutable
			 * functions to the datanode. We need a better way to see what
			 * can be shipped to the datanode and what can not be.
			 */
			if (!is_immutable_func(funcexpr->funcid))
				pgxc_FQS_set_reason(fqs_context, FQS_UNSHIPPABLE_EXPR);
		}
		break;

		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
		{
			/*
			 * All of these three are structurally equivalent to OpExpr, so
			 * cast the node to OpExpr and check if the operator function is
			 * immutable. See PGXC_FQS_TODO item for FuncExpr.
			 */
			OpExpr *op_expr = (OpExpr *)node;
			Oid		opfuncid = OidIsValid(op_expr->opfuncid) ?
								op_expr->opfuncid : get_opcode(op_expr->opno);
			if (!OidIsValid(opfuncid) || !is_immutable_func(opfuncid))
				pgxc_FQS_set_reason(fqs_context, FQS_UNSHIPPABLE_EXPR);
		}
		break;

		case T_ScalarArrayOpExpr:
		{
			/*
			 * Check if the operator function is shippable to the datanode
			 * PGXC_FQS_TODO: see immutability note for FuncExpr above
			 */
			ScalarArrayOpExpr *sao_expr = (ScalarArrayOpExpr *)node;
			Oid		opfuncid = OidIsValid(sao_expr->opfuncid) ?
								sao_expr->opfuncid : get_opcode(sao_expr->opno);
			if (!OidIsValid(opfuncid) || !is_immutable_func(opfuncid))
				pgxc_FQS_set_reason(fqs_context, FQS_UNSHIPPABLE_EXPR);
		}
		break;

		case T_RowCompareExpr:
		case T_MinMaxExpr:
		{
			/*
			 * PGXCTODO should we be checking the comparision operator
			 * functions as well, as we did for OpExpr OR that check is
			 * unnecessary. Operator functions are always shippable?
			 * Otherwise this node should be treated similar to other
			 * "shell" nodes.
			 */
		}
		break;

		case T_Query:
		{
			Query *query = (Query *)node;

			if (query->hasRecursive || 	query->intoClause)
				pgxc_FQS_set_reason(fqs_context, FQS_UNSUPPORTED_EXPR);
			/*
			 * If the query needs coordinator for evaluation or the query can be
			 * completed on coordinator itself, we don't ship it to the datanode
			 */
			if (pgxc_query_needs_coord(query))
				pgxc_FQS_set_reason(fqs_context, FQS_NEEDS_COORD);

			/* PGXC_FQS_TODO: It should be possible to look at the Query and find out
			 * whether it can be completely evaluated on the datanode just like SELECT
			 * queries. But we need to be careful while finding out the datanodes to
			 * execute the query on, esp. for the result relations. If one happens to
			 * remove/change this restriction, make sure you change
			 * pgxc_FQS_get_relation_nodes appropriately.
			 * For now DMLs with single rtable entry are candidates for FQS
			 */
			if (query->commandType != CMD_SELECT && list_length(query->rtable) > 1)
				pgxc_FQS_set_reason(fqs_context, FQS_UNSUPPORTED_EXPR);

			/*
			 * In following conditions query is shippable when there is only one
			 * datanode involved
			 * 1. the query has aggregagtes
			 * 2. the query has window functions
			 * 3. the query has ORDER BY clause
			 * 4. the query has Distinct clause
			 * 5. the query has limit and offset clause
			 *
			 * PGXC_FQS_TODO: Condition 1 above is really dependent upon the GROUP BY clause. If
			 * all rows in each group reside on the same datanode, aggregates can be
			 * evaluated on that datanode, thus condition 1 is has aggregates & the rows
			 * in any group reside on multiple datanodes.
			 * PGXC_FQS_TODO: Condition 2 above is really dependent upon whether the distinct
			 * clause has distribution column in it. If the distinct clause has
			 * distribution column in it, we can ship DISTINCT clause to the datanodes.
			 */
			if (query->hasAggs || query->hasWindowFuncs || query->sortClause ||
				query->distinctClause || query->groupClause || query->havingQual ||
				query->limitOffset || query->limitCount)
				pgxc_FQS_set_reason(fqs_context, FQS_SINGLENODE_EXPR);

			/* walk the entire query tree to analyse the query */
			if (query_tree_walker(query, pgxc_FQS_walker, fqs_context, 0))
				return true;

			/*
			 * PGXC_FQS_TODO:
			 * There is a subquery in this query, which references Vars in the upper
			 * query. For now stop shipping such queries. We should get rid of this
			 * condition.
			 */
			if (fqs_context->fqsc_max_varlevelsup != 0)
				pgxc_FQS_set_reason(fqs_context, FQS_VARLEVEL);

			/*
			 * Walk the RangeTableEntries of the query and find the
			 * datanodes needed for evaluating this query
			 */
			pgxc_FQS_find_datanodes(fqs_context);
		}
		break;

		case T_FromExpr:
		{
			/*
			 * We will be examining the range table entries separately and
			 * Join expressions are not candidate for FQS, so nothing to be
			 * done for now
			 */
		}
		break;

		case T_WindowFunc:
		{
			WindowFunc *winf = (WindowFunc *)node;
			/*
			 * A window function can be evaluated on a datanode if there is
			 * only one datanode involved. This can be checked outside the
			 * walker by looking at Query::hasWindowFuncs.
			 */
			if (!is_immutable_func(winf->winfnoid))
				pgxc_FQS_set_reason(fqs_context, FQS_UNSHIPPABLE_EXPR);
		}
		break;

		/* Nodes which do not need to be examined but worth some explanation */
		case T_WindowClause:
				/*
				 * A window function can be evaluated on a datanode if there is
				 * only one datanode involved. This can be checked outside the
				 * walker by looking at Query::hasWindowFuncs.
				 */
				/* FALL THROUGH */
		case T_JoinExpr:
				/*
				 * The compatibility of joining ranges will be deduced while
				 * examining the range table of the query. Nothing to do here
				 */
			break;

		case T_SubLink:
		{
			SubLink		*sublink = (SubLink *)node;
			ExecNodes	*sublink_en;
			/*
			 * Walk the query and find the nodes where the query should be
			 * executed and node distribution. Merge this with the existing
			 * node list obtained for other subqueries. If merging fails, we
			 * can not ship the whole query.
			 */
			if (IsA(sublink->subselect, Query))
				sublink_en = pgxc_is_query_shippable((Query *)(sublink->subselect),
														fqs_context->fqsc_query_level);
			else
				sublink_en = NULL;

			/* PGXCTODO free the old fqsc_subquery_en. */
			/* If we already know that this query does not have a set of nodes
			 * to evaluate on, don't bother to merge again.
			 */
			if (!pgxc_FQS_test_reason(fqs_context, FQS_NO_NODES))
			{
				fqs_context->fqsc_subquery_en = pgxc_merge_exec_nodes(sublink_en,
																	fqs_context->fqsc_subquery_en,
																	false,
																	true);
				if (!fqs_context->fqsc_subquery_en)
					pgxc_FQS_set_reason(fqs_context, FQS_NO_NODES);
			}
		}
		break;

		case T_SubPlan:
		case T_AlternativeSubPlan:
		case T_CommonTableExpr:
		case T_SetOperationStmt:
		case T_PlaceHolderVar:
		case T_AppendRelInfo:
		case T_PlaceHolderInfo:
		{
			/* PGXCTODO: till we exhaust this list */
			pgxc_FQS_set_reason(fqs_context, FQS_UNSUPPORTED_EXPR);
		}
		break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
	return expression_tree_walker(node, pgxc_FQS_walker, (void *)fqs_context);
}

/*
 * See if we can reduce the passed in RemoteQuery nodes to a single step.
 *
 * We need to check when we can further collapse already collapsed nodes.
 * We cannot always collapse- we do not want to allow a replicated table
 * to be used twice. That is if we have
 *
 *     partitioned_1 -- replicated -- partitioned_2
 *
 * partitioned_1 and partitioned_2 cannot (usually) be safely joined only
 * locally.
 * We can do this by checking (may need tracking) what type it is,
 * and looking at context->conditions->replicated_joins
 *
 * The following cases are possible, and whether or not it is ok
 * to reduce.
 *
 * If the join between the two RemoteQuery nodes is replicated
 *
 *      Node 1            Node 2
 * rep-part folded   rep-part  folded    ok to reduce?
 *    0       0         0         1       1
 *    0       0         1         1       1
 *    0       1         0         1       1
 *    0       1         1         1       1
 *    1       1         1         1       0
 *
 *
 * If the join between the two RemoteQuery nodes is replicated - partitioned
 *
 *      Node 1            Node 2
 * rep-part folded   rep-part  folded    ok to reduce?
 *    0       0         0         1       1
 *    0       0         1         1       0
 *    0       1         0         1       1
 *    0       1         1         1       0
 *    1       1         1         1       0
 *
 *
 * If the join between the two RemoteQuery nodes is partitioned - partitioned
 * it is always reducibile safely,
 *
 * RemoteQuery *innernode  - the inner node
 * RemoteQuery *outernode  - the outer node
 * List *rtable_list	   - rtables
 * JoinPath *join_path	   - used to examine join restrictions
 * PGXCJoinInfo *join_info - contains info about the join reduction
 * join_info->partitioned_replicated  is set to true if we have a partitioned-replicated
 *                          join. We want to use replicated tables with non-replicated
 *                          tables ony once. Only use this value if this function
 *							returns true.
 */
ExecNodes *
IsJoinReducible(RemoteQuery *innernode, RemoteQuery *outernode, Relids in_relids, Relids out_relids,
				Join *join, JoinPath *join_path, List *rtables)
{
	ExecNodes 	*join_exec_nodes;
	bool		merge_dist_equijoin = false;
	bool		merge_replicated_only;
	ListCell	*cell;
	ExecNodes	*inner_en = innernode->exec_nodes;
	ExecNodes	*outer_en = outernode->exec_nodes;
	List		*quals = join->joinqual;

	/*
	 * When join type is other than INNER, we will get the unmatched rows on
	 * either side. The result will be correct only in case both the sides of
	 * join are replicated. In case one of the sides is replicated, and the
	 * unmatched results are not coming from that side, it might be possible to
	 * ship such join, but this needs to be validated from correctness
	 * perspective.
	 */
	merge_replicated_only = (join->jointype != JOIN_INNER);

	/*
	 * If both the relations are distributed with similar distribution strategy
	 * walk through the restriction info for this JOIN to find if there is an
	 * equality condition on the distributed columns of both the relations. In
	 * such case, we can reduce the JOIN if the distribution nodelist is also
	 * same.
	 */
	if (IsLocatorDistributedByValue(inner_en->baselocatortype) &&
		inner_en->baselocatortype == outer_en->baselocatortype &&
		!merge_replicated_only)
	{
		foreach(cell, quals)
		{
			Node	*qual = (Node *)lfirst(cell);
			if (pgxc_qual_hash_dist_equijoin(in_relids, out_relids, InvalidOid,
												qual, rtables))
			{
				merge_dist_equijoin = true;
				break;
			}
		}
	}
	/*
	 * If the ExecNodes of inner and outer nodes can be merged, the JOIN is
	 * shippable
	 * PGXCTODO: Can we take into consideration the JOIN conditions to optimize
	 * further?
	 */
	join_exec_nodes = pgxc_merge_exec_nodes(inner_en, outer_en,
											merge_dist_equijoin,
											merge_replicated_only);
	return join_exec_nodes;
}

/*
 * validate whether partition column of a table is being updated
 */
static void
validate_part_col_updatable(const Query *query)
{
	RangeTblEntry *rte;
	RelationLocInfo *rel_loc_info;
	ListCell *lc;

	/* Make sure there is one table at least */
	if (query->rtable == NULL)
		return;

	rte = (RangeTblEntry *) list_nth(query->rtable, query->resultRelation - 1);


	if (rte != NULL && rte->relkind != RELKIND_RELATION)
		/* Bad relation type */
		return;

	/* See if we have the partitioned case. */
	rel_loc_info = GetRelationLocInfo(rte->relid);

	if (!rel_loc_info)
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				(errmsg("Could not find relation for oid = %d", rte->relid))));


	/* Only LOCATOR_TYPE_HASH & LOCATOR_TYPE_MODULO should be checked */
	if ( (rel_loc_info->partAttrName != NULL) &&
		( (rel_loc_info->locatorType == LOCATOR_TYPE_HASH) || (rel_loc_info->locatorType == LOCATOR_TYPE_MODULO) ) )
	{
		/* It is a partitioned table, check partition column in targetList */
		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (tle->resjunk)
				continue;

			/*
			 * See if we have a constant expression comparing against the
			 * designated partitioned column
			 */
			if (strcmp(tle->resname, rel_loc_info->partAttrName) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
			  			(errmsg("Partition column can't be updated in current version"))));
		}
	}
}

/*
 * AddRemoteQueryNode
 *
 * Add a Remote Query node to launch on Datanodes.
 * This can only be done for a query a Top Level to avoid
 * duplicated queries on Datanodes.
 */
List *
AddRemoteQueryNode(List *stmts, const char *queryString, RemoteQueryExecType remoteExecType, bool is_temp)
{
	List *result = stmts;

	/* If node is appplied on EXEC_ON_NONE, simply return the list unchanged */
	if (remoteExecType == EXEC_ON_NONE)
		return result;

	/* Only a remote Coordinator is allowed to send a query to backend nodes */
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		RemoteQuery *step = makeNode(RemoteQuery);
		step->combine_type = COMBINE_TYPE_SAME;
		step->sql_statement = (char *) queryString;
		step->exec_type = remoteExecType;
		step->is_temp = is_temp;
		result = lappend(result, step);
	}

	return result;
}

/*
 * pgxc_query_contains_temp_tables
 *
 * Check if there is any temporary object used in given list of queries.
 */
bool
pgxc_query_contains_temp_tables(List *queries)
{
	ListCell   *elt;

	foreach(elt, queries)
	{
		Query *query = (Query *) lfirst(elt);

		if (!query)
			continue;

		switch(query->commandType)
		{
			case CMD_SELECT:
			case CMD_UPDATE:
			case CMD_INSERT:
			case CMD_DELETE:
				if (contains_temp_tables(query->rtable))
					return true;
			default:
				break;
		}
	}

	return false;
}
