/*-------------------------------------------------------------------------
 *
 * pgxcplan.c
 *
 *	  Functions for generating a PGXC plans.
 *
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/backend/optimizer/plan/pgxcplan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#include "pgxc/postgresql_fdw.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static void pgxc_locate_grouping_columns(PlannerInfo *root, List *tlist,
											AttrNumber *grpColIdx);
static List *pgxc_process_grouping_targetlist(PlannerInfo *root,
												List **local_tlist);
static List *pgxc_process_having_clause(PlannerInfo *root, List *remote_tlist,
												Node *havingQual, List **local_qual,
												List **remote_qual, bool *reduce_plan);
static Expr *pgxc_set_en_expr(Oid tableoid, Index resultRelationIndex);
static int pgxc_count_rowmarks_entries(List *rowMarks);
static Oid *pgxc_build_rowmark_entries(List *rowMarks, List *rtable, Oid *types, int prepparams, int totparams);
static List *pgxc_separate_quals(List *quals, List **local_quals);
static Query *pgxc_build_shippable_query_recurse(PlannerInfo *root,
													RemoteQueryPath *rqpath,
													List **unshippable_quals,
													List **rep_tlist);
static RemoteQuery *make_remotequery(List *qptlist, List *qpqual,
										Index scanrelid);
static RangeTblEntry *make_dummy_remote_rte(char *relname, Alias *alias);

/*
 * pgxc_separate_quals
 * Separate the quals into shippable and unshippable quals. Return the shippable
 * quals as return value and unshippable quals as local_quals
 */
static List *
pgxc_separate_quals(List *quals, List **unshippabl_quals)
{
	ListCell	*l;
	List		*shippabl_quals = NIL;

	*unshippabl_quals = NIL;
	foreach(l, quals)
	{
		Expr *clause = lfirst(l);

		if (pgxc_is_expr_shippable(clause, NULL))
			shippabl_quals = lappend(shippabl_quals, clause);
		else
			*unshippabl_quals = lappend(*unshippabl_quals, clause);
	}
	return shippabl_quals;
}

/*
 * pgxc_build_shippable_tlist
 * Scan the target list expected from this relation, to see if there are any
 * expressions which are not shippable. If a member in the target list has
 * unshippable expression, we can not ship the expression corresponding to
 * that member as is to the datanode. Hence, pull the vars in that
 * expression and add those vars to the target list to be shipped to the
 * datanode. If no such expression exists, we can add the member to the
 * datanode target list as is.
 * We need the values of VARs in the quals to be applied on coordinator. Add
 * those in the target list for the datanode.
 */
static List *
pgxc_build_shippable_tlist(List *tlist, List *unshippabl_quals)
{
	ListCell	*lcell;
	List		*remote_tlist = NIL;
	List		*tmp_rtlist = NIL;

	/*
	 * Add all the shippable members as they are to the target list being built,
	 * and add the Var nodes from the unshippable members.
	 */
	foreach(lcell, tlist)
	{
		TargetEntry *tle = lfirst(lcell);
		Expr		*expr;

		if (!IsA(tle, TargetEntry))
			elog(ERROR, "Expected TargetEntry node, but got node with type %d",
							nodeTag(tle));
		expr = tle->expr;
		/*
		 * It's crucial that we pass TargetEntry to the
		 * pgxc_is_expr_shippable(). Helps in detecting top level RowExpr, which
		 * are unshippable
		 */
		if (pgxc_is_expr_shippable((Expr *)tle, NULL))
			tmp_rtlist = lappend(tmp_rtlist, expr);
		else
			tmp_rtlist = list_concat(tmp_rtlist, pull_var_clause((Node *)expr, 
														PVC_REJECT_AGGREGATES,
														PVC_RECURSE_PLACEHOLDERS));
	}
	remote_tlist = add_to_flat_tlist(remote_tlist, tmp_rtlist);

	/*
	 * The passed in quals are already checked for shippability, so just add the
	 * vars from those clauses to the targetlist being built.
	 */
	remote_tlist = add_to_flat_tlist(remote_tlist, pull_var_clause((Node *)unshippabl_quals,
															PVC_RECURSE_AGGREGATES,
															PVC_RECURSE_PLACEHOLDERS));
											
	return remote_tlist;
}

/*
 * pgxc_build_shippable_query_baserel
 * builds a shippable Query structure for a base relation. Since there is only
 * one relation involved in this Query, all the Vars need to be restamped. Hence
 * we need a mapping of actuals Vars to those in the Query being built. This
 * mapping is provided by rep_tlist.
 */
static Query *
pgxc_build_shippable_query_baserel(PlannerInfo *root, RemoteQueryPath *rqpath,
									List **unshippable_quals, List **rep_tlist)
{
	RelOptInfo	*baserel = rqpath->path.parent;
	List		*tlist;
	List		*scan_clauses;
	List		*shippable_quals;
	Query		*query;
	RangeTblEntry *rte = rt_fetch(baserel->relid, root->parse->rtable);
	ListCell	*lcell;
	RangeTblRef	*rtr;

	if ((baserel->reloptkind != RELOPT_BASEREL && 
			baserel->reloptkind != RELOPT_OTHER_MEMBER_REL) ||
		baserel->rtekind != RTE_RELATION)
		elog(ERROR, "can not generate shippable query for base relations of type other than plain tables");
	
	*rep_tlist = NIL;
	*unshippable_quals = NIL;
	/*
	 * The RTE needs to be marked to be included in FROM clause and to prepend
	 * it with ONLY (so that it does not include results from children.
	 */
	rte->inh = false;
	rte->inFromCl = true;

	/*
	 * Get the scan clauses for this relation and separate them into shippable
	 * and non-shippable quals. We need to include the pseudoconstant quals as
	 * well.
	 */
	scan_clauses = pgxc_order_qual_clauses(root, baserel->baserestrictinfo);
	scan_clauses = list_concat(extract_actual_clauses(scan_clauses, false),
								extract_actual_clauses(scan_clauses, true));
	shippable_quals = pgxc_separate_quals(scan_clauses, unshippable_quals);
	shippable_quals = copyObject(shippable_quals);

	/*
	 * Build the target list for this relation. We included only the Vars to
	 * start with.
	 */
	tlist = pgxc_build_relation_tlist(baserel);
	tlist = pull_var_clause((Node *)tlist, PVC_REJECT_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
	tlist = list_concat(tlist, pull_var_clause((Node *)*unshippable_quals,
												PVC_REJECT_AGGREGATES,
												PVC_RECURSE_PLACEHOLDERS));

	/*
	 * The target list that we built just now represents the result of the
	 * query being built. This serves as a reference for building the 
	 * encapsulating queries. So, copy it. We then modify the Vars to change
	 * their varno with 1 for the query being built
	 */
	*rep_tlist = add_to_flat_tlist(NIL, tlist);

	/* Build the query */
	query = makeNode(Query);
	query->commandType = CMD_SELECT;
	query->rtable = list_make1(rte);
	query->targetList = copyObject(*rep_tlist); 
	query->jointree = (FromExpr *)makeNode(FromExpr);

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = list_length(query->rtable);
	/* There can be only one relation */
	Assert(rtr->rtindex == 1);

	query->jointree->fromlist = list_make1(rtr);
	query->jointree->quals = (Node *)make_ands_explicit(shippable_quals);

	/* Collect the Var nodes in the entire query and restamp the varnos */
	tlist = list_concat(pull_var_clause((Node *)query->targetList, PVC_REJECT_AGGREGATES,
															PVC_RECURSE_PLACEHOLDERS),
						pull_var_clause((Node *)query->jointree->quals,
											PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS));
	foreach (lcell, tlist)
	{
		Var *var = lfirst(lcell);
		Assert(IsA(var, Var));
		if (var->varno != baserel->relid)
			elog(ERROR, "can not handle multiple relations in a single baserel");
		var->varno = rtr->rtindex;
	}

	return query;
}

/*
 * pgxc_generate_colnames
 * Given a prefix and number of names to generate, the function returns a list
 * of names of the form prefix_{1,...,num_cols}
 */
static List *
pgxc_generate_colnames(char *prefix, int num_cols)
{
	List	*colnames = NIL;
	StringInfo	colname = makeStringInfo();
	int		cnt_col;
	for (cnt_col = 0; cnt_col < num_cols; cnt_col++)
	{
		appendStringInfo(colname, "%s_%d", prefix, cnt_col + 1);
		colnames = lappend(colnames, makeString(pstrdup(colname->data)));
		resetStringInfo(colname);
	}
	pfree(colname->data);
	pfree(colname);
	return colnames;
}

/*
 * pgxc_build_shippable_query_jointree
 * builds a shippable Query structure for a join relation. Since there are only
 * two relations involved in this Query, all the Vars need to be restamped. Hence
 * we need a mapping of actuals Vars to those in the Query being built. This
 * mapping is provided by rep_tlist.
 */
static Query *
pgxc_build_shippable_query_jointree(PlannerInfo *root, RemoteQueryPath *rqpath,
									List **unshippable_quals, List **rep_tlist)
{
	/* Variables for the part of the Query representing the JOIN */
	Query			*join_query;
	FromExpr		*from_expr;
	List			*rtable = NIL;
	List			*tlist;
	JoinExpr		*join_expr;
	List			*join_clauses;
	List			*other_clauses;
	List			*varlist;
	/* Variables for the left side of the JOIN */
	Query			*left_query;
	List			*left_us_quals;
	List			*left_rep_tlist;
	RangeTblEntry	*left_rte;
	Alias			*left_alias;
	List			*left_colnames;
	char			*left_aname = "l";	/* For the time being! Do we really care
										 * about unique alias names across the
										 * tree
										 */
	RangeTblRef		*left_rtr;
	/* Variables for the right side of the JOIN */
	Query			*right_query;
	List			*right_us_quals;
	List			*right_rep_tlist;
	RangeTblEntry	*right_rte;
	Alias 			*right_alias;
	List			*right_colnames;
	char			*right_aname = "r";
	RangeTblRef		*right_rtr;
	/* Miscellaneous variables */
	ListCell		*lcell;
	
	if (!rqpath->leftpath || !rqpath->rightpath)
		elog(ERROR, "a join relation path should have both left and right paths");

	/* 
	 * Build the query representing the left side of JOIN and add corresponding
	 * RTE with proper aliases
	 */
	left_query = pgxc_build_shippable_query_recurse(root, rqpath->leftpath,
													&left_us_quals,
													&left_rep_tlist);
	left_colnames = pgxc_generate_colnames("a", list_length(left_rep_tlist));  
	left_alias = makeAlias(left_aname, left_colnames);
	left_rte = addRangeTableEntryForSubquery(NULL, left_query, left_alias, true);
	rtable = lappend(rtable, left_rte); 
	left_rtr = makeNode(RangeTblRef);
	left_rtr->rtindex = list_length(rtable);
	/* 
	 * Build the query representing the right side of JOIN and add corresponding
	 * RTE with proper aliases
	 */
	right_query = pgxc_build_shippable_query_recurse(root, rqpath->rightpath,
													&right_us_quals,
													&right_rep_tlist);
	right_colnames = pgxc_generate_colnames("a", list_length(right_rep_tlist));
	right_alias = makeAlias(right_aname, right_colnames);
	right_rte = addRangeTableEntryForSubquery(NULL, right_query, right_alias,
												true);
	rtable = lappend(rtable, right_rte); 
	right_rtr = makeNode(RangeTblRef);
	right_rtr->rtindex = list_length(rtable);

	if (left_us_quals || right_us_quals)
		elog(ERROR, "unexpected unshippable quals in JOIN tree");

	/*
	 * Prepare quals to be included in the query when they are shippable, return
	 * back the unshippable quals. We need to restamp the Vars in the clauses
	 * to match the JOINing queries, so make a copy of those.
	 */
	extract_actual_join_clauses(rqpath->join_restrictlist, &join_clauses,
									&other_clauses);
	if (!pgxc_is_expr_shippable((Expr *)join_clauses, false))
		elog(ERROR, "join with unshippable join clauses can not be shipped");
	join_clauses = copyObject(join_clauses);
	other_clauses = list_concat(other_clauses,
								extract_actual_clauses(rqpath->join_restrictlist,
														true));
	other_clauses = pgxc_separate_quals(other_clauses, unshippable_quals);
	other_clauses = copyObject(other_clauses);

	/* 
	 * Build the targetlist for this relation and also the targetlist
	 * representing the query targetlist. The representative target list is in
	 * the form that rest of the plan can understand. The Vars in the JOIN Query
	 * targetlist need to be restamped with the right varno (left or right) and
	 * varattno to match the columns of the JOINing queries.
	 */
	tlist = pgxc_build_relation_tlist(rqpath->path.parent);
	varlist = list_concat(pull_var_clause((Node *)tlist, PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS),
						pull_var_clause((Node *)*unshippable_quals, PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS));
	*rep_tlist = add_to_flat_tlist(NIL, varlist);
	list_free(varlist);
	tlist = copyObject(*rep_tlist);
	varlist = list_concat(pull_var_clause((Node *)tlist, PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS),
							pull_var_clause((Node *)join_clauses,
												PVC_REJECT_AGGREGATES,
												PVC_RECURSE_PLACEHOLDERS));
	varlist = list_concat(varlist, pull_var_clause((Node *)other_clauses,
													PVC_REJECT_AGGREGATES,
													PVC_RECURSE_PLACEHOLDERS));
	foreach (lcell, varlist)
	{
		Var	*var = lfirst(lcell);
		TargetEntry *tle;
		Assert(IsA(var, Var));

		tle = tlist_member((Node *)var, left_rep_tlist);
		if (tle)
		{
			var->varno = left_rtr->rtindex;
			var->varattno = tle->resno;
		}
		else
		{
			tle = tlist_member((Node *)var, right_rep_tlist);
			if (tle)
			{
				var->varno = right_rtr->rtindex;
				var->varattno = tle->resno;
			}
			else
			{
				elog(ERROR, "can not find var with varno = %d and varattno = %d",
								var->varno, var->varattno);
			}
		}
	}
	list_free(varlist);

	/* Build the Join expression with the above left and right queries */
	join_expr = makeNode(JoinExpr);
	join_expr->jointype = rqpath->jointype;
	join_expr->larg = (Node *)left_rtr;
	join_expr->rarg = (Node *)right_rtr;
	join_expr->quals = (Node *)make_ands_explicit(join_clauses);

	/* Build the From clause of the JOIN query */
	from_expr = makeNode(FromExpr);
	from_expr->fromlist = list_make1(join_expr);
	from_expr->quals = (Node *)make_ands_explicit(other_clauses);

	/* Build the query now */
	join_query = makeNode(Query);
	join_query->commandType = CMD_SELECT;
	join_query->rtable = rtable;
	join_query->jointree = from_expr;
	join_query->targetList = tlist;

	return join_query;
}

/*
 * pgxc_build_shippable_query_recurse
 * Recursively build Query structure for the RelOptInfo in the given RemoteQuery
 * path.
 */
static Query *
pgxc_build_shippable_query_recurse(PlannerInfo *root, RemoteQueryPath *rqpath,
							List **unshippable_quals, List **rep_tlist)
{
	RelOptInfo	*parent_rel = rqpath->path.parent;
	Query		*result_query;

	switch (parent_rel->reloptkind)
	{
		case RELOPT_BASEREL:
		case RELOPT_OTHER_MEMBER_REL:
			result_query = pgxc_build_shippable_query_baserel(root, rqpath,
																unshippable_quals,
																rep_tlist);
		break;

		case RELOPT_JOINREL:
			result_query = pgxc_build_shippable_query_jointree(root, rqpath,
																unshippable_quals,
																rep_tlist);
		break;

		default:
			elog(ERROR, "Creating remote query plan for relations of type %d is not supported",
							parent_rel->reloptkind);
	}
	return result_query;
}

/*
 * pgxc_rqplan_adjust_vars
 * Adjust the Var nodes in given node to be suitable to the
 * rqplan->remote_query. We don't need a expression mutator here, since we are
 * replacing scalar members in the Var nodes.
 */
extern void
pgxc_rqplan_adjust_vars(RemoteQuery *rqplan, Node *node)
{
	List		*var_list = pull_var_clause(node, PVC_RECURSE_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS);
	ListCell	*lcell_var;

	/*
	 * For every Var node in base_tlist find a matching Var node in ref_tlist.
	 * Get the Var node in the with same resno. This is
	 * assumed to the Var node corresponding to the Var node in base_tlist in
	 * the "query" targetlist is copied into the later.
	 */
	foreach (lcell_var, var_list)
	{
		TargetEntry	*ref_tle;
		TargetEntry	*qry_tle;
		Var *var = (Var *)lfirst(lcell_var);

		ref_tle = tlist_member((Node *)var, rqplan->coord_var_tlist);	
		qry_tle = get_tle_by_resno(rqplan->query_var_tlist, ref_tle->resno);	
		if (!IsA(qry_tle->expr, Var))
			elog(ERROR, "expected a VAR node but got node of type %d", nodeTag(qry_tle->expr));
		*var = *(Var *)(qry_tle->expr);
	}
}

/*
 * pgxc_rqplan_build_statement
 * Given a RemoteQuery plan generate the SQL statement from Query structure
 * inside it.
 */
static void
pgxc_rqplan_build_statement(RemoteQuery *rqplan)
{
	StringInfo sql = makeStringInfo();
	deparse_query(rqplan->remote_query, sql, NULL);
	if (rqplan->sql_statement)
		pfree(rqplan->sql_statement);
	rqplan->sql_statement = sql->data;
	return;
}

/*
 * pgxc_rqplan_adjust_tlist
 * The function adjusts the targetlist of remote_query in RemoteQuery node
 * according to the plan's targetlist. This function should be
 * called whenever we modify or set plan's targetlist (plan->targetlist). 
 */
extern void
pgxc_rqplan_adjust_tlist(RemoteQuery *rqplan)
{
	Plan	*plan = &(rqplan->scan.plan);
	List	*base_tlist;
	List	*query_tlist;
	/*
	 * Build the target list to be shipped to the datanode from the targetlist
	 * expected from the plan, if the targetlist is not shippable as is.
	 */
	if (!rqplan->is_shippable_tlist)
		base_tlist = pgxc_build_shippable_tlist(plan->targetlist, plan->qual);
	else
		base_tlist = copyObject(plan->targetlist);

	query_tlist = copyObject(base_tlist);
	/* Replace the targetlist of remote_query with the new base_tlist */
	pgxc_rqplan_adjust_vars(rqplan, (Node *)query_tlist);
	rqplan->remote_query->targetList = query_tlist;
	rqplan->base_tlist = base_tlist;
	/* Build the SQL query statement for the modified remote_query */
	pgxc_rqplan_build_statement(rqplan);
}

/*
 * pgxc_build_shippable_query
 * Builds a shippable Query for given RemoteQuery path to be stuffed in given
 * RemoteQuery plan.
 */
static void
pgxc_build_shippable_query(PlannerInfo *root, RemoteQueryPath *covering_path,
							RemoteQuery *result_node)
{
	Query		*query;	
	List		*rep_tlist;
	List		*unshippable_quals;
	
	/*
	 * Build Query representing the result of the JOIN tree. During the process
	 * we also get the set of unshippable quals to be applied after getting the
	 * results from the datanode and the targetlist representing the results of
	 * the query, in a form which plan nodes on coordinator can understand
	 */
	query = pgxc_build_shippable_query_recurse(root, covering_path,
															&unshippable_quals,
															&rep_tlist);
	result_node->scan.plan.qual = unshippable_quals;
	result_node->query_var_tlist = query->targetList;
	result_node->coord_var_tlist = rep_tlist;
	result_node->remote_query = query;

	pgxc_rqplan_adjust_tlist(result_node);
}

/*
 * create_remotequery_plan
 * The function creates a remote query plan corresponding to the path passed in.
 * It creates a query statement to fetch the results corresponding to the
 * RelOptInfo in the given RemoteQuery path. It first builds the Query structure
 * and deparses it to generate the query statement.
 * At the end it creates a dummy RTE corresponding to this RemoteQuery plan to
 * be added in the rtable list in Query.
 */
Plan *
create_remotequery_plan(PlannerInfo *root, RemoteQueryPath *best_path)
{
	RelOptInfo		*rel = best_path->path.parent;	/* relation for which plan is 
													 * being built
													 */
	RemoteQuery		*result_node;	/* the built plan */
	List			*tlist;			/* expected target list */
	RangeTblEntry	*dummy_rte;			/* RTE for the remote query node being
										 * added.
										 */
	Index			dummy_rtindex;
	char			*rte_name;

	/* Get the target list required from this plan */ 
	tlist = pgxc_build_relation_tlist(rel);
	result_node = makeNode(RemoteQuery);
	result_node->scan.plan.targetlist = tlist;
	pgxc_build_shippable_query(root, best_path, result_node);
	
	/*
	 * Create and append the dummy range table entry to the range table.
	 * Note that this modifies the master copy the caller passed us, otherwise
	 * e.g EXPLAIN VERBOSE will fail to find the rte the Vars built below.
	 * PGXC_TODO: If there is only a single table, should we set the table name as
	 * the name of the rte?
	 */
	rte_name = "_REMOTE_TABLE_QUERY_";
	if (rel->reloptkind == RELOPT_BASEREL ||
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
	{
		RangeTblEntry *rel_rte = rt_fetch(rel->relid, root->parse->rtable);
		char	*tmp_rtename = get_rel_name(rel_rte->relid);
		if (tmp_rtename)
			rte_name = tmp_rtename;
	}

	dummy_rte = make_dummy_remote_rte(rte_name,
										makeAlias("_REMOTE_TABLE_QUERY_", NIL));
	root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
	dummy_rtindex = list_length(root->parse->rtable);
	
	result_node->scan.scanrelid = dummy_rtindex;
	result_node->read_only = true; 
	/* result_node->read_only = (query->commandType == CMD_SELECT && !query->hasForUpdate); */
	/* result_node->has_row_marks = query->hasForUpdate; */
	result_node->exec_nodes = best_path->rqpath_en;
	/*
	 * For replicated results, we need to choose one of the nodes, if there are
	 * many of them.
	 */
	if (IsLocatorReplicated(result_node->exec_nodes->baselocatortype))
		result_node->exec_nodes->nodeList = 
						GetPreferredReplicationNode(result_node->exec_nodes->nodeList);

	result_node->is_temp = best_path->rqhas_temp_rel;

	if (!result_node->exec_nodes)
		elog(ERROR, "No distribution information found for remote query path");

	pgxc_copy_path_costsize(&(result_node->scan.plan), (Path *)best_path);

	/* PGXCTODO - get better estimates */
 	result_node->scan.plan.plan_rows = 1000;

	result_node->has_ins_child_sel_parent = root->parse->is_ins_child_sel_parent;
	/*
	 * If there is a pseudoconstant, we should create a gating plan on top of
	 * this node. We must have included the pseudoconstant qual in the remote
	 * query as well, but gating the plan improves performance in case the plan
	 * quals happens to be false.
	 */
	if (root->hasPseudoConstantQuals)
	{
		List *quals;
		switch(rel->reloptkind)
		{
			case RELOPT_BASEREL:
			case RELOPT_OTHER_MEMBER_REL:
				quals = rel->baserestrictinfo;
			break;

			case RELOPT_JOINREL:
				quals = best_path->join_restrictlist;
			break;

			default:
				elog(ERROR, "creating remote query plan for relations of type %d is not supported",
							rel->reloptkind);
		}
		return pgxc_create_gating_plan(root, (Plan *)result_node, quals);
	}

	return (Plan *)result_node;
}

static RemoteQuery *
make_remotequery(List *qptlist, List *qpqual, Index scanrelid)
{
	RemoteQuery *node = makeNode(RemoteQuery);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->read_only = true;
	node->has_row_marks = false;

	return node;
}

/*
 * create_remoteinsert_plan()
 *
 * For every target relation, add a remote query node to carry out remote
 * operations.
 */
Plan *
create_remoteinsert_plan(PlannerInfo *root, Plan *topplan)
{
	ModifyTable *mt = (ModifyTable *)topplan;
	ListCell	   *l;

	/* We expect to work only on ModifyTable node */
	if (!IsA(topplan, ModifyTable))
		elog(ERROR, "Unexpected node type: %d", topplan->type);

	/*
	 * For every result relation, build a remote plan to execute remote insert.
	 */
	foreach(l, mt->resultRelations)
	{
		Index			resultRelationIndex = lfirst_int(l);
		RangeTblEntry	*ttab;
		RelationLocInfo *rel_loc_info;
		StringInfo		buf, buf2;
		RemoteQuery	   *fstep;
		Oid				nspid;
		char		   *nspname;
		int				natts, att;
		Oid 		   *att_types;
		char		   *relname;
		bool			first_att_printed = false;

		ttab = rt_fetch(resultRelationIndex, root->parse->rtable);

		/* Bad relation ? */
		if (ttab == NULL || ttab->rtekind != RTE_RELATION)
			continue;

		/* Get location info of the target table */
		rel_loc_info = GetRelationLocInfo(ttab->relid);
		if (rel_loc_info == NULL)
			continue;

		/* For main string */
		buf = makeStringInfo();
		/* For values */
		buf2 = makeStringInfo();

		/* Compose INSERT FROM target_table */
		nspid = get_rel_namespace(ttab->relid);
		nspname = get_namespace_name(nspid);
		relname = get_rel_name(ttab->relid);

		/*
		 * Do not qualify with namespace for TEMP tables. The schema name may
		 * vary on each node
		 */
		if (IsTempTable(ttab->relid))
			appendStringInfo(buf, "INSERT INTO %s (",
					quote_identifier(relname));
		else
			appendStringInfo(buf, "INSERT INTO %s.%s (", quote_identifier(nspname),
					quote_identifier(relname));

		fstep = make_remotequery(NIL, NIL, resultRelationIndex);
		fstep->is_temp = IsTempTable(ttab->relid);

		natts = get_relnatts(ttab->relid);
		att_types = (Oid *) palloc0 (sizeof (Oid) * natts);

		/*
		 * Populate the column information
		 */
		for (att = 1; att <= natts; att++)
		{
			HeapTuple tp;

			tp = SearchSysCache(ATTNUM,
					ObjectIdGetDatum(ttab->relid),
					Int16GetDatum(att),
					0, 0);
			if (HeapTupleIsValid(tp))
			{
				Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);

				/* Bypass dropped attributes in query */
				if (att_tup->attisdropped)
				{
					/* Dropped attributes are casted as int4 in prepared parameters */
					att_types[att - 1] = INT4OID;
				}
				else
				{
					/* Add comma before all except first attributes */
					if (first_att_printed)
						appendStringInfoString(buf, ", ");

					/* Build the value part, parameters are filled at run time */
					if (first_att_printed)
						appendStringInfoString(buf2, ", ");

					first_att_printed = true;

					/* Append column name */
					appendStringInfoString(buf, quote_identifier(NameStr(att_tup->attname)));

					/* Append value in string */
					appendStringInfo(buf2, "$%d", att);

					/* Assign parameter type */
					att_types[att - 1] = att_tup->atttypid;
				}

				ReleaseSysCache(tp);
			}
			else
				elog(ERROR, "cache lookup failed for attribute %d of relation %u",
						att, ttab->relid);
		}

		/* Gather the two strings */
		appendStringInfo(buf, ") VALUES (%s)", buf2->data);

		fstep->sql_statement = pstrdup(buf->data);

		/* Processed rows are counted by the main planner */
		fstep->combine_type = COMBINE_TYPE_NONE;

		fstep->read_only = false;
		fstep->exec_nodes = makeNode(ExecNodes);
		fstep->exec_nodes->baselocatortype = rel_loc_info->locatorType;
		fstep->exec_nodes->primarynodelist = NULL;
		fstep->exec_nodes->nodeList = rel_loc_info->nodeList;
		fstep->exec_nodes->en_relid = ttab->relid;
		fstep->exec_nodes->accesstype = RELATION_ACCESS_INSERT;
		fstep->exec_nodes->en_expr = pgxc_set_en_expr(ttab->relid, resultRelationIndex);

		SetRemoteStatementName((Plan *) fstep, NULL, natts, att_types, 0);

		/* Free everything */
		pfree(buf->data);
		pfree(buf);
		pfree(buf2->data);
		pfree(buf2);

		mt->remote_plans = lappend(mt->remote_plans, fstep);
	}

	return topplan;
}


/*
 * create_remoteupdate_plan()
 *
 * For every target relation, add a remote query node to carry out remote
 * operations.
 * WHERE and SET clauses are populated with the relation attributes.
 * Target list is used for SET clause and completed with the expressions already given
 * Those are the non-junk expressions in target list of parser tree.
 * WHERE clause is completed by the other expressions in target tree that have been
 * marked as junk during target list rewriting to be able to identify consistently
 * tuples on remote Coordinators. This target list is based on the information obtained
 * from the inner plan that should be generated by create_remotequery_plan.
 */
Plan *
create_remoteupdate_plan(PlannerInfo *root, Plan *topplan)
{
	ModifyTable *mt = (ModifyTable *)topplan;
	ListCell	*l;

	/* We expect to work only on ModifyTable node */
	if (!IsA(topplan, ModifyTable))
		elog(ERROR, "Unexpected node type: %d", topplan->type);

	/*
	 * For every result relation, build a remote plan to execute remote update.
	 */
	foreach(l, mt->resultRelations)
	{
		Index			resultRelationIndex = lfirst_int(l);
		Query			*parse = root->parse;
		RangeTblEntry   *ttab;
		RelationLocInfo *rel_loc_info;
		StringInfo		buf, buf2;
		Oid			    nspid;		/* Relation namespace Oid */
		char		   *nspname;	/* Relation namespace name */
		Oid			   *param_types;	/* Types of query parameters */
		bool			is_set_printed = false;	/* Control of SET generation */
		bool			is_where_printed = false;	/* Control of WHERE generation */
		RemoteQuery	   *fstep;		/* Plan step generated */
		ListCell	   *elt;
		int				count = 0, where_count = 1;
		int				natts, count_prepparams, tot_prepparams;
		char		   *relname;

		ttab = rt_fetch(resultRelationIndex, parse->rtable);

		/* Bad relation ? */
		if (ttab == NULL || ttab->rtekind != RTE_RELATION)
			continue;

		relname = get_rel_name(ttab->relid);

		/* Get location info of the target table */
		rel_loc_info = GetRelationLocInfo(ttab->relid);
		if (rel_loc_info == NULL)
			continue;

		/* Create query buffers */
		buf = makeStringInfo();		/* For SET clause */
		buf2 = makeStringInfo();	/* For WHERE clause */

		/* Compose UPDATE target_table */
		natts = get_relnatts(ttab->relid);
		nspid = get_rel_namespace(ttab->relid);
		nspname = get_namespace_name(nspid);

		/*
		 * Do not qualify with namespace for TEMP tables. The schema name may
		 * vary on each node
		 */
		if (IsTempTable(ttab->relid))
			appendStringInfo(buf, "UPDATE ONLY %s SET ",
							 quote_identifier(relname));
		else
			appendStringInfo(buf, "UPDATE ONLY %s.%s SET ", quote_identifier(nspname),
							 quote_identifier(relname));

		/*
		 * Count the number of junk entries before setting the parameter type list.
		 * This helps to know how many parameters part of the WHERE clause need to
		 * be sent down by extended query protocol.
		 */
		foreach(elt, parse->targetList)
		{
			TargetEntry *tle = lfirst(elt);
			if (tle->resjunk)
				count++;
		}
		count_prepparams = natts + count;
		/* Count entries related to Rowmarks */
		tot_prepparams = count_prepparams + pgxc_count_rowmarks_entries(root->rowMarks);

		/* Then allocate the array for this purpose */
		param_types = (Oid *) palloc0(sizeof (Oid) * tot_prepparams);

		/*
		 * Now build the query based on the target list. SET clause is completed
		 * by non-junk entries and WHERE clause by junk entries used to identify
		 * uniquely tuples on remote nodes.
		 */
		foreach(elt, parse->targetList)
		{
			TargetEntry *tle = lfirst(elt);

			if (!tle->resjunk)
			{
				int attno = 0;
				int i;

				/* Add target list element to SET clause */

				/* Add comma before all except first attributes */
				if (!is_set_printed)
					is_set_printed = true;
				else
					appendStringInfoString(buf, ", ");

				/* We need first to find the position of this element in attribute list */
				for (i = 0; i < natts; i++)
				{
					if (strcmp(tle->resname,
					    get_relid_attribute_name(ttab->relid, i + 1)) == 0)
					{
						attno = i + 1;
						break;
					}
				}

				/* Complete string */
				appendStringInfo(buf, "%s = $%d",
								 tle->resname,
								 attno);

				/* Set parameter type correctly */
				param_types[attno - 1] = exprType((Node *) tle->expr);
			}
			else
			{
				/* Set parameter type */
				param_types[natts + where_count - 1] = exprType((Node *) tle->expr);
				where_count++;

				/*
				 * ctid and xc_node_id are sufficient to identify
				 * remote tuple.
				 */
				if (strcmp(tle->resname, "xc_node_id") != 0 &&
					strcmp(tle->resname, "ctid") != 0)
					continue;

				/* Set the clause if necessary */
				if (!is_where_printed)
				{
					is_where_printed = true;
					appendStringInfoString(buf2, " WHERE ");
				}
				else
					appendStringInfoString(buf2, "AND ");

				/* Complete string */
				appendStringInfo(buf2, "%s = $%d ",
								 tle->resname,
								 natts + where_count - 1);
			}
		}

		/*
		 * Before finalizing query be sure that there are no missing entries for attributes.
		 * If there are complete the last holes. Those ones are mandatory to insure that
		 * update is executed consistently.
		 */
		for (count = 1; count <= natts; count++)
		{
			if (param_types[count - 1] == 0)
			{
				HeapTuple tp;

				tp = SearchSysCache(ATTNUM,
									ObjectIdGetDatum(ttab->relid),
									Int16GetDatum(count),
									0, 0);

				if (HeapTupleIsValid(tp))
				{
					Form_pg_attribute att_saved = (Form_pg_attribute) GETSTRUCT(tp);

					/*
					 * Set parameter type of attribute
					 * Dropped columns are casted as int4
					 */
					if (att_saved->attisdropped)
						param_types[count - 1] = INT4OID;
					else
						param_types[count - 1] = att_saved->atttypid;
					ReleaseSysCache(tp);
				}
				else
					elog(ERROR, "cache lookup failed for attribute %d of relation %u",
							count, ttab->relid);
			}
		}

		/*
		 * The query needs to be completed by nullifying the non-parent entries
		 * defined in RowMarks. This is essential for UPDATE queries running with child
		 * entries as we need to bypass them correctly at executor level.
		 */
		param_types = pgxc_build_rowmark_entries(root->rowMarks, parse->rtable, param_types,
												count_prepparams, tot_prepparams);

		/* Finish building the query by gathering SET and WHERE clauses */
		appendStringInfo(buf, "%s", buf2->data);

		/* Finally build the final UPDATE step */
		fstep = make_remotequery(parse->targetList, NIL, resultRelationIndex);
		fstep->is_temp = IsTempTable(ttab->relid);
		fstep->sql_statement = pstrdup(buf->data);
		fstep->combine_type = COMBINE_TYPE_NONE;

		fstep->read_only = false;
		/*
		 * Get the nodes to execute the query on. We will execute this query on
		 * all nodes. The WHERE condition will take care of updating the columns
		 * accordingly.
		 */
		fstep->exec_nodes = GetRelationNodes(rel_loc_info, 0, true, UNKNOWNOID, RELATION_ACCESS_UPDATE);
		fstep->exec_nodes->baselocatortype = rel_loc_info->locatorType;
		fstep->exec_nodes->en_relid = ttab->relid;
		fstep->exec_nodes->nodeList = rel_loc_info->nodeList;
		fstep->exec_nodes->accesstype = RELATION_ACCESS_UPDATE;
		fstep->exec_nodes->en_expr = pgxc_set_en_expr(ttab->relid, resultRelationIndex);
		SetRemoteStatementName((Plan *) fstep, NULL, tot_prepparams, param_types, 0);
		pfree(buf->data);
		pfree(buf2->data);
		pfree(buf);
		pfree(buf2);

		mt->remote_plans = lappend(mt->remote_plans, fstep);
	}

	return topplan;
}

/*
 * create_remotedelete_plan()
 *
 * For every target relation, add a remote query node to carry out remote
 * operations. The tuple to be deleted is selected depending on the target
 * list of given plan, generating parametrized WHERE clause in consequence.
 */
Plan *
create_remotedelete_plan(PlannerInfo *root, Plan *topplan)
{
	ModifyTable *mt = (ModifyTable *)topplan;
	ListCell	*l;

	/* We expect to work only on ModifyTable node */
	if (!IsA(topplan, ModifyTable))
		elog(ERROR, "Unexpected node type: %d", topplan->type);

	/*
	 * For every result relation, build a remote plan to execute remote delete.
	 */
	foreach(l, mt->resultRelations)
	{
		Index			resultRelationIndex = lfirst_int(l);
		Query			*parse = root->parse;
		RangeTblEntry   *ttab;
		RelationLocInfo *rel_loc_info;
		StringInfo		buf;
		Oid			    nspid;		/* Relation namespace Oid */
		char		   *nspname;	/* Relation namespace name */
		int				count_prepparams, tot_prepparams;	/* Attribute used is CTID */
		Oid			   *param_types;	/* Types of query parameters */
		RemoteQuery	   *fstep;		/* Plan step generated */
		bool			is_where_created = false;
		ListCell	   *elt;
		int				count = 1;
		char		   *relname;

		ttab = rt_fetch(resultRelationIndex, parse->rtable);

		/* Bad relation ? */
		if (ttab == NULL || ttab->rtekind != RTE_RELATION)
			continue;

		/* Get location info of the target table */
		rel_loc_info = GetRelationLocInfo(ttab->relid);
		if (rel_loc_info == NULL)
			continue;

		/* Create query buffers */
		buf = makeStringInfo();

		/* Compose DELETE target_table */
		nspid = get_rel_namespace(ttab->relid);
		nspname = get_namespace_name(nspid);
		relname = get_rel_name(ttab->relid);

		/* Parameters are defined by target list */
		count_prepparams = list_length(parse->targetList);

		/* Count entries related to Rowmarks only if there are child relations here */
		if (list_length(mt->resultRelations) != 1)
			tot_prepparams = count_prepparams + pgxc_count_rowmarks_entries(root->rowMarks);
		else
			tot_prepparams = count_prepparams;

		param_types = (Oid *) palloc0(sizeof(Oid) * tot_prepparams);

		/*
		 * Do not qualify with namespace for TEMP tables. The schema name may
		 * vary on each node.
		 */
		if (IsTempTable(ttab->relid))
			appendStringInfo(buf, "DELETE FROM ONLY %s ",
							 quote_identifier(relname));
		else
			appendStringInfo(buf, "DELETE FROM ONLY %s.%s ", quote_identifier(nspname),
							 quote_identifier(relname));

		/* Generate WHERE clause for each target list item */
		foreach(elt, parse->targetList)
		{
			TargetEntry *tle = lfirst(elt);

			/* Set up the parameter type */
			param_types[count - 1] = exprType((Node *) tle->expr);
			count++;

			/*
			 * In WHERE clause, ctid and xc_node_id are
			 * sufficient to fetch a tuple from remote node.
			 */
			if (strcmp(tle->resname, "xc_node_id") != 0 &&
				strcmp(tle->resname, "ctid") != 0)
				continue;

			/* Set the clause if necessary */
			if (!is_where_created)
			{
				is_where_created = true;
				appendStringInfoString(buf, "WHERE ");
			}
			else
				appendStringInfoString(buf, "AND ");

			appendStringInfo(buf, "%s = $%d ",
							quote_identifier(tle->resname),
							count - 1);
		}

		/*
		 * The query needs to be completed by nullifying the non-parent entries
		 * defined in RowMarks. This is essential for UPDATE queries running with child
		 * entries as we need to bypass them correctly at executor level.
		 */
		param_types = pgxc_build_rowmark_entries(root->rowMarks, parse->rtable, param_types,
												count_prepparams, tot_prepparams);

		/* Finish by building the plan step */
		fstep = make_remotequery(parse->targetList, NIL, resultRelationIndex);
		fstep->is_temp = IsTempTable(ttab->relid);
		fstep->sql_statement = pstrdup(buf->data);
		fstep->combine_type = COMBINE_TYPE_NONE;

		fstep->read_only = false;
		/*
		 * Get the nodes to execute the query on. We will execute this query on
		 * all nodes. The WHERE condition will take care of updating the columns
		 * accordingly.
		 */
		fstep->exec_nodes = GetRelationNodes(rel_loc_info, 0, true, UNKNOWNOID,
												RELATION_ACCESS_UPDATE);
		fstep->exec_nodes->baselocatortype = rel_loc_info->locatorType;
		fstep->exec_nodes->en_relid = ttab->relid;
		fstep->exec_nodes->nodeList = rel_loc_info->nodeList;
		fstep->exec_nodes->accesstype = RELATION_ACCESS_UPDATE;
		SetRemoteStatementName((Plan *) fstep, NULL, tot_prepparams, param_types, 0);
		pfree(buf->data);
		pfree(buf);

		mt->remote_plans = lappend(mt->remote_plans, fstep);
	}

	return topplan;
}

/*
 * create_remotegrouping_plan
 * Check if the grouping and aggregates can be pushed down to the
 * Datanodes.
 * Right now we can push with following restrictions
 * 1. there are plain aggregates (no expressions involving aggregates) and/or
 *    expressions in group by clauses
 * 2. No distinct or order by clauses
 * 3. No windowing clause
 * 4. No having clause
 *
 * Inputs
 * root - planerInfo root for this query
 * agg_plan - local grouping plan produced by grouping_planner()
 *
 * PGXCTODO: work on reducing these restrictions as much or document the reasons
 * why we need the restrictions, in these comments themselves. In case of
 * replicated tables, we should be able to push the whole query to the data
 * node in case there are no local clauses.
 */
Plan *
create_remotegrouping_plan(PlannerInfo *root, Plan *local_plan)
{
	Query		*query = root->parse;
	Sort		*sort_plan;
	RemoteQuery	*remote_scan;	/* remote query in the passed in plan */
	Plan		*temp_plan;
	List		*base_tlist;
	RangeTblEntry	*dummy_rte;
	int			numGroupCols;
	AttrNumber	*grpColIdx;
	bool		reduce_plan;
	List		*remote_qual;
	List 		*local_qual;

	/* Remote grouping is not enabled, don't do anything */
	if (!enable_remotegroup)
		return local_plan;
	/*
	 * We don't push aggregation and grouping to Datanodes, in case there are
	 * windowing aggregates, distinct, having clause or sort clauses.
	 */
	if (query->hasWindowFuncs ||
		query->distinctClause ||
		query->sortClause)
		return local_plan;

	/* for now only Agg/Group plans */
	if (local_plan && IsA(local_plan, Agg))
	{
		numGroupCols = ((Agg *)local_plan)->numCols;
		grpColIdx = ((Agg *)local_plan)->grpColIdx;
	}
	else if (local_plan && IsA(local_plan, Group))
	{
		numGroupCols = ((Group *)local_plan)->numCols;
		grpColIdx = ((Group *)local_plan)->grpColIdx;
	}
	else
		return local_plan;

	/*
	 * We expect plan tree as Group/Agg->Sort->Result->Material->RemoteQuery,
	 * Result, Material nodes are optional. Sort is compulsory for Group but not
	 * for Agg.
	 * anything else is not handled right now.
	 */
	temp_plan = local_plan->lefttree;
	remote_scan = NULL;
	sort_plan = NULL;
	if (temp_plan && IsA(temp_plan, Sort))
	{
		sort_plan = (Sort *)temp_plan;
		temp_plan = temp_plan->lefttree;
	}
	if (temp_plan && IsA(temp_plan, Result))
		temp_plan = temp_plan->lefttree;
	if (temp_plan && IsA(temp_plan, Material))
		temp_plan = temp_plan->lefttree;
	if (temp_plan && IsA(temp_plan, RemoteQuery))
		remote_scan = (RemoteQuery *)temp_plan;

	if (!remote_scan)
		return local_plan;
	/*
	 * for Group plan we expect Sort under the Group, which is always the case,
	 * the condition below is really for some possibly non-existent case.
	 */
	if (IsA(local_plan, Group) && !sort_plan)
		return local_plan;
	/*
	 * If the remote_scan has any quals on it, those need to be executed before
	 * doing anything. Hence we won't be able to push any aggregates or grouping
	 * to the Datanode.
	 * If it has any SimpleSort in it, then sorting is intended to be applied
	 * before doing anything. Hence can not push any aggregates or grouping to
	 * the Datanode.
	 */
	if (remote_scan->scan.plan.qual || remote_scan->sort)
		return local_plan;

	/*
	 * Grouping_planner may add Sort node to sort the rows
	 * based on the columns in GROUP BY clause. Hence the columns in Sort and
	 * those in Group node in should be same. The columns are usually in the
	 * same order in both nodes, hence check the equality in order. If this
	 * condition fails, we can not handle this plan for now.
	 */
	if (sort_plan)
	{
		int cntCols;
		if (sort_plan->numCols != numGroupCols)
			return local_plan;
		for (cntCols = 0; cntCols < numGroupCols; cntCols++)
		{
			if (sort_plan->sortColIdx[cntCols] != grpColIdx[cntCols])
				return local_plan;
		}
	}

	/*
	 * process the targetlist of the grouping plan, also construct the
	 * targetlist of the query to be shipped to the remote side
	 */
	base_tlist = pgxc_process_grouping_targetlist(root, &(local_plan->targetlist));
	/*
	 * If can not construct a targetlist shippable to the Datanode. Resort to
	 * the plan created by grouping_planner()
	 */
	if (!base_tlist)
		return local_plan;

	base_tlist = pgxc_process_having_clause(root, base_tlist, query->havingQual,
												&local_qual, &remote_qual, &reduce_plan);
	remote_qual = copyObject(remote_qual);
	/*
	 * Because of HAVING clause, we can not push the aggregates and GROUP BY
	 * clause to the Datanode. Resort to the plan created by grouping planner.
	 */
	if (!reduce_plan)
		return local_plan;
	Assert(base_tlist);

	/* Modify the targetlist of the JOIN plan with the new targetlist */
	remote_scan->scan.plan.targetlist = base_tlist;
	/*
	 * recompute the column ids of the grouping columns,
	 * the group column indexes computed earlier point in the
	 * targetlists of the scan plans under this node. But now the grouping
	 * column indexes will be pointing in the targetlist of the new
	 * RemoteQuery, hence those need to be recomputed
	 */
	pgxc_locate_grouping_columns(root, base_tlist, grpColIdx);

	/* Add the GROUP BY clause to the JOIN query */
	remote_scan->remote_query->groupClause = query->groupClause;
	remote_scan->remote_query->hasAggs = query->hasAggs;
	/* Add the ORDER BY clause if necessary */
	if (sort_plan)
	{
		List		*sortClause = NIL;
		SimpleSort	*remote_sort = makeNode(SimpleSort);
		int 		cntCols;

		Assert(query->groupClause);
		/*
		 * reuse the arrays allocated in sort_plan to create SimpleSort
		 * structure. sort_plan is useless henceforth.
		 */
		remote_sort->numCols = sort_plan->numCols;
		remote_sort->sortColIdx = sort_plan->sortColIdx;
		remote_sort->sortOperators = sort_plan->sortOperators;
		remote_sort->sortCollations = sort_plan->collations;
		remote_sort->nullsFirst = sort_plan->nullsFirst;
		/*
		 * Create list of SortGroupClauses corresponding to the Sorting expected
		 * by the sort_plan node. Earlier we have made sure that the columns on
		 * which to sort are same as the grouping columns.
		 */
		for (cntCols = 0; cntCols < remote_sort->numCols; cntCols++)
		{
			SortGroupClause	*sortClauseItem = makeNode(SortGroupClause);
			TargetEntry		*sc_tle = get_tle_by_resno(base_tlist,
														grpColIdx[cntCols]);
			
			Assert(sc_tle->ressortgroupref > 0);
			sortClauseItem->tleSortGroupRef = sc_tle->ressortgroupref;
			sortClauseItem->sortop = remote_sort->sortOperators[cntCols];
			sortClauseItem->nulls_first = remote_sort->nullsFirst[cntCols];
			sortClause = lappend(sortClause, sortClauseItem);
			
			/* set the sorting column index in the Sort node in RemoteQuery */
			remote_sort->sortColIdx[cntCols] = grpColIdx[cntCols];
		}
		remote_scan->sort = remote_sort;
		remote_scan->remote_query->sortClause = sortClause;
	}

	/* we need to adjust the Vars in HAVING clause to be shipped */
	pgxc_rqplan_adjust_vars(remote_scan, (Node *)remote_qual);
	remote_scan->remote_query->havingQual = (Node *)remote_qual;
	/*
	 * Ultimately modify the targetlist of the remote_query according to the new
	 * base_tlist and generate the SQL statement. We don't want to check again
	 * for shippability while adjusting the Vars.
	 */
	remote_scan->is_shippable_tlist = true;
	pgxc_rqplan_adjust_tlist(remote_scan);

	local_plan->lefttree = (Plan *)remote_scan;
	local_plan->qual = local_qual;

	/* Change the dummy RTE added to show the new place of reduction */
	dummy_rte = rt_fetch(remote_scan->scan.scanrelid, query->rtable);
	dummy_rte->relname = "__REMOTE_GROUP_QUERY__";
	dummy_rte->eref = makeAlias("__REMOTE_GROUP_QUERY__", NIL);
	/* indicate that we should apply collection function directly */
	if (IsA(local_plan, Agg))
		((Agg *)local_plan)->skip_trans = true;

	return local_plan;
}

/*
 * pgxc_locate_grouping_columns
 * Locates the grouping clauses in the given target list. This is very similar
 * to locate_grouping_columns except that there is only one target list to
 * search into.
 * PGXCTODO: Can we reuse locate_grouping_columns() instead of writing this
 * function? But this function is optimized to search in the same target list.
 */
static void
pgxc_locate_grouping_columns(PlannerInfo *root, List *tlist,
								AttrNumber *groupColIdx)
{
	int			keyno = 0;
	ListCell   *gl;

	/*
	 * No work unless grouping.
	 */
	if (!root->parse->groupClause)
	{
		Assert(groupColIdx == NULL);
		return;
	}
	Assert(groupColIdx != NULL);

	foreach(gl, root->parse->groupClause)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(gl);
		TargetEntry *te = get_sortgroupclause_tle(grpcl, tlist);
		if (!te)
			elog(ERROR, "failed to locate grouping columns");
		groupColIdx[keyno++] = te->resno;
	}
}

/*
 * pgxc_add_node_to_grouping_tlist
 * Add the given node to the target list to be sent to the Datanode. If it's
 * Aggref node, also change the passed in node to point to the Aggref node in
 * the Datanode's target list
 */
static List *
pgxc_add_node_to_grouping_tlist(List *remote_tlist, Node *expr, Index ressortgroupref)
{
	TargetEntry *remote_tle;
	Oid			saved_aggtype;

	/*
	 * When we add an aggregate to the remote targetlist the aggtype of such
	 * Aggref node is changed to aggtrantype. Hence while searching a given
	 * Aggref in remote targetlist, we need to change the aggtype accordingly
	 * and then switch it back.
	 */
	if (IsA(expr, Aggref))
	{
		Aggref *aggref = (Aggref *)expr;
		saved_aggtype = aggref->aggtype;
		aggref->aggtype = aggref->aggtrantype;
	}
	else
	{
		/*
		 * When we use saved_aggtype later, compiler complains that this
		 * variable can be used without assigning a value to it, which can not
		 * be a case. Still keep compiler silent
		 */
		saved_aggtype = InvalidOid;
	}

	remote_tle = tlist_member(expr, remote_tlist);
	if (IsA(expr, Aggref))
		((Aggref *)expr)->aggtype = saved_aggtype;

	if (!remote_tle)
	{
		remote_tle = makeTargetEntry(copyObject(expr),
							  list_length(remote_tlist) + 1,
							  NULL,
							  false);
		/* Copy GROUP BY/SORT BY reference for the locating group by columns */
		remote_tle->ressortgroupref = ressortgroupref;
		remote_tlist = lappend(remote_tlist, remote_tle);
	}
	else
	{
		if (remote_tle->ressortgroupref == 0)
			remote_tle->ressortgroupref = ressortgroupref;
		else if (ressortgroupref == 0)
		{
			/* do nothing remote_tle->ressortgroupref has the right value */
		}
		else
		{
			/*
			 * if the expression's TLE already has a Sorting/Grouping reference,
			 * and caller has passed a non-zero one as well, better both of them
			 * be same
			 */
			Assert(remote_tle->ressortgroupref == ressortgroupref);
		}
	}

	/*
	 * Replace the args of the local Aggref with Aggref node to be
	 * included in RemoteQuery node, so that set_plan_refs can convert
	 * the args into VAR pointing to the appropriate result in the tuple
	 * coming from RemoteQuery node
	 * PGXCTODO: should we push this change in targetlists of plans
	 * above?
	 */
	if (IsA(expr, Aggref))
	{
		Aggref	*local_aggref = (Aggref *)expr;
		Aggref	*remote_aggref = (Aggref *)remote_tle->expr;
		Assert(IsA(remote_tle->expr, Aggref));
		remote_aggref->aggtype = remote_aggref->aggtrantype;
		/* Is copyObject() needed here? probably yes */
		local_aggref->args = list_make1(makeTargetEntry(copyObject(remote_tle->expr),
																1, NULL,
																false));
	}
	return remote_tlist;
}
/*
 * pgxc_process_grouping_targetlist
 * The function scans the targetlist to check if the we can push anything
 * from the targetlist to the Datanode. Following rules govern the choice
 * 1. Either all of the aggregates are pushed to the Datanode or none is pushed
 * 2. If there are no aggregates, the targetlist is good to be shipped as is
 * 3. If aggregates are involved in expressions, we push the aggregates to the
 *    Datanodes but not the involving expressions.
 *
 * The function constructs the targetlist for the query to be pushed to the
 * Datanode. It modifies the local targetlist to point to the expressions in
 * remote targetlist wherever necessary (e.g. aggregates)
 *
 * PGXCTODO: we should be careful while pushing the function expressions, it's
 * better to push functions like strlen() which can be evaluated at the
 * Datanode, but we should avoid pushing functions which can only be evaluated
 * at Coordinator.
 */
static List *
pgxc_process_grouping_targetlist(PlannerInfo *root, List **local_tlist)
{
	bool	shippable_remote_tlist = true;
	List	*remote_tlist = NIL;
	List	*orig_local_tlist = NIL;/* Copy original local_tlist, in case it changes */
	ListCell	*temp;

	/*
	 * Walk through the target list and find out whether we can push the
	 * aggregates and grouping to Datanodes. Also while doing so, create the
	 * targetlist for the query to be shipped to the Datanode. Adjust the local
	 * targetlist accordingly.
	 */
	foreach(temp, *local_tlist)
	{
		TargetEntry				*local_tle = lfirst(temp);
		Node					*expr = (Node *)local_tle->expr;
		bool					has_aggs;

		/*
		 * If the expression is not Aggref but involves aggregates (has Aggref
		 * nodes in the expression tree, we can not push the entire expression
		 * to the Datanode, but push those aggregates to the Datanode, if those
		 * aggregates can be evaluated at the Datanodes (if is_foreign_expr
		 * returns true for entire expression). To evaluate the rest of the
		 * expression, we need to fetch the values of VARs participating in the
		 * expression. But, if we include the VARs under the aggregate nodes,
		 * they may not be part of GROUP BY clause, thus generating an invalid
		 * query. Hence, is_foreign_expr() wouldn't collect VARs under the
		 * expression tree rooted under Aggref node.
		 * For example, the original query is
		 * SELECT sum(val) * val2 FROM tab1 GROUP BY val2;
		 * the query pushed to the Datanode is
		 * SELECT sum(val), val2 FROM tab1 GROUP BY val2;
		 * Notice that, if we include val in the query, it will become invalid.
		 *
		 * It's crucial that we pass TargetEntry here to detect top-level
		 * RowExpr which are unshippable.
		 */
		if (!pgxc_is_expr_shippable((Expr *)local_tle, &has_aggs))
		{
				shippable_remote_tlist = false;
				break;
		}

		/*
		 * We are about to change the local_tlist, check if we have already
		 * copied original local_tlist, if not take a copy
		 */
		if (!orig_local_tlist && has_aggs)
				orig_local_tlist = copyObject(*local_tlist);

		/*
		 * If there are aggregates involved in the expression, whole expression
		 * can not be pushed to the Datanode. Pick up the aggregates and the
		 * VAR nodes not covered by aggregates.
		 */
		if (has_aggs)
		{
			ListCell	*lcell;
			List		*aggs_n_vars;
			/*
			 * This expression is not going to be pushed as whole, thus other
			 * clauses won't be able to find out this TLE in the results
			 * obtained from Datanode. Hence can't optimize this query.
			 * PGXCTODO: with projection support in RemoteQuery node, this
			 * condition can be worked around, please check.
			 */
			if (local_tle->ressortgroupref > 0)
			{
				shippable_remote_tlist = false;
				break;
			}

			aggs_n_vars = pull_var_clause(expr, PVC_INCLUDE_AGGREGATES,
															PVC_RECURSE_PLACEHOLDERS);
			/* copy the aggregates into the remote target list */
			foreach (lcell, aggs_n_vars)
			{
				Assert(IsA(lfirst(lcell), Aggref) || IsA(lfirst(lcell), Var));
				remote_tlist = pgxc_add_node_to_grouping_tlist(remote_tlist, lfirst(lcell),
																0);
			}
		}
		/* Expression doesn't contain any aggregate */
		else
			remote_tlist = pgxc_add_node_to_grouping_tlist(remote_tlist, expr,
													local_tle->ressortgroupref);
	}

	if (!shippable_remote_tlist)
	{
		/*
		 * If local_tlist has changed but we didn't find anything shippable to
		 * Datanode, we need to restore the local_tlist to original state,
		 */
		if (orig_local_tlist)
			*local_tlist = orig_local_tlist;
		if (remote_tlist)
			list_free_deep(remote_tlist);
		remote_tlist = NIL;
	}
	else if (orig_local_tlist)
	{
		/*
		 * If we have changed the targetlist passed, we need to pass back the
		 * changed targetlist. Free the copy that has been created.
		 */
		list_free_deep(orig_local_tlist);
	}

	return remote_tlist;
}

/*
 * pgxc_process_having_clause
 * For every expression in the havingQual take following action
 * 1. If it has aggregates, which can be evaluated at the Datanodes, add those
 *    aggregates to the targetlist and modify the local aggregate expressions to
 *    point to the aggregate expressions being pushed to the Datanode. Add this
 *    expression to the local qual to be evaluated locally.
 * 2. If the expression does not have aggregates and the whole expression can be
 *    evaluated at the Datanode, add the expression to the remote qual to be
 *    evaluated at the Datanode.
 * 3. If qual contains an expression which can not be evaluated at the data
 *    node, the parent group plan can not be reduced to a remote_query.
 */
static List *
pgxc_process_having_clause(PlannerInfo *root, List *remote_tlist, Node *havingQual,
												List **local_qual, List **remote_qual,
												bool *reduce_plan)
{
	List		*qual;
	ListCell	*temp;

	*reduce_plan = true;
	*remote_qual = NIL;
	*local_qual = NIL;

	if (!havingQual)
		return remote_tlist;
	/*
	 * PGXCTODO: we expect the quals in the form of List only. Is there a
	 * possibility that the quals will be another form?
	 */
	if (!IsA(havingQual, List))
	{
		*reduce_plan = false;
		return remote_tlist;
	}
	/*
	 * Copy the havingQual so that the copy can be modified later. In case we
	 * back out in between, the original expression remains intact.
	 */
	qual = copyObject(havingQual);
	foreach(temp, qual)
	{
		Node	*expr = lfirst(temp);
		bool	has_aggs;
		List	*vars_n_aggs;

		if (!pgxc_is_expr_shippable((Expr *)expr, &has_aggs))
		{
			*reduce_plan = false;
			break;
		}

		if (has_aggs)
		{
			ListCell	*lcell;

			/* Pull the aggregates and var nodes from the quals */
			vars_n_aggs = pull_var_clause(expr, PVC_INCLUDE_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS);
			/* copy the aggregates into the remote target list */
			foreach (lcell, vars_n_aggs)
			{
				Assert(IsA(lfirst(lcell), Aggref) || IsA(lfirst(lcell), Var));
				remote_tlist = pgxc_add_node_to_grouping_tlist(remote_tlist, lfirst(lcell),
																0);
			}
			*local_qual = lappend(*local_qual, expr);
		}
		else
			*remote_qual = lappend(*remote_qual, expr);
	}

	if (!(*reduce_plan))
		list_free_deep(qual);

	return remote_tlist;
}

/*
 * pgxc_set_en_expr
 * Try to find the expression of distribution column to calculate node at plan execution
 */
static Expr *
pgxc_set_en_expr(Oid tableoid, Index resultRelationIndex)
{
	HeapTuple tp;
	Form_pg_attribute partAttrTup;
	Var	*var;
	RelationLocInfo *rel_loc_info;

	/* Get location info of the target table */
	rel_loc_info = GetRelationLocInfo(tableoid);
	if (rel_loc_info == NULL)
		 return NULL;

	/*
	 * For hash/modulo distributed tables, the target node must be selected
	 * at the execution time based on the partition column value.
	 *
	 * For round robin distributed tables, tuples must be divided equally
	 * between the nodes.
	 *
	 * For replicated tables, tuple must be inserted in all the Datanodes
	 *
	 * XXX Need further testing for replicated and round-robin tables
	 */
	if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH &&
		rel_loc_info->locatorType != LOCATOR_TYPE_MODULO)
		return NULL;

	tp = SearchSysCache(ATTNUM,
						ObjectIdGetDatum(tableoid),
						Int16GetDatum(rel_loc_info->partAttrNum),
						0, 0);
	partAttrTup = (Form_pg_attribute) GETSTRUCT(tp);

	/*
	 * Create a Var for the distribution column and set it for
	 * execution time evaluation of target node. ExecEvalVar() picks
	 * up values from ecxt_scantuple if Var does not refer either OUTER
	 * or INNER varno. We utilize that mechanism to pick up values from
	 * the tuple returned by the current plan node
	 */
	var = makeVar(resultRelationIndex,
				  rel_loc_info->partAttrNum,
				  partAttrTup->atttypid,
				  partAttrTup->atttypmod,
				  partAttrTup->attcollation,
				  0);
	ReleaseSysCache(tp);

	return (Expr *) var;
}

/*
 * pgxc_count_rowmarks_entries
 * Count the number of rowmarks that need to be added as prepared parameters
 * for remote DML plan
 */
static int
pgxc_count_rowmarks_entries(List *rowMarks)
{
	int res = 0;
	ListCell *elt;

	foreach(elt, rowMarks)
	{
		PlanRowMark *rc = (PlanRowMark *) lfirst(elt);

		/* RowMarks with different parent are not needed */
		if (rc->rti != rc->prti)
			continue;

		/*
		 * Count the entry and move to next element
		 * For a non-parent rowmark, only ctid is used.
		 * For a parent rowmark, ctid and tableoid are used.
		 */
		if (!rc->isParent)
			res++;
		else
			res = res + 2;
	}

	return res;
}

/*
 * pgxc_build_rowmark_entries
 * Complete type array for SetRemoteStatementName based on given RowMarks list
 * The list of total parameters is calculated based on the current number of prepared
 * parameters and the rowmark list.
 */
static Oid *
pgxc_build_rowmark_entries(List *rowMarks, List *rtable, Oid *types, int prepparams, int totparams)
{
	Oid *newtypes = types;
	int rowmark_entry_num;
	int count = prepparams;
	ListCell *elt;

	/* No modifications is list is empty */
	if (rowMarks == NIL)
		return newtypes;

	/* Nothing to do, total number of parameters is already correct */
	if (prepparams == totparams)
		return newtypes;

	/* Fetch number of extra entries related to Rowmarks */
	rowmark_entry_num = pgxc_count_rowmarks_entries(rowMarks);

	/* Nothing to do */
	if (rowmark_entry_num == 0)
		return newtypes;

	/* This needs to be absolutely verified */
	Assert(totparams == (prepparams + rowmark_entry_num));

	foreach(elt, rowMarks)
	{
		PlanRowMark *rc = (PlanRowMark *) lfirst(elt);

		/* RowMarks with different parent are not needed */
		if (rc->rti != rc->prti)
			continue;

		/* Determine the correct parameter type */
		switch (rc->markType)
		{
			case ROW_MARK_COPY:
			{
				RangeTblEntry *rte = rt_fetch(rc->prti, rtable);

				/*
				 * PGXCTODO: We still need to determine the rowtype
				 * in case relation involved here is a view (see inherit.sql).
				 */
				if (!OidIsValid(rte->relid))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("Cannot generate remote query plan"),
							 errdetail("This relation rowtype cannot be fetched")));

				/*
				 * This is the complete copy of a row, so it is necessary
				 * to set parameter as a rowtype
				 */
				count++;
				newtypes[count - 1] = get_rel_type_id(rte->relid);
			}
			break;

			case ROW_MARK_REFERENCE:
				/* Here we have a ctid for sure */
				count++;
				newtypes[count - 1] = TIDOID;

				if (rc->isParent)
				{
					/* For a parent table, tableoid is also necessary */
					count++;
					/* Set parameter type */
					newtypes[count - 1] = OIDOID;
				}
				break;

			/* Ignore other entries */
			case ROW_MARK_SHARE:
			case ROW_MARK_EXCLUSIVE:
				default:
				break;
		}
	}

	/* This should not happen */
	if (count != totparams)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("Error when generating remote query plan")));

	return newtypes;
}

static RangeTblEntry *
make_dummy_remote_rte(char *relname, Alias *alias)
{
	RangeTblEntry *dummy_rte = makeNode(RangeTblEntry);
	dummy_rte->rtekind = RTE_REMOTE_DUMMY;

	/* use a dummy relname... */
	dummy_rte->relname		 = relname;
	dummy_rte->eref			 = alias;

	return dummy_rte;
}
