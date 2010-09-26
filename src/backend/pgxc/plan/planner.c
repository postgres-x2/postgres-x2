/*-------------------------------------------------------------------------
 *
 * planner.c
 *
 *	  Functions for generating a PGXC style plan.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
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
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "pgxc/locator.h"
#include "pgxc/planner.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * Convenient format for literal comparisons
 *
 * PGXCTODO - make constant type Datum, handle other types
 */
typedef struct
{
	Oid			relid;
	RelationLocInfo *rel_loc_info;
	Oid			attrnum;
	char	   *col_name;
	long		constant;		/* assume long PGXCTODO - should be Datum */
} Literal_Comparison;

/* Parent-Child joins for relations being joined on
 * their respective hash distribuion columns
 */
typedef struct
{
	RelationLocInfo *rel_loc_info1;
	RelationLocInfo *rel_loc_info2;
	OpExpr			*opexpr;
} Parent_Child_Join;

/*
 * This struct helps us detect special conditions to determine what nodes
 * to execute on.
 */
typedef struct
{
	List	   *partitioned_literal_comps;		/* List of Literal_Comparison */
	List	   *partitioned_parent_child;		/* List of Parent_Child_Join */
	List	   *replicated_joins;

	/*
	 * Used when joining a single replicated or non-replicated table with
	 * other replicated tables. Use as a basis for partitioning determination.
	 */
	char	   *base_rel_name;
	RelationLocInfo *base_rel_loc_info;

} Special_Conditions;

/* If two relations are joined based on special location information */
typedef enum PGXCJoinType
{
	JOIN_REPLICATED,
	JOIN_COLOCATED_PARTITIONED,
	JOIN_OTHER
} PGXCJoinType;

/* used to track which tables are joined */
typedef struct
{
	int			relid1;			/* the first relation */
	char	   *aliasname1;
	int			relid2;			/* the second relation */
	char	   *aliasname2;

	PGXCJoinType join_type;
} PGXC_Join;

/* used for base column in an expression */
typedef struct ColumnBase
{
	int 	relid;
	char	*relname;
	char	*relalias;
	char	*colname;
} ColumnBase;

/* Used for looking for XC-safe queries
 *
 * rtables is a pointer to List, each item of which is
 * the rtable for the particular query. This way we can use
 * varlevelsup to resolve Vars in nested queries
 */
typedef struct XCWalkerContext
{
	Query				   *query;
	bool					isRead;
	ExecNodes 			   *exec_nodes;	/* resulting execution nodes */
	Special_Conditions 	   *conditions;
	bool					multilevel_join;
	List 				   *rtables;	/* a pointer to a list of rtables */
	int					    varno;
	bool				    within_or;
	bool				    within_not;
	bool					exec_on_coord; /* fallback to standard planner to have plan executed on coordinator only */
	List				   *join_list;   /* A list of List*'s, one for each relation. */
} XCWalkerContext;


/* Forbid unsafe SQL statements */
bool		StrictStatementChecking = true;

/* Forbid multi-node SELECT statements with an ORDER BY clause */
bool		StrictSelectChecking = false;


static ExecNodes *get_plan_nodes(Query *query, bool isRead);
static bool get_plan_nodes_walker(Node *query_node, XCWalkerContext *context);
static bool examine_conditions_walker(Node *expr_node, XCWalkerContext *context);
static int handle_limit_offset(RemoteQuery *query_step, Query *query, PlannedStmt *plan_stmt);

/*
 * True if both lists contain only one node and are the same
 */
static bool
same_single_node (List *nodelist1, List *nodelist2)
{
	return nodelist1 && list_length(nodelist1) == 1
			&& nodelist2 && list_length(nodelist2) == 1
			&& linitial_int(nodelist1) == linitial_int(nodelist2);
}

/*
 * Create a new join struct for tracking how relations are joined
 */
static PGXC_Join *
new_pgxc_join(int relid1, char *aliasname1, int relid2, char *aliasname2)
{
	PGXC_Join   *pgxc_join = (PGXC_Join *) palloc(sizeof(PGXC_Join));

	if (relid1 < relid2)
	{
		pgxc_join->relid1 = relid1;
		pgxc_join->relid2 = relid2;
		pgxc_join->aliasname1 = aliasname1;
		pgxc_join->aliasname2 = aliasname2;
	}
	else
	{
		pgxc_join->relid1 = relid2;
		pgxc_join->relid2 = relid1;
		pgxc_join->aliasname1 = aliasname2;
		pgxc_join->aliasname2 = aliasname1;
	}

	pgxc_join->join_type = JOIN_OTHER;

	return pgxc_join;
}


/*
 * Look up the join struct for a particular join
 */
static PGXC_Join *
find_pgxc_join(int relid1, char *aliasname1, int relid2, char *aliasname2, XCWalkerContext *context)
{
	ListCell   *lc;

	/* return if list is still empty */
	if (context->join_list == NULL)
		return NULL;

	/* in the PGXC_Join struct, we always sort with relid1 < relid2 */
	if (relid2 < relid1)
	{
		int			tmp = relid1;
		char	   *tmpalias = aliasname1;

		relid1 = relid2;
		aliasname1 = aliasname2;
		relid2 = tmp;
		aliasname2 = tmpalias;
	}

	/*
	 * there should be a small number, so we just search linearly, although
	 * long term a hash table would be better.
	 */
	foreach(lc, context->join_list)
	{
		PGXC_Join   *pgxcjoin = (PGXC_Join *) lfirst(lc);

		if (pgxcjoin->relid1 == relid1 && pgxcjoin->relid2 == relid2
			&& !strcmp(pgxcjoin->aliasname1, aliasname1)
			&& !strcmp(pgxcjoin->aliasname2, aliasname2))
			return pgxcjoin;
	}
	return NULL;
}

/*
 * Find or create a join between 2 relations
 */
static PGXC_Join *
find_or_create_pgxc_join(int relid1, char *aliasname1, int relid2, char *aliasname2, XCWalkerContext *context)
{
	PGXC_Join   *pgxcjoin;

	pgxcjoin = find_pgxc_join(relid1, aliasname1, relid2, aliasname2, context);

	if (pgxcjoin == NULL)
	{
		pgxcjoin = new_pgxc_join(relid1, aliasname1, relid2, aliasname2);
		context->join_list = lappend(context->join_list, pgxcjoin);
	}

	return pgxcjoin;
}


/*
 * new_special_conditions - Allocate Special_Conditions struct and initialize
 */
static Special_Conditions *
new_special_conditions(void)
{
	Special_Conditions *special_conditions =
	(Special_Conditions *) palloc0(sizeof(Special_Conditions));

	return special_conditions;
}

/*
 * free Special_Conditions struct
 */
static void
free_special_relations(Special_Conditions *special_conditions)
{
	if (special_conditions == NULL)
		return;

	/* free all items in list, including Literal_Comparison struct */
	list_free_deep(special_conditions->partitioned_literal_comps);

	/* free list, but not items pointed to */
	list_free(special_conditions->partitioned_parent_child);
	list_free(special_conditions->replicated_joins);

	pfree(special_conditions);
	special_conditions = NULL;
}

/*
 * frees join_list
 */
static void
free_join_list(List *join_list)
{
	if (join_list == NULL)
		return;

	list_free_deep(join_list);
	join_list = NULL;
}

/*
 * get_numeric_constant - extract casted constant
 *
 * Searches an expression to see if it is a Constant that is being cast
 * to numeric.  Return a pointer to the Constant, or NULL.
 * We need this because of casting.
 */
static Expr *
get_numeric_constant(Expr *expr)
{
	if (expr == NULL)
		return NULL;

	if (IsA(expr, Const))
		return expr;

	/* We may have a cast, represented by a function */
	if (IsA(expr, FuncExpr))
	{
		FuncExpr   *funcexpr = (FuncExpr *) expr;

		/* try and get at what is being cast  */
		/* We may have an implicit double-cast, so we do this recurisvely */
		if (funcexpr->funcid == F_NUMERIC || funcexpr->funcid == F_INT4_NUMERIC)
		{
			return get_numeric_constant(linitial(funcexpr->args));
		}
	}

	return NULL;
}


/*
 * get_base_var_table_and_column - determine the base table and column
 *
 * This is required because a RangeTblEntry may actually be another
 * type, like a join, and we need to then look at the joinaliasvars
 * to determine what the base table and column really is.
 *
 * rtables is a List of rtable Lists.
 */
static ColumnBase*
get_base_var(Var *var, XCWalkerContext *context)
{
	RangeTblEntry *rte;
	List *col_rtable;

	/* Skip system attributes */
	if (!AttrNumberIsForUserDefinedAttr(var->varattno))
		return NULL;

	/*
	 * Get the RangeTableEntry
	 * We take nested subqueries into account first,
	 * we may need to look further up the query tree.
	 * The most recent rtable is at the end of the list; top most one is first.
	 */
	Assert (list_length(context->rtables) - var->varlevelsup > 0);
	col_rtable = list_nth(context->rtables, (list_length(context->rtables) - var->varlevelsup) - 1);
	rte = list_nth(col_rtable, var->varno - 1);

	if (rte->rtekind == RTE_RELATION)
	{
		ColumnBase *column_base = (ColumnBase *) palloc0(sizeof(ColumnBase));

		column_base->relid = rte->relid;
		column_base->relname = get_rel_name(rte->relid);
		column_base->colname = strVal(list_nth(rte->eref->colnames,
										   var->varattno - 1));
		column_base->relalias = rte->eref->aliasname;
		return column_base;
	}
	else if (rte->rtekind == RTE_JOIN)
	{
		Var		   *colvar = list_nth(rte->joinaliasvars, var->varattno - 1);

		/* continue resolving recursively */
		return get_base_var(colvar, context);
	}
	else if (rte->rtekind == RTE_SUBQUERY)
	{
		/*
		 * Handle views like select * from v1 where col1 = 1
		 * where col1 is partition column of base relation
		 */
		/* the varattno corresponds with the subquery's target list (projections) */
		TargetEntry *tle = list_nth(rte->subquery->targetList, var->varattno - 1); /* or varno? */

		if (!IsA(tle->expr, Var))
			return NULL;	/* not column based expressoin, return */
		else
		{
			ColumnBase *base;
			Var *colvar = (Var *) tle->expr;

			/* continue resolving recursively */
			/* push onto rtables list */
			context->rtables = lappend(context->rtables, rte->subquery->rtable);
			base = get_base_var(colvar, context);
			/* pop from rtables list */
			context->rtables = list_delete_ptr(context->rtables, rte->subquery->rtable);
			return base;
		}
	}

	return NULL;
}


/*
 * get_plan_nodes_insert - determine nodes on which to execute insert.
 */
static ExecNodes *
get_plan_nodes_insert(Query *query)
{
	RangeTblEntry *rte;
	RelationLocInfo *rel_loc_info;
	Const	   *constant;
	ExecNodes  *exec_nodes;
	ListCell   *lc;
	long		part_value;
	long	   *part_value_ptr = NULL;
	Expr	   *eval_expr = NULL;

	/* Looks complex (correlated?) - best to skip */
	if (query->jointree != NULL && query->jointree->fromlist != NULL)
		return NULL;

	/* Make sure there is just one table */
	if (query->rtable == NULL)
		return NULL;

	rte = (RangeTblEntry *) list_nth(query->rtable, query->resultRelation - 1);


	if (rte != NULL && rte->rtekind != RTE_RELATION)
		/* Bad relation type */
		return NULL;

	/* See if we have the partitioned case. */
	rel_loc_info = GetRelationLocInfo(rte->relid);

	if (!rel_loc_info)
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
			  (errmsg("Could not find relation for oid = %d", rte->relid))));

	if (rel_loc_info->locatorType == LOCATOR_TYPE_HASH &&
		rel_loc_info->partAttrName != NULL)
	{
		/* It is a partitioned table, get value by looking in targetList */
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
			{
				/* We may have a cast, try and handle it */
				Expr	   *checkexpr = tle->expr;

				if (!IsA(tle->expr, Const))
				{
					eval_expr = (Expr *) eval_const_expressions(NULL, (Node *) tle->expr);
					checkexpr = get_numeric_constant(eval_expr);
				}

				if (checkexpr == NULL)
					break;		/* no constant */

				constant = (Const *) checkexpr;

				if (constant->consttype == INT4OID ||
					constant->consttype == INT2OID ||
					constant->consttype == INT8OID)
				{
					part_value = (long) constant->constvalue;
					part_value_ptr = &part_value;

				}
				/* PGXCTODO - handle other data types */
				/*
				else
					if (constant->consttype == VARCHAR ...
				*/
			}
		}
	}

	/* single call handles both replicated and partitioned types */
	exec_nodes = GetRelationNodes(rel_loc_info, part_value_ptr, false);

	if (eval_expr)
		pfree(eval_expr);

	return exec_nodes;
}


/*
 * examine_conditions_walker
 *
 * Examine conditions and find special ones to later help us determine
 * what tables can be joined together. Put findings in Special_Conditions
 * struct.
 *
 * Get list of constant comparisons conditions on partitioned column
 * Get list of parent-child joins (partitioned together)
 * Get list of joins with replicated tables
 *
 * If we encounter a cross-node join, we stop processing and return false,
 * otherwise true.
 *
 */
static bool
examine_conditions_walker(Node *expr_node, XCWalkerContext *context)
{
	RelationLocInfo *rel_loc_info1,
			   *rel_loc_info2;
	Const	   *constant;
	Expr	   *checkexpr;
	bool		result = false;
	bool		is_and = false;


	Assert(context);

	if (expr_node == NULL)
		return false;

	if (!context->rtables)
		return true;

	if (!context->conditions)
		context->conditions = new_special_conditions();

	if (IsA(expr_node, Var))
	{
		/* If we get here, that meant the previous call before recursing down did not
		 * find the condition safe yet.
		 * Since we pass down our context, this is the bit of code that will detect
		 * that we are using more than one relation in a condition which has not
		 * already been deemed safe.
		 */
		Var *var_node = (Var *) expr_node;

		if (context->varno)
		{
			if (var_node->varno != context->varno)
				return true;
		}
		else
		{
			context->varno = var_node->varno;
			return false;
		}
	}

	else if (IsA(expr_node, BoolExpr))
	{
		BoolExpr   *boolexpr = (BoolExpr *) expr_node;

		if (boolexpr->boolop == AND_EXPR)
			is_and = true;
		if (boolexpr->boolop == NOT_EXPR)
		{
			bool save_within_not = context->within_not;
			context->within_not = true;

			if (examine_conditions_walker(linitial(boolexpr->args), context))
			{
				context->within_not = save_within_not;
				return true;
			}
			context->within_not = save_within_not;
			return false;
		}
		else if (boolexpr->boolop == OR_EXPR)
		{
			bool save_within_or = context->within_or;
			context->within_or = true;

			if (examine_conditions_walker(linitial(boolexpr->args), context))
			{
				context->within_or = save_within_or;
				return true;
			}

			if (examine_conditions_walker(lsecond(boolexpr->args), context))
			{
				context->within_or = save_within_or;
				return true;
			}
			context->within_or = save_within_or;
			return false;
		}
	}

	/*
	 * Look for equality conditions on partiioned columns, but only do so
	 * if we are not in an OR or NOT expression
	 */
	if (!context->within_or && !context->within_not && IsA(expr_node, OpExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) expr_node;

		/* See if we can equijoin these */
		if (op_mergejoinable(opexpr->opno) && opexpr->args->length == 2)
		{
			Expr	   *arg1 = linitial(opexpr->args);
			Expr	   *arg2 = lsecond(opexpr->args);

			/* Look for a table */
			if (IsA(arg1, Var))
			{
				/* get the RangeTableEntry */
				Var		   *colvar = (Var *) arg1;

				ColumnBase *column_base = get_base_var(colvar, context);

				if (!column_base)
					return true;

				/* Look at other argument */
				checkexpr = arg2;

				/* We may have a cast or expression, try and handle it */
				if (!IsA(arg2, Const))
				{
					/* this gets freed when the memory context gets freed */
					Expr *eval_expr = (Expr *) eval_const_expressions(NULL, (Node *) arg2);
					checkexpr = get_numeric_constant(eval_expr);
				}

				if (checkexpr != NULL)
					arg2 = checkexpr;

				if (IsA(arg2, Const))
				{
					/* We have column = literal. Check if partitioned case */
					constant = (Const *) arg2;

					rel_loc_info1 = GetRelationLocInfo(column_base->relid);

					if (!rel_loc_info1)
						return true;

					/* If hash partitioned, check if the part column was used */
					if (IsHashColumn(rel_loc_info1, column_base->colname))
					{
						/* add to partitioned literal join conditions */
						Literal_Comparison *lit_comp =
						palloc(sizeof(Literal_Comparison));

						lit_comp->relid = column_base->relid;
						lit_comp->rel_loc_info = rel_loc_info1;
						lit_comp->col_name = column_base->colname;
						lit_comp->constant = constant->constvalue;

						context->conditions->partitioned_literal_comps = lappend(
									   context->conditions->partitioned_literal_comps,
																   lit_comp);

						return false;
					}
					else
					{
						/* Continue walking below */
						if (rel_loc_info1)
							FreeRelationLocInfo(rel_loc_info1);
					}

				}
				else if (IsA(arg2, Var))
				{
					PGXC_Join   *pgxc_join;
					ColumnBase *column_base2;
					Var		   *colvar2 = (Var *) arg2;

					rel_loc_info1 = GetRelationLocInfo(column_base->relid);

					if (!rel_loc_info1)
						return true;

					column_base2 = get_base_var(colvar2, context);
					if (!column_base2)
						return true;
					rel_loc_info2 = GetRelationLocInfo(column_base2->relid);

					/* get data struct about these two relations joining */
					pgxc_join = find_or_create_pgxc_join(column_base->relid, column_base->relalias,
										 column_base2->relid, column_base2->relalias, context);

					if (rel_loc_info1->locatorType == LOCATOR_TYPE_REPLICATED)
					{
						/* add to replicated join conditions */
						context->conditions->replicated_joins =
							lappend(context->conditions->replicated_joins, opexpr);

						if (colvar->varlevelsup != colvar2->varlevelsup)
							context->multilevel_join = true;

						if (rel_loc_info2->locatorType != LOCATOR_TYPE_REPLICATED)
						{
							/* Note other relation, saves us work later. */
							context->conditions->base_rel_name = column_base2->relname;
							context->conditions->base_rel_loc_info = rel_loc_info2;
							if (rel_loc_info1)
								FreeRelationLocInfo(rel_loc_info1);
						}

						if (context->conditions->base_rel_name == NULL)
						{
							context->conditions->base_rel_name = column_base->relname;
							context->conditions->base_rel_loc_info = rel_loc_info1;
							if (rel_loc_info2)
								FreeRelationLocInfo(rel_loc_info2);
						}

						/* note nature of join between the two relations */
						pgxc_join->join_type = JOIN_REPLICATED;
						return false;
					}
					else if (rel_loc_info2->locatorType == LOCATOR_TYPE_REPLICATED)
					{
						/* add to replicated join conditions */
						context->conditions->replicated_joins =
							lappend(context->conditions->replicated_joins, opexpr);

						/* other relation not replicated, note it for later */
						context->conditions->base_rel_name = column_base->relname;
						context->conditions->base_rel_loc_info = rel_loc_info1;

						/* note nature of join between the two relations */
						pgxc_join->join_type = JOIN_REPLICATED;

						if (rel_loc_info2)
							FreeRelationLocInfo(rel_loc_info2);

						return false;
					}
					/* Now check for a partitioned join */
					/*
					 * PGXCTODO - for the prototype, we assume all partitioned
					 * tables are on the same nodes.
					 */
					if (IsHashColumn(rel_loc_info1, column_base->colname)
						&& IsHashColumn(rel_loc_info2, column_base2->colname))
					{
						/* We found a partitioned join */
						Parent_Child_Join *parent_child = (Parent_Child_Join *)
								palloc0(sizeof(Parent_Child_Join));

						parent_child->rel_loc_info1 = rel_loc_info1;
						parent_child->rel_loc_info2 = rel_loc_info2;
						parent_child->opexpr = opexpr;

						context->conditions->partitioned_parent_child =
							lappend(context->conditions->partitioned_parent_child,
									parent_child);
						pgxc_join->join_type = JOIN_COLOCATED_PARTITIONED;
						if (colvar->varlevelsup != colvar2->varlevelsup)
							context->multilevel_join = true;
						return false;
					}

					/*
					 * At this point, there is some other type of join that
					 * can probably not be executed on only a single node.
					 * Just return, as it may be updated later.
					 * Important: We preserve previous
					 * pgxc_join->join_type value, there may be multiple
					 * columns joining two tables, and we want to make sure at
					 * least one of them make it colocated partitioned, in
					 * which case it will update it when examining another
					 * condition.
					 */
					return false;
				}
			}
		}
	}

	/* Handle subquery */
	if (IsA(expr_node, SubLink))
	{
		List *current_rtable;
		bool is_multilevel;
		int save_parent_child_count = 0;
		SubLink *sublink = (SubLink *) expr_node;
		ExecNodes *save_exec_nodes = context->exec_nodes; /* Save old exec_nodes */

		/* save parent-child count */
		if (context->exec_nodes)
			save_parent_child_count = list_length(context->conditions->partitioned_parent_child);

		context->exec_nodes = NULL;
		context->multilevel_join = false;
		current_rtable = ((Query *) sublink->subselect)->rtable;

		/* push onto rtables list before recursing */
		context->rtables = lappend(context->rtables, current_rtable);

		if (get_plan_nodes_walker(sublink->subselect, context))
			return true;

		/* pop off (remove) rtable */
		context->rtables = list_delete_ptr(context->rtables, current_rtable);

		is_multilevel = context->multilevel_join;
		context->multilevel_join = false;

		/* Allow for replicated tables */
		if (!context->exec_nodes)
			context->exec_nodes = save_exec_nodes;
		else
		{
			if (save_exec_nodes)
			{
				if (context->exec_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED)
				{
					context->exec_nodes = save_exec_nodes;
				}
				else
				{
					if (save_exec_nodes->tableusagetype != TABLE_USAGE_TYPE_USER_REPLICATED)
					{
						/* See if they run on the same node */
						if (same_single_node (context->exec_nodes->nodelist, save_exec_nodes->nodelist))
							return false;
					}
					else
						/* use old value */
						context->exec_nodes = save_exec_nodes;
				}
			} else
			{
				if (context->exec_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED)
					return false;
				/* See if subquery safely joins with parent */
				if (!is_multilevel)
					return true;
			}
		}
	}

	/* Keep on walking */
	result = expression_tree_walker(expr_node, examine_conditions_walker, (void *) context);

	/* Reset context->varno if is_and to detect cross-node operations */
	if (is_and)
		context->varno = 0;

	return result;
}


/*
 * examine_conditions_fromlist - Examine FROM clause for joins
 *
 * Examine FROM clause join conditions to determine special conditions
 * to help us decide which nodes to execute on.
 */
static bool
examine_conditions_fromlist(Node *treenode, XCWalkerContext *context)
{
	if (treenode == NULL)
		return false;

	if (context->rtables == NULL)
		return false;

	if (IsA(treenode, JoinExpr))
	{
		JoinExpr   *joinexpr = (JoinExpr *) treenode;

		/* recursively examine FROM join tree */
		if (examine_conditions_fromlist(joinexpr->larg, context))
			return true;

		if (examine_conditions_fromlist(joinexpr->rarg, context))
			return true;

		/* Now look at join condition */
		if (examine_conditions_walker(joinexpr->quals, context))
			return true;

		return false;
	}
	else if (IsA(treenode, RangeTblRef))
		return false;
	else if (IsA(treenode, BoolExpr) ||IsA(treenode, OpExpr))
	{
		/* check base condition, if possible */
		if (examine_conditions_walker(treenode, context));
			return true;
	}

	/* Some other more complicated beast */
	return true;
}


/*
 * Returns whether or not the rtable (and its subqueries)
 * only contain pg_catalog entries.
 */
static bool
contains_only_pg_catalog (List *rtable)
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
		} else if (rte->rtekind == RTE_SUBQUERY &&
				!contains_only_pg_catalog (rte->subquery->rtable))
			return false;
	}
	return true;
}


/*
 * get_plan_nodes - determine the nodes to execute the command on.
 *
 * Examines the "special" query conditions in determining execution node list.
 *
 * returns NULL if it appears to be a mutli-step query.
 */
static bool
get_plan_nodes_walker(Node *query_node, XCWalkerContext *context)
{
	Query *query;
	RangeTblEntry *rte;
	ListCell   *lc,
			   *item;
	RelationLocInfo *rel_loc_info;
	ExecNodes  *test_exec_nodes = NULL;
	ExecNodes  *current_nodes = NULL;
	ExecNodes  *from_query_nodes = NULL;
	TableUsageType 	table_usage_type = TABLE_USAGE_TYPE_NO_TABLE;
	TableUsageType 	current_usage_type = TABLE_USAGE_TYPE_NO_TABLE;
	int 		from_subquery_count = 0;


	if (!query_node && !IsA(query_node,Query))
		return true;

	query = (Query *) query_node;

	/* If no tables, just return */
	if (query->rtable == NULL && query->jointree == NULL)
		return false;

	/* Look for special conditions */

	/* Look for JOIN syntax joins */
	foreach(item, query->jointree->fromlist)
	{
		Node	   *treenode = (Node *) lfirst(item);

		if (IsA(treenode, JoinExpr))
		{
			if (examine_conditions_fromlist(treenode, context))
			{
				/* May be complicated. Before giving up, just check for pg_catalog usage */
				if (contains_only_pg_catalog (query->rtable))
				{
					/* just pg_catalog tables */
					context->exec_nodes = makeNode(ExecNodes);
					context->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_PGCATALOG;
					context->exec_on_coord = true;
					return false;
				}

				/* complicated */
				return true;
			}
		}
		else if (IsA(treenode, RangeTblRef))
		{
			RangeTblRef *rtr = (RangeTblRef *) treenode;

			/* look at the entity */
			RangeTblEntry *rte = list_nth(query->rtable, rtr->rtindex - 1);

			if (rte->rtekind == RTE_SUBQUERY)
			{
				ExecNodes *save_exec_nodes = context->exec_nodes;
				Special_Conditions *save_conditions = context->conditions; /* Save old conditions */
				List *current_rtable = rte->subquery->rtable;

				from_subquery_count++;

				/*
				 * Recursively call for subqueries.
				 * Note this also works for views, which are rewritten as subqueries.
				 */
				context->rtables = lappend(context->rtables, current_rtable);
				context->conditions = (Special_Conditions *) palloc0(sizeof(Special_Conditions));

				if (get_plan_nodes_walker((Node *) rte->subquery, context))
					return true;

				/* restore rtables and conditions */
				context->rtables = list_delete_ptr(context->rtables, current_rtable);
				context->conditions = save_conditions;

				current_nodes = context->exec_nodes;
				context->exec_nodes = save_exec_nodes;

				if (current_nodes)
					current_usage_type = current_nodes->tableusagetype;
				else
					/* could be complicated */
					return true;

				/* We compare to make sure that the subquery is safe to execute with previous-
				 * we may have multiple ones in the FROM clause.
				 * We handle the simple case of allowing multiple subqueries in the from clause,
				 * but only allow one of them to not contain replicated tables
				 */
				if (!from_query_nodes)
					from_query_nodes = current_nodes;
				else if (current_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED)
				{
					/* ok, safe */
					if (!from_query_nodes)
						from_query_nodes = current_nodes;
				}
				else
				{
					if (from_query_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED)
						from_query_nodes = current_nodes;
					else
					{
						/* Allow if they are both using one node, and the same one */
						if (!same_single_node (from_query_nodes->nodelist, current_nodes->nodelist))
							/* Complicated */
							return true;
					}
				}
			}
			else if (rte->rtekind == RTE_RELATION)
			{
					/* Look for pg_catalog tables */
					if (get_rel_namespace(rte->relid) == PG_CATALOG_NAMESPACE)
						current_usage_type = TABLE_USAGE_TYPE_PGCATALOG;
					else
						current_usage_type = TABLE_USAGE_TYPE_USER;
			}
			else if (rte->rtekind == RTE_FUNCTION)
			{
				/* See if it is a catalog function */
				FuncExpr *funcexpr = (FuncExpr *) rte->funcexpr;
				if (get_func_namespace(funcexpr->funcid) == PG_CATALOG_NAMESPACE)
					current_usage_type = TABLE_USAGE_TYPE_PGCATALOG;
				else
				{
					/* Complicated */
					return true;
				}
			}
			else
				/* could be complicated */
				return true;

			/* See if we have pg_catalog mixed with other tables */
			if (table_usage_type == TABLE_USAGE_TYPE_NO_TABLE)
				table_usage_type = current_usage_type;
			else if (current_usage_type != table_usage_type)
			{
				/* mixed- too complicated for us for now */
				return true;
			}
		}
		else
		{
			/* could be complicated */
			return true;
		}
	}

	/* If we are just dealing with pg_catalog, just return */
	if (table_usage_type == TABLE_USAGE_TYPE_PGCATALOG)
	{
		context->exec_nodes = makeNode(ExecNodes);
		context->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_PGCATALOG;
		context->exec_on_coord = true;
		return false;
	}

	/* Examine the WHERE clause, too */
	if (examine_conditions_walker(query->jointree->quals, context))
		return true;

	/* Examine join conditions, see if each join is single-node safe */
	if (context->join_list != NULL)
	{
		foreach(lc, context->join_list)
		{
			PGXC_Join   *pgxcjoin = (PGXC_Join *) lfirst(lc);

			/* If it is not replicated or parent-child, not single-node safe */
			if (pgxcjoin->join_type == JOIN_OTHER)
				return true;
		}
	}


	/* check for non-partitioned cases */
	if (context->conditions->partitioned_parent_child == NULL &&
		context->conditions->partitioned_literal_comps == NULL)
	{
		/*
		 * We have either a single table, just replicated tables, or a
		 * table that just joins with replicated tables, or something
		 * complicated.
		 */

		/* See if we noted a table earlier to use */
		rel_loc_info = context->conditions->base_rel_loc_info;

		if (rel_loc_info == NULL)
		{
			RangeTblEntry *rtesave = NULL;

			foreach(lc, query->rtable)
			{
				rte = (RangeTblEntry *) lfirst(lc);

				/*
				 * If the query is rewritten (which can be due to rules or views),
				 * ignore extra stuff. Also ignore subqueries we have processed
				 */
				if ((!rte->inFromCl && query->commandType == CMD_SELECT) || rte->rtekind != RTE_RELATION)
					continue;

				/* PGXCTODO - handle RTEs that are functions */
				if (rtesave)
					/*
					 * Too complicated, we have multiple relations that still
					 * cannot be joined safely
					 */
					return true;

				rtesave = rte;
			}

			if (rtesave)
			{
				/* a single table, just grab it */
				rel_loc_info = GetRelationLocInfo(rtesave->relid);

				if (!rel_loc_info)
					return true;

				context->exec_nodes = GetRelationNodes(rel_loc_info, NULL, context->isRead);
			}
		}
		else
		{
			context->exec_nodes = GetRelationNodes(rel_loc_info, NULL, context->isRead);
		}

		/* Note replicated table usage for determining safe queries */
		if (context->exec_nodes)
		{
			if (table_usage_type == TABLE_USAGE_TYPE_USER && IsReplicated(rel_loc_info))
				table_usage_type = TABLE_USAGE_TYPE_USER_REPLICATED;

			context->exec_nodes->tableusagetype = table_usage_type;
		}
	}
	/* check for partitioned col comparison against a literal */
	else if (list_length(context->conditions->partitioned_literal_comps) > 0)
	{
		context->exec_nodes = NULL;

		/*
		 * Make sure that if there are multiple such comparisons, that they
		 * are all on the same nodes.
		 */
		foreach(lc, context->conditions->partitioned_literal_comps)
		{
			Literal_Comparison *lit_comp = (Literal_Comparison *) lfirst(lc);

			test_exec_nodes = GetRelationNodes(
						lit_comp->rel_loc_info, &(lit_comp->constant), true);

			test_exec_nodes->tableusagetype = table_usage_type;
			if (context->exec_nodes == NULL)
				context->exec_nodes = test_exec_nodes;
			else
			{
				if (!same_single_node(context->exec_nodes->nodelist, test_exec_nodes->nodelist))
				{
					return true;
				}
			}
		}
	}
	else
	{
		/*
		 * At this point, we have partitioned parent child relationship, with
		 * no partitioned column comparison condition with a literal. We just
		 * use one of the tables as a basis for node determination.
		 */
		Parent_Child_Join *parent_child;

		parent_child = (Parent_Child_Join *)
				linitial(context->conditions->partitioned_parent_child);

		context->exec_nodes = GetRelationNodes(parent_child->rel_loc_info1, NULL, context->isRead);
		context->exec_nodes->tableusagetype = table_usage_type;
	}

	if (from_query_nodes)
	{
		if (!context->exec_nodes)
		{
			context->exec_nodes = from_query_nodes;
			return false;
		}
		/* Just use exec_nodes if the from subqueries are all replicated or using the exact
		 * same node
		 */
		else if (from_query_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED
					|| (same_single_node(from_query_nodes->nodelist, context->exec_nodes->nodelist)))
				return false;
		else
		{
			/* We allow views, where the (rewritten) subquery may be on all nodes,
			 * but the parent query applies a condition on the from subquery.
			 */
			if (list_length(query->jointree->fromlist) == from_subquery_count
					&& list_length(context->exec_nodes->nodelist) == 1)
				return false;
		}
		/* Too complicated, give up */
		return true;
	}

	return false;
}


/*
 * Top level entry point before walking query to determine plan nodes
 *
 */
static ExecNodes *
get_plan_nodes(Query *query, bool isRead)
{
	ExecNodes *result_nodes = NULL;
	XCWalkerContext context;


	context.query = query;
	context.isRead = isRead;
	context.exec_nodes = NULL;
	context.conditions = (Special_Conditions *) palloc0(sizeof(Special_Conditions));
	context.rtables = NIL;
	context.rtables = lappend(context.rtables, query->rtable);
	context.multilevel_join = false;
	context.varno = 0;
	context.within_or = false;
	context.within_not = false;
	context.exec_on_coord = false;
	context.join_list = NIL;

	if (!get_plan_nodes_walker((Node *) query, &context))
		result_nodes = context.exec_nodes;
	if (context.exec_on_coord && result_nodes)
	{
		pfree(result_nodes);
		result_nodes = NULL;
	}
	free_special_relations(context.conditions);
	free_join_list(context.join_list);
	return result_nodes;
}


/*
 * get_plan_nodes_command - determine the nodes to execute the plan on
 *
 * return NULL if it is not safe to be done in a single step.
 */
static ExecNodes *
get_plan_nodes_command(Query *query)
{
	ExecNodes *exec_nodes = NULL;

	switch (query->commandType)
	{
		case CMD_SELECT:
			exec_nodes = get_plan_nodes(query, true);
			break;

		case CMD_INSERT:
			exec_nodes = get_plan_nodes_insert(query);
			break;

		case CMD_UPDATE:
		case CMD_DELETE:
			/* treat as a select */
			exec_nodes = get_plan_nodes(query, false);
			break;

		default:
			return NULL;
	}

	return exec_nodes;
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
 * Get list of simple aggregates used.
 */
static List *
get_simple_aggregates(Query * query)
{
	List	   *simple_agg_list = NIL;

	/* Check for simple multi-node aggregate */
	if (query->hasAggs)
  	{
		ListCell   *lc;
		int			column_pos = 0;

		foreach (lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (IsA(tle->expr, Aggref))
			{
				/*PGXC borrowed this code from nodeAgg.c, see ExecInitAgg()*/
				SimpleAgg  *simple_agg;
				Aggref 	   *aggref = (Aggref *) tle->expr;
				HeapTuple	aggTuple;
				Form_pg_aggregate aggform;
				Oid			aggcollecttype;
				AclResult	aclresult;
				Oid			transfn_oid,
							finalfn_oid;
				Expr	   *transfnexpr,
						   *finalfnexpr;
				Datum		textInitVal;

				simple_agg = makeNode(SimpleAgg);
				simple_agg->column_pos = column_pos;
				initStringInfo(&simple_agg->valuebuf);
				simple_agg->aggref = aggref;

				aggTuple = SearchSysCache(AGGFNOID,
										  ObjectIdGetDatum(aggref->aggfnoid),
										  0, 0, 0);
				if (!HeapTupleIsValid(aggTuple))
					elog(ERROR, "cache lookup failed for aggregate %u",
						 aggref->aggfnoid);
				aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

				/* Check permission to call aggregate function */
				aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(),
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_PROC,
								   get_func_name(aggref->aggfnoid));

				simple_agg->transfn_oid = transfn_oid = aggform->aggcollectfn;
				simple_agg->finalfn_oid = finalfn_oid = aggform->aggfinalfn;

				/* Check that aggregate owner has permission to call component fns */
				{
					HeapTuple	procTuple;
					Oid			aggOwner;

					procTuple = SearchSysCache(PROCOID,
											   ObjectIdGetDatum(aggref->aggfnoid),
											   0, 0, 0);
					if (!HeapTupleIsValid(procTuple))
						elog(ERROR, "cache lookup failed for function %u",
							 aggref->aggfnoid);
					aggOwner = ((Form_pg_proc) GETSTRUCT(procTuple))->proowner;
					ReleaseSysCache(procTuple);

					aclresult = pg_proc_aclcheck(transfn_oid, aggOwner,
												 ACL_EXECUTE);
					if (aclresult != ACLCHECK_OK)
						aclcheck_error(aclresult, ACL_KIND_PROC,
									   get_func_name(transfn_oid));
					if (OidIsValid(finalfn_oid))
					{
						aclresult = pg_proc_aclcheck(finalfn_oid, aggOwner,
													 ACL_EXECUTE);
						if (aclresult != ACLCHECK_OK)
							aclcheck_error(aclresult, ACL_KIND_PROC,
										   get_func_name(finalfn_oid));
					}
				}

				/* resolve actual type of transition state, if polymorphic */
				aggcollecttype = aggform->aggcollecttype;

				/* build expression trees using actual argument & result types */
				build_aggregate_fnexprs(&aggform->aggtranstype,
										1,
										aggcollecttype,
										aggref->aggtype,
										transfn_oid,
										finalfn_oid,
										&transfnexpr,
										&finalfnexpr);

				/* Get InputFunction info for transition result */
				{
					Oid			typinput;

					getTypeInputInfo(aggform->aggtranstype, &typinput, &simple_agg->argioparam);
					fmgr_info(typinput, &simple_agg->arginputfn);
				}

				/* Get InputFunction info for result */
				{
					Oid			typoutput;
					bool		typvarlena;

					getTypeOutputInfo(simple_agg->aggref->aggtype, &typoutput, &typvarlena);
					fmgr_info(typoutput, &simple_agg->resoutputfn);
				}

				fmgr_info(transfn_oid, &simple_agg->transfn);
				simple_agg->transfn.fn_expr = (Node *) transfnexpr;

				if (OidIsValid(finalfn_oid))
				{
					fmgr_info(finalfn_oid, &simple_agg->finalfn);
					simple_agg->finalfn.fn_expr = (Node *) finalfnexpr;
				}

				get_typlenbyval(aggref->aggtype,
								&simple_agg->resulttypeLen,
								&simple_agg->resulttypeByVal);
				get_typlenbyval(aggcollecttype,
								&simple_agg->transtypeLen,
								&simple_agg->transtypeByVal);

				/*
				 * initval is potentially null, so don't try to access it as a struct
				 * field. Must do it the hard way with SysCacheGetAttr.
				 */
				textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
											  Anum_pg_aggregate_agginitcollect,
											  &simple_agg->initValueIsNull);

				if (simple_agg->initValueIsNull)
					simple_agg->initValue = (Datum) 0;
				else
				{
					Oid			typinput,
								typioparam;
					char	   *strInitVal;
					Datum		initVal;

					getTypeInputInfo(aggcollecttype, &typinput, &typioparam);
					strInitVal = TextDatumGetCString(textInitVal);
					initVal = OidInputFunctionCall(typinput, strInitVal,
												   typioparam, -1);
					pfree(strInitVal);
					simple_agg->initValue = initVal;
				}

				/*
				 * If the transfn is strict and the initval is NULL, make sure trans
				 * type and collect type are the same (or at least binary-compatible),
				 * so that it's OK to use the first input value as the initial
				 * transValue.	This should have been checked at agg definition time,
				 * but just in case...
				 */
				if (simple_agg->transfn.fn_strict && simple_agg->initValueIsNull)
				{
					if (!IsBinaryCoercible(aggform->aggtranstype, aggcollecttype))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
								 errmsg("aggregate %u needs to have compatible transition type and collection type",
										aggref->aggfnoid)));
				}

				/* PGXCTODO distinct support */

				ReleaseSysCache(aggTuple);

				simple_agg_list = lappend(simple_agg_list, simple_agg);
			}
			column_pos++;
  		}
  	}
	return simple_agg_list;
}


/*
 * add_sort_column --- utility subroutine for building sort info arrays
 *
 * We need this routine because the same column might be selected more than
 * once as a sort key column; if so, the extra mentions are redundant.
 *
 * Caller is assumed to have allocated the arrays large enough for the
 * max possible number of columns.	Return value is the new column count.
 *
 * PGXC: copied from optimizer/plan/planner.c
 */
static int
add_sort_column(AttrNumber colIdx, Oid sortOp, bool nulls_first,
				int numCols, AttrNumber *sortColIdx,
				Oid *sortOperators, bool *nullsFirst)
{
	int			i;

	if (!OidIsValid(sortOp))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an ordering operator")));


	for (i = 0; i < numCols; i++)
	{
		/*
		 * Note: we check sortOp because it's conceivable that "ORDER BY foo
		 * USING <, foo USING <<<" is not redundant, if <<< distinguishes
		 * values that < considers equal.  We need not check nulls_first
		 * however because a lower-order column with the same sortop but
		 * opposite nulls direction is redundant.
		 */
		if (sortColIdx[i] == colIdx && sortOperators[i] == sortOp)
		{
			/* Already sorting by this col, so extra sort key is useless */
			return numCols;
		}
	}

	/* Add the column */
	sortColIdx[numCols] = colIdx;
	sortOperators[numCols] = sortOp;
	nullsFirst[numCols] = nulls_first;
	return numCols + 1;
}

/*
 * add_distinct_column - utility subroutine to remove redundant columns, just
 * like add_sort_column
 */
static int
add_distinct_column(AttrNumber colIdx, Oid eqOp, int numCols,
					AttrNumber *sortColIdx, Oid *eqOperators)
{
	int			i;

	Assert(OidIsValid(eqOp));

	for (i = 0; i < numCols; i++)
	{
		if (sortColIdx[i] == colIdx && eqOperators[i] == eqOp)
		{
			/* Already sorting by this col, so extra sort key is useless */
			return numCols;
		}
	}

	/* Add the column */
	sortColIdx[numCols] = colIdx;
	eqOperators[numCols] = eqOp;
	return numCols + 1;
}


/*
 * Reconstruct the step query
 */
static void
reconstruct_step_query(List *rtable, bool has_order_by, List *extra_sort,
					   RemoteQuery *step)
{
	List	   *context;
	bool		useprefix;
	List	   *sub_tlist = step->scan.plan.targetlist;
	ListCell   *l;
	StringInfo	buf = makeStringInfo();
	char	   *sql;
	char	   *cur;
	char	   *sql_from;

	context = deparse_context_for_plan((Node *) step, NULL, rtable, NIL);
	useprefix = list_length(rtable) > 1;

	foreach(l, sub_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);
		char *exprstr = deparse_expression((Node *) tle->expr, context,
										   useprefix, false);

		if (buf->len == 0)
		{
			appendStringInfo(buf, "SELECT ");
			if (step->distinct)
				appendStringInfo(buf, "DISTINCT ");
		}
		else
			appendStringInfo(buf, ", ");

		appendStringInfoString(buf, exprstr);
	}

	/*
	 * A kind of dummy
	 * Do not reconstruct remaining query, just search original statement
	 * for " FROM " and append remainder to the target list we just generated.
	 * Do not handle the case if " FROM " we found is not a "FROM" keyword, but,
	 * for example, a part of string constant.
	 */
	sql = pstrdup(step->sql_statement); /* mutable copy */
	/* string to upper case, for comparing */
	cur = sql;
	while (*cur)
	{
		/* replace whitespace with a space */
		if (isspace((unsigned char) *cur))
			*cur = ' ';
		*cur++ = toupper(*cur);
	}

	/* find the keyword */
	sql_from = strstr(sql, " FROM ");
	if (sql_from)
	{
		/* the same offset in the original string */
		int		offset = sql_from - sql;
		/*
		 * Truncate query at the position of terminating semicolon to be able
		 * to append extra order by entries. If query is submitted from client
		 * other than psql the terminator may not present.
		 */
		char   *end = step->sql_statement + strlen(step->sql_statement) - 1;
		while(isspace((unsigned char) *end) && end > step->sql_statement)
			end--;
		if (*end == ';')
			*end = '\0';

		appendStringInfoString(buf, step->sql_statement + offset);
	}

	if (extra_sort)
	{
		foreach(l, extra_sort)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(l);
			char *exprstr = deparse_expression((Node *) tle->expr, context,
											   useprefix, false);

			if (has_order_by)
				appendStringInfo(buf, ", ");
			else
			{
				appendStringInfo(buf, " ORDER BY ");
				has_order_by = true;
			}

			appendStringInfoString(buf, exprstr);
		}
	}

	/* do not need the copy */
	pfree(sql);

	/* free previous query */
	pfree(step->sql_statement);
	/* get a copy of new query */
	step->sql_statement = pstrdup(buf->data);
	/* free the query buffer */
	pfree(buf->data);
	pfree(buf);
}


/*
 * Plan to sort step tuples
 * PGXC: copied and adopted from optimizer/plan/planner.c
 */
static void
make_simple_sort_from_sortclauses(Query *query, RemoteQuery *step)
{
	List 	   *sortcls = query->sortClause;
	List	   *distinctcls = query->distinctClause;
	List	   *sub_tlist = step->scan.plan.targetlist;
	SimpleSort *sort;
	SimpleDistinct *distinct;
	ListCell   *l;
	int			numsortkeys;
	int			numdistkeys;
	AttrNumber *sortColIdx;
	AttrNumber *distColIdx;
	Oid		   *sortOperators;
	Oid		   *eqOperators;
	bool	   *nullsFirst;
	bool		need_reconstruct = false;
	/*
	 * List of target list entries from DISTINCT which are not in the ORDER BY.
	 * The exressions should be appended to the ORDER BY clause of remote query
	 */
	List	   *extra_distincts = NIL;

	Assert(step->sort == NULL);
	Assert(step->distinct == NULL);

	/*
	 * We will need at most list_length(sortcls) sort columns; possibly less
	 * Also need room for extra distinct expressions if we need to append them
	 */
	numsortkeys = list_length(sortcls) + list_length(distinctcls);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;
	sort  = makeNode(SimpleSort);

	if (sortcls)
	{
		foreach(l, sortcls)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl, sub_tlist);

			if (tle->resjunk)
				need_reconstruct = true;

			/*
			 * Check for the possibility of duplicate order-by clauses --- the
			 * parser should have removed 'em, but no point in sorting
			 * redundantly.
			 */
			numsortkeys = add_sort_column(tle->resno, sortcl->sortop,
										  sortcl->nulls_first,
										  numsortkeys,
										  sortColIdx, sortOperators, nullsFirst);
		}
	}

	if (distinctcls)
	{
		/*
		 * Validate distinct clause
		 * We have to sort tuples to filter duplicates, and if ORDER BY clause
		 * is already present the sort order specified here may be incompatible
		 * with order needed for distinct.
		 *
		 * To be compatible, all expressions from DISTINCT must appear at the
		 * beginning of ORDER BY list. If list of DISTINCT expressions is longer
		 * then ORDER BY we can make ORDER BY compatible we can append remaining
		 * expressions from DISTINCT to ORDER BY. Obviously ORDER BY must not
		 * contain expressions not from the DISTINCT list in this case.
		 *
		 * For validation purposes we use column indexes (AttrNumber) to
		 * identify expressions. May be this is not enough and we should revisit
		 * the algorithm.
		 *
		 * We validate compatibility as follow:
		 * 1. Make working copy of DISTINCT
		 * 1a. Remove possible duplicates when copying: do not add expression
		 * 2. If order by is empty they are already compatible, skip 3
		 * 3. Iterate over ORDER BY items
		 * 3a. If the item is in the working copy delete it from the working
		 * 	   list. If working list is empty after deletion DISTINCT and
		 * 	   ORDER BY are compatible, so break the loop. If working list is
		 * 	   not empty continue iterating
		 * 3b. ORDER BY clause may contain duplicates. So if we can not found
		 * 	   expression in the remainder of DISTINCT, probably it has already
		 * 	   been removed because of duplicate ORDER BY entry. Check original
		 * 	   DISTINCT clause, if expression is there continue iterating.
		 * 3c. DISTINCT and ORDER BY are not compatible, emit error
		 * 4. DISTINCT and ORDER BY are compatible, if we have remaining items
		 *    in the working copy we should append it to the order by list
		 */
		/*
		 * Create the list of unique DISTINCT clause expressions
		 */
		foreach(l, distinctcls)
		{
			SortGroupClause *distinctcl = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(distinctcl, sub_tlist);
			bool found = false;

			if (extra_distincts)
			{
				ListCell   *xl;

				foreach(xl, extra_distincts)
				{
					TargetEntry *xtle = (TargetEntry *) lfirst(xl);
					if (xtle->resno == tle->resno)
					{
						found = true;
						break;
					}
				}
			}

			if (!found)
				extra_distincts = lappend(extra_distincts, tle);
		}

		if (sortcls)
		{
			foreach(l, sortcls)
			{
				SortGroupClause *sortcl = (SortGroupClause *) lfirst(l);
				TargetEntry *tle = get_sortgroupclause_tle(sortcl, sub_tlist);
				bool found = false;
				ListCell   *xl;
				ListCell   *prev = NULL;

				/* Search for the expression in the DISTINCT clause */
				foreach(xl, extra_distincts)
				{
					TargetEntry *xtle = (TargetEntry *) lfirst(xl);
					if (xtle->resno == tle->resno)
					{
						extra_distincts = list_delete_cell(extra_distincts, xl,
														   prev);
						found = true;
						break;
					}
					prev = xl;
				}

				/* Probably we've done */
				if (found && list_length(extra_distincts) == 0)
					break;

				/* Ensure sort expression is not a duplicate */
				if (!found)
				{
					foreach(xl, distinctcls)
					{
						SortGroupClause *xcl = (SortGroupClause *) lfirst(xl);
						TargetEntry *xtle = get_sortgroupclause_tle(xcl, sub_tlist);
						if (xtle->resno == tle->resno)
						{
							/* it is a duplicate then */
							found = true;
							break;
						}
					}
				}

				/* Give up, we do not support it */
				if (!found)
				{
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("Such combination of ORDER BY and DISTINCT is not yet supported"))));
				}
			}
		}
		/* need to append to the ORDER BY */
		if (list_length(extra_distincts) > 0)
			need_reconstruct = true;

		/*
		 * End of validation, expression to append to ORDER BY are in the
		 * extra_distincts list
		 */

		distinct = makeNode(SimpleDistinct);

		/*
		 * We will need at most list_length(distinctcls) sort columns
		 */
		numdistkeys = list_length(distinctcls);
		distColIdx = (AttrNumber *) palloc(numdistkeys * sizeof(AttrNumber));
		eqOperators = (Oid *) palloc(numdistkeys * sizeof(Oid));

		numdistkeys = 0;

		foreach(l, distinctcls)
		{
			SortGroupClause *distinctcl = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(distinctcl, sub_tlist);

			/*
			 * Check for the possibility of duplicate order-by clauses --- the
			 * parser should have removed 'em, but no point in sorting
			 * redundantly.
			 */
			numdistkeys = add_distinct_column(tle->resno,
											  distinctcl->eqop,
											  numdistkeys,
											  distColIdx,
											  eqOperators);
			/* append also extra sort operator, if not already there */
			numsortkeys = add_sort_column(tle->resno,
										  distinctcl->sortop,
										  distinctcl->nulls_first,
										  numsortkeys,
										  sortColIdx,
										  sortOperators,
										  nullsFirst);
		}

		Assert(numdistkeys > 0);

		distinct->numCols = numdistkeys;
		distinct->uniqColIdx = distColIdx;
		distinct->eqOperators = eqOperators;

		step->distinct = distinct;
	}


	Assert(numsortkeys > 0);

	sort->numCols = numsortkeys;
	sort->sortColIdx = sortColIdx;
	sort->sortOperators = sortOperators;
	sort->nullsFirst = nullsFirst;

	step->sort = sort;

	if (need_reconstruct)
		reconstruct_step_query(query->rtable, sortcls != NULL, extra_distincts,
							   step);
}

/*
 * Special case optimization.
 * Handle LIMIT and OFFSET for single-step queries on multiple nodes.
 *
 * Return non-zero if we need to fall back to the standard plan.
 */
static int
handle_limit_offset(RemoteQuery *query_step, Query *query, PlannedStmt *plan_stmt)
{

	/* check if no special handling needed */
	if (!query->limitCount && !query->limitOffset)
		return 0;

	if (query_step && query_step->exec_nodes &&
				list_length(query_step->exec_nodes->nodelist) <= 1)
		return 0;

	/* if order by and limit are present, do not optimize yet */
	if ((query->limitCount || query->limitOffset) && query->sortClause)
		return 1;

	/*
	 * Note that query_step->is_single_step is set to true, but
	 * it is ok even if we add limit here.
	 * If OFFSET is set, we strip the final offset value and add
	 * it to the LIMIT passed down. If there is an OFFSET and no
	 * LIMIT, we just strip off OFFSET.
	 */
	if (query->limitOffset)
	{
		int64 newLimit = 0;
		char *newpos;
		char *pos;
		char *limitpos;
		char *newQuery;
		char *newchar;
		char *c;

		pos = NULL;
		newpos = NULL;

		if (query->limitCount)
		{
			for (pos = query_step->sql_statement, newpos = pos; newpos != NULL; )
			{
				pos = newpos;
				newpos = strcasestr(pos+1, "LIMIT");
			}
			limitpos = pos;

			if (IsA(query->limitCount, Const))
				newLimit = DatumGetInt64(((Const *) query->limitCount)->constvalue);
			else
				return 1;
		}

		for (pos = query_step->sql_statement, newpos = pos; newpos != NULL; )
		{
			pos = newpos;
			newpos = strcasestr(pos+1, "OFFSET");
		}

		if (limitpos && limitpos < pos)
			pos = limitpos;

		if (IsA(query->limitOffset, Const))
			newLimit += DatumGetInt64(((Const *) query->limitOffset)->constvalue);
		else
			return 1;

		if (!pos || pos == query_step->sql_statement)
			elog(ERROR, "Could not handle LIMIT/OFFSET");

		newQuery = (char *) palloc(strlen(query_step->sql_statement)+1);
		newchar = newQuery;

		/* copy up until position where we found clause */
		for (c = &query_step->sql_statement[0]; c != pos && *c != '\0'; *newchar++ = *c++);

		if (query->limitCount)
			sprintf(newchar, "LIMIT %I64d", newLimit);
		else
			*newchar = '\0';

		pfree(query_step->sql_statement);
		query_step->sql_statement = newQuery;
	}

	/* Now add a limit execution node at the top of the plan */
	plan_stmt->planTree = (Plan *) make_limit(plan_stmt->planTree,
						query->limitOffset, query->limitCount, 0, 0);

	return 0;
}


/*
 * Build up a QueryPlan to execute on.
 *
 * For the prototype, there will only be one step,
 * and the nodelist will be NULL if it is not a PGXC-safe statement.
 */
PlannedStmt *
pgxc_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;
	Plan *standardPlan;
	RemoteQuery *query_step;


	/* build the PlannedStmt result */
	result = makeNode(PlannedStmt);

	/* Try and set what we can */
	result->commandType = query->commandType;
	result->canSetTag = query->canSetTag;
	result->utilityStmt = query->utilityStmt;
	result->intoClause = query->intoClause;

	result->rtable = query->rtable;

	query_step = makeNode(RemoteQuery);

	query_step->is_single_step = false;
	/*
	 * Declare Cursor case:
	 * We should leave as a step query only SELECT statement
	 * Further if we need refer source statement for planning we should take
	 * the truncated string
	 */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, DeclareCursorStmt))
	{

		char	   *src = query->sql_statement;
		char 		str[strlen(src) + 1]; /* mutable copy */
		char	   *dst = str;

		cursorOptions |= ((DeclareCursorStmt *) query->utilityStmt)->options;

		/*
		 * Initialize mutable copy, converting letters to uppercase and
		 * various witespace characters to spaces
		 */
		while (*src)
		{
			if (isspace(*src))
			{
				src++;
				*dst++ = ' ';
			}
			else
				*dst++ = toupper(*src++);
		}
		*dst = '\0';
		/* search for SELECT keyword in the normalized string */
		dst = strstr(str, " SELECT ");
		/* Take substring of the original string using found offset */
		query_step->sql_statement = pstrdup(query->sql_statement + (dst - str + 1));
	}
	else
		query_step->sql_statement = pstrdup(query->sql_statement);

	query_step->exec_nodes = NULL;
	query_step->combine_type = COMBINE_TYPE_NONE;
	query_step->simple_aggregates = NULL;
	/* Optimize multi-node handling */
	query_step->read_only = query->commandType == CMD_SELECT;
	query_step->force_autocommit = false;

	result->planTree = (Plan *) query_step;

	/*
	 * Determine where to execute the command, either at the Coordinator
	 * level, Data Nodes, or both. By default we choose both. We should be
	 * able to quickly expand this for more commands.
	 */
	switch (query->commandType)
	{
		case CMD_SELECT:
			/* Perform some checks to make sure we can support the statement */
			if (query->intoClause)
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("INTO clause not yet supported"))));
			/* fallthru */
		case CMD_INSERT:
		case CMD_UPDATE:
		case CMD_DELETE:
			/* Set result relations */
			if (query->commandType != CMD_SELECT)
				result->resultRelations = list_make1_int(query->resultRelation);

			query_step->exec_nodes = get_plan_nodes_command(query);

			if (query_step->exec_nodes == NULL)
			{
				/* Do not yet allow multi-node correlated UPDATE or DELETE */
				if (query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE)
				{
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("Complex and correlated UPDATE and DELETE not yet supported"))));
				}

				/*
				 * Processing guery against catalog tables, or multi-step command.
				 * Run through standard planner
				 */
				result = standard_planner(query, cursorOptions, boundParams);
				return result;
			}

			/* Do not yet allow multi-node correlated UPDATE or DELETE */
			if ((query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE)
							&& !query_step->exec_nodes
							&& list_length(query->rtable) > 1)
			{
				result = standard_planner(query, cursorOptions, boundParams);
				return result;
			}

			/*
			 * Use standard plan if we have more than one data node with either
			 * group by, hasWindowFuncs, or hasRecursive
			 */
			/*
			 * PGXCTODO - this could be improved to check if the first
			 * group by expression is the partitioning column, in which
			 * case it is ok to treat as a single step.
			 */
			if (query->commandType == CMD_SELECT
							&& query_step->exec_nodes
							&& list_length(query_step->exec_nodes->nodelist) > 1
							&& (query->groupClause || query->hasWindowFuncs || query->hasRecursive))
			{
				result = standard_planner(query, cursorOptions, boundParams);
				return result;
			}

			query_step->is_single_step = true;
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
			query_step->scan.plan.targetlist = query->targetList;

			if (query_step->exec_nodes)
				query_step->combine_type = get_plan_combine_type(
						query, query_step->exec_nodes->baselocatortype);

			/* Set up simple aggregates */
			/* PGXCTODO - we should detect what types of aggregates are used.
			 * in some cases we can avoid the final step and merely proxy results
			 * (when there is only one data node involved) instead of using
			 * coordinator consolidation. At the moment this is needed for AVG()
			 */
			query_step->simple_aggregates = get_simple_aggregates(query);

			/*
			 * Add sorting to the step
			 */
			if (list_length(query_step->exec_nodes->nodelist) > 1 &&
					(query->sortClause || query->distinctClause))
				make_simple_sort_from_sortclauses(query, query_step);

			/* Handle LIMIT and OFFSET for single-step queries on multiple nodes */
			if (handle_limit_offset(query_step, query, result))
			{
				/* complicated expressions, just fallback to standard plan */
				result = standard_planner(query, cursorOptions, boundParams);
				return result;
			}
			break;

		default:
			/* Allow for override */
			if (StrictStatementChecking)
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("This command is not yet supported."))));
			else
				result->planTree = standardPlan;
	}

	/*
	 * If creating a plan for a scrollable cursor, make sure it can run
	 * backwards on demand.  Add a Material node at the top at need.
	 */
	if (cursorOptions & CURSOR_OPT_SCROLL)
	{
		if (!ExecSupportsBackwardScan(result->planTree))
			result->planTree = materialize_finished_plan(result->planTree);
	}

	return result;
}


/*
 * Free Query_Step struct
 */
static void
free_query_step(RemoteQuery *query_step)
{
	if (query_step == NULL)
		return;

	pfree(query_step->sql_statement);
	if (query_step->exec_nodes)
	{
		if (query_step->exec_nodes->nodelist)
			list_free(query_step->exec_nodes->nodelist);
		if (query_step->exec_nodes->primarynodelist)
			list_free(query_step->exec_nodes->primarynodelist);
	}
	if (query_step->simple_aggregates != NULL)
		list_free_deep(query_step->simple_aggregates);
	pfree(query_step);
}
