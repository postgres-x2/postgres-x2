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
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
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
	Query				    *query;
	bool					isRead;
	Exec_Nodes 			   *exec_nodes;	/* resulting execution nodes */
	Special_Conditions 	   *conditions;
	bool					multilevel_join;
	List 				   *rtables;	/* a pointer to a list of rtables */
	int					    varno;
	bool				    within_or;
	bool				    within_not;
} XCWalkerContext;


/* A list of List*'s, one for each relation. */
List	   *join_list = NULL;

/* Forbid unsafe SQL statements */
bool		StrictStatementChecking = true;

/* Forbid multi-node SELECT statements with an ORDER BY clause */
bool		StrictSelectChecking = false;


static Exec_Nodes *get_plan_nodes(Query *query, bool isRead);
static bool get_plan_nodes_walker(Node *query_node, XCWalkerContext *context);
static bool examine_conditions_walker(Node *expr_node, XCWalkerContext *context);


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
find_pgxc_join(int relid1, char *aliasname1, int relid2, char *aliasname2)
{
	ListCell   *lc;

	/* return if list is still empty */
	if (join_list == NULL)
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
	foreach(lc, join_list)
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
find_or_create_pgxc_join(int relid1, char *aliasname1, int relid2, char *aliasname2)
{
	PGXC_Join   *pgxcjoin;

	pgxcjoin = find_pgxc_join(relid1, aliasname1, relid2, aliasname2);

	if (pgxcjoin == NULL)
	{
		pgxcjoin = new_pgxc_join(relid1, aliasname1, relid2, aliasname2);
		join_list = lappend(join_list, pgxcjoin);
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
free_join_list(void)
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
static Exec_Nodes *
get_plan_nodes_insert(Query *query)
{
	RangeTblEntry *rte;
	RelationLocInfo *rel_loc_info;
	Const	   *constant;
	Exec_Nodes *exec_nodes;
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


	Assert(!context);

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
										 column_base2->relid, column_base2->relalias);

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
		Exec_Nodes *save_exec_nodes = context->exec_nodes; /* Save old exec_nodes */

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
	Exec_Nodes *test_exec_nodes = NULL;
	Exec_Nodes *current_nodes = NULL;
	Exec_Nodes *from_query_nodes = NULL;
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
					context->exec_nodes = (Exec_Nodes *) palloc0(sizeof(Exec_Nodes));
					context->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_PGCATALOG;
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
				Exec_Nodes *save_exec_nodes = context->exec_nodes;
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
		context->exec_nodes = (Exec_Nodes *) palloc0(sizeof(Exec_Nodes));
		context->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_PGCATALOG;
		return false;
	} 

	/* Examine the WHERE clause, too */
	if (examine_conditions_walker(query->jointree->quals, context))
		return true;

	/* Examine join conditions, see if each join is single-node safe */
	if (join_list != NULL)
	{
		foreach(lc, join_list)
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
static Exec_Nodes *
get_plan_nodes(Query *query, bool isRead)
{
	Exec_Nodes *result_nodes;
	XCWalkerContext *context = palloc0(sizeof(XCWalkerContext));

	context->query = query;
	context->isRead = isRead;

	context->conditions = (Special_Conditions *) palloc0(sizeof(Special_Conditions));
	context->rtables = lappend(context->rtables, query->rtable);

	join_list = NULL;

	if (get_plan_nodes_walker((Node *) query, context))
		result_nodes = NULL;
	else
		result_nodes = context->exec_nodes;

	free_special_relations(context->conditions);
	return result_nodes;
}


/*
 * get_plan_nodes_command - determine the nodes to execute the plan on
 *
 * return NULL if it is not safe to be done in a single step.
 */
static Exec_Nodes *
get_plan_nodes_command(Query *query)
{
	Exec_Nodes *exec_nodes = NULL;

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

	free_join_list();
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

				simple_agg = (SimpleAgg *) palloc0(sizeof(SimpleAgg));
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
			else
  			{
				/*
				 * PGXCTODO relax this limit after adding GROUP BY support
				 * then support expressions of aggregates
				 */
  				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("Query is not yet supported"))));
  			}
			column_pos++;
  		}
  	}
	return simple_agg_list;
}


/*
 * Build up a QueryPlan to execute on.
 *
 * For the prototype, there will only be one step,
 * and the nodelist will be NULL if it is not a PGXC-safe statement.
 */
Query_Plan *
GetQueryPlan(Node *parsetree, const char *sql_statement, List *querytree_list)
{
	Query_Plan *query_plan = palloc(sizeof(Query_Plan));
	Query_Step *query_step = palloc(sizeof(Query_Step));
	Query	   *query;


	query_plan->force_autocommit = false;

	query_step->sql_statement = (char *) palloc(strlen(sql_statement) + 1);
	strcpy(query_step->sql_statement, sql_statement);
	query_step->exec_nodes = NULL;
	query_step->combine_type = COMBINE_TYPE_NONE;
	query_step->simple_aggregates = NULL;

	query_plan->query_step_list = lappend(NULL, query_step);

	/*
	 * Determine where to execute the command, either at the Coordinator
	 * level, Data Nodes, or both. By default we choose both. We should be
	 * able to quickly expand this for more commands.
	 */
	switch (nodeTag(parsetree))
	{
		case T_SelectStmt:
		case T_InsertStmt:
		case T_UpdateStmt:
		case T_DeleteStmt:
			/* just use first one in querytree_list */
			query = (Query *) linitial(querytree_list);

			/* Perform some checks to make sure we can support the statement */
			if (nodeTag(parsetree) == T_SelectStmt)
			{
				if (query->intoClause)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("INTO clause not yet supported"))));

				if (query->setOperations)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("UNION, INTERSECT and EXCEPT are not yet supported"))));

				if (query->hasRecursive)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("WITH RECURSIVE not yet supported"))));

				if (query->hasWindowFuncs)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("Window functions not yet supported"))));
			}

			query_step->exec_nodes =
				get_plan_nodes_command(query);
			if (query_step->exec_nodes)
				query_step->combine_type = get_plan_combine_type(
						query, query_step->exec_nodes->baselocatortype);
			/* Only set up if running on more than one node */
			if (query_step->exec_nodes && query_step->exec_nodes->nodelist &&
							list_length(query_step->exec_nodes->nodelist) > 1)
				query_step->simple_aggregates = get_simple_aggregates(query);

			/*
			 * See if it is a SELECT with no relations, like SELECT 1+1 or
			 * SELECT nextval('fred'), and just use coord.
			 */
			if (query_step->exec_nodes == NULL
						&& (query->jointree->fromlist == NULL
						|| query->jointree->fromlist->length == 0))
				/* Just execute it on Coordinator */
				query_plan->exec_loc_type = EXEC_ON_COORD;
			else
			{
				if (query_step->exec_nodes != NULL
						&& query_step->exec_nodes->tableusagetype == TABLE_USAGE_TYPE_PGCATALOG)
				{
					/* pg_catalog query, run on coordinator */
					query_plan->exec_loc_type = EXEC_ON_COORD;
				}
				else
				{
					query_plan->exec_loc_type = EXEC_ON_DATA_NODES;

					/* If node list is NULL, execute on coordinator */
					if (!query_step->exec_nodes)
						query_plan->exec_loc_type = EXEC_ON_COORD;
				}
			}

			/*
			 * PG-XC cannot yet support some variations of SQL statements.
			 * We perform some checks to at least catch common cases
			 */

			/*
			 * Check if we have multiple nodes and an unsupported clause. This
			 * is temporary until we expand supported SQL
			 */
			if (nodeTag(parsetree) == T_SelectStmt)
			{
				if (StrictStatementChecking && query_step->exec_nodes
						&& list_length(query_step->exec_nodes->nodelist) > 1)
				{
					/*
					 * PGXCTODO - this could be improved to check if the first
					 * group by expression is the partitioning column
					 */
					if (query->groupClause)
						ereport(ERROR,
								(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("Multi-node GROUP BY not yet supported"))));
					if (query->limitCount && StrictSelectChecking)
						ereport(ERROR,
								(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							(errmsg("Multi-node LIMIT not yet supported"))));
					if (query->sortClause && StrictSelectChecking)
						ereport(ERROR,
								(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("Multi-node ORDER BY not yet supported"))));
					/* PGXCTODO - check if first column partitioning column */
					if (query->distinctClause)
						ereport(ERROR,
								(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("Multi-node DISTINCT`not yet supported"))));
				}
			}
			break;

			/* Statements that we only want to execute on the Coordinator */
		case T_VariableShowStmt:
			query_plan->exec_loc_type = EXEC_ON_COORD;
			break;

			/*
			 * Statements that need to run in autocommit mode, on Coordinator
			 * and Data Nodes with suppressed implicit two phase commit.
			 */
		case T_CheckPointStmt:
		case T_ClusterStmt:
		case T_CreatedbStmt:
		case T_DropdbStmt:
		case T_VacuumStmt:
			query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			query_plan->force_autocommit = true;
			break;

		case T_DropPropertyStmt:
			/*
			 * Triggers are not yet supported by PGXC
			 * all other queries are executed on both Coordinator and Datanode
			 * On the same point, assert also is not supported
			 */
			if (((DropPropertyStmt *)parsetree)->removeType == OBJECT_TRIGGER)
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						(errmsg("This command is not yet supported."))));
			else
				query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;

		case T_CreateStmt:
			if (((CreateStmt *)parsetree)->relation->istemp)
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						(errmsg("Temp tables are not yet supported."))));

			query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;

			/*
			 * Statements that we execute on both the Coordinator and Data Nodes
			 */
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterDomainStmt:
		case T_AlterFdwStmt:
		case T_AlterForeignServerStmt:
		case T_AlterFunctionStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOpFamilyStmt:
		case T_AlterSeqStmt:
		case T_AlterTableStmt: /* Can also be used to rename a sequence */
		case T_AlterTSConfigurationStmt:
		case T_AlterTSDictionaryStmt:
		case T_ClosePortalStmt:   /* In case CLOSE ALL is issued */
		case T_CommentStmt:
		case T_CompositeTypeStmt:
		case T_ConstraintsSetStmt:
		case T_CreateCastStmt:
		case T_CreateConversionStmt:
		case T_CreateDomainStmt:
		case T_CreateEnumStmt:
		case T_CreateFdwStmt:
		case T_CreateForeignServerStmt:
		case T_CreateFunctionStmt: /* Only global functions are supported */
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_CreatePLangStmt:
		case T_CreateSeqStmt:
		case T_CreateSchemaStmt:
		case T_DeallocateStmt: /* Allow for DEALLOCATE ALL */
		case T_DiscardStmt:
		case T_DropCastStmt:
		case T_DropFdwStmt:
		case T_DropForeignServerStmt:
		case T_DropPLangStmt:
		case T_DropStmt:
		case T_IndexStmt:
		case T_LockStmt:
		case T_ReindexStmt:
		case T_RemoveFuncStmt:
		case T_RemoveOpClassStmt:
		case T_RemoveOpFamilyStmt:
		case T_RenameStmt:
		case T_RuleStmt:
		case T_TruncateStmt:
		case T_VariableSetStmt:
		case T_ViewStmt:

			/*
			 * Also support these, should help later with pg_restore, although
			 * not very useful because of the pooler using the same user
			 */
		case T_GrantStmt:
		case T_GrantRoleStmt:
		case T_CreateRoleStmt:
		case T_AlterRoleStmt:
		case T_AlterRoleSetStmt:
		case T_AlterUserMappingStmt:
		case T_CreateUserMappingStmt:
		case T_DropRoleStmt:
		case T_AlterOwnerStmt:
		case T_DropOwnedStmt:
		case T_DropUserMappingStmt:
		case T_ReassignOwnedStmt:
		case T_DefineStmt:		/* used for aggregates, some types */
			query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;

		case T_TransactionStmt:
			switch (((TransactionStmt *) parsetree)->kind)
			{
				case TRANS_STMT_SAVEPOINT:
				case TRANS_STMT_RELEASE:
				case TRANS_STMT_ROLLBACK_TO:
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("This type of transaction statement not yet supported"))));
					break;

				default:
					break; /* keep compiler quiet */
			}
			query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;

			/*
			 * For now, pick one of the data nodes until we modify real
			 * planner It will give an approximate idea of what an isolated
			 * data node will do
			 */
		case T_ExplainStmt:
			if (((ExplainStmt *) parsetree)->analyze)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("ANALYZE with EXPLAIN is currently not supported."))));

			query_step->exec_nodes = palloc0(sizeof(Exec_Nodes));
			query_step->exec_nodes->nodelist = GetAnyDataNode();
			query_step->exec_nodes->baselocatortype = LOCATOR_TYPE_RROBIN;
			query_plan->exec_loc_type = EXEC_ON_DATA_NODES;
			break;

			/*
			 * Trigger queries are not yet supported by PGXC.
			 * Tablespace queries are also not yet supported.
			 * Two nodes on the same servers cannot use the same tablespace.
			 */
		case T_CreateTableSpaceStmt:
		case T_CreateTrigStmt:
		case T_DropTableSpaceStmt:
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					(errmsg("This command is not yet supported."))));
			break;

			/*
			 * Other statements we do not yet want to handle.
			 * By default they would  be fobidden, but we list these for reference.
			 * Note that there is not a 1-1 correspndence between
			 * SQL command and the T_*Stmt structures.
			 */
		case T_DeclareCursorStmt:
		case T_ExecuteStmt:
		case T_FetchStmt:
		case T_ListenStmt:
		case T_LoadStmt:
		case T_NotifyStmt:
		case T_PrepareStmt:
		case T_UnlistenStmt:
			/* fall through */
		default:
			/* Allow for override */
			if (StrictStatementChecking)
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("This command is not yet supported."))));
			else
				query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;
	}


	return query_plan;
}


/*
 * Free Query_Step struct
 */
static void
free_query_step(Query_Step *query_step)
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

/*
 * Free Query_Plan struct
 */
void
FreeQueryPlan(Query_Plan *query_plan)
{
	ListCell   *item;

	if (query_plan == NULL)
		return;

	foreach(item, query_plan->query_step_list)
		free_query_step((Query_Step *) lfirst(item));

	pfree(query_plan->query_step_list);
	pfree(query_plan);
}
