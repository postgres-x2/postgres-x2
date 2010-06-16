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

/*
 * This struct helps us detect special conditions to determine what nodes
 * to execute on.
 */
typedef struct
{
	List	   *partitioned_literal_comps;		/* List of Literal_Comparison */
	List	   *partitioned_parent_child;
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

/* A list of List*'s, one for each relation. */
List	   *join_list = NULL;

/* Forbid unsafe SQL statements */
bool		StrictStatementChecking = true;

/* Forbid multi-node SELECT statements with an ORDER BY clause */
bool		StrictSelectChecking = false;

/*
 * True if both lists contain only one node and are the same
 */
static bool
same_single_node (List *nodelist1, List *nodelist2)
{
	return nodelist1 && list_length(nodelist1) == 1
			&& nodelist2 && list_length(nodelist2) == 1
			&& linitial_int(nodelist1) != linitial_int(nodelist2);
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
 */
static ColumnBase*
get_base_var(Var *var, List *rtables)
{
	RangeTblEntry *rte;

	/* Skip system attributes */
	if (!AttrNumberIsForUserDefinedAttr(var->varattno))
		return NULL;

	/* get the RangeTableEntry */
	rte = list_nth(rtables, var->varno - 1);

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
		return get_base_var(colvar, rtables);
		//may need to set this, toocolumn_base->relalias = rte->eref->aliasname;
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
			Var *colvar = (Var *) tle->expr;

			/* continue resolving recursively */
			return get_base_var(colvar, rte->subquery->rtable);
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
					eval_expr = eval_const_expressions(NULL, tle->expr);
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
 * examine_conditions
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
 * PGXCTODO: Recognize subqueries, and give up (long term allow safe ones).
 *
 */
static bool
examine_conditions(Special_Conditions *conditions, List *rtables, Node *expr_node)
{
	RelationLocInfo *rel_loc_info1,
			   *rel_loc_info2;
	Const	   *constant;
	Expr	   *checkexpr;


	if (expr_node == NULL)
		return true;

	if (rtables == NULL)
		return true;

	if (conditions == NULL)
		conditions = new_special_conditions();

	if (IsA(expr_node, BoolExpr))
	{
		BoolExpr   *boolexpr = (BoolExpr *) expr_node;

		/* Recursively handle ANDed expressions, but don't handle others */
		if (boolexpr->boolop == AND_EXPR)
		{
			if (!examine_conditions(conditions, rtables,
									linitial(boolexpr->args)))
				return false;

			return examine_conditions(
							   conditions, rtables, lsecond(boolexpr->args));
		}
		else if (boolexpr->boolop == OR_EXPR)
		{
			/*
			 * look at OR's as work-around for reported issue.
			 * NOTE: THIS IS NOT CORRECT, BUT JUST DONE FOR THE PROTOTYPE.
			 * More rigorous
			 * checking needs to be done. PGXCTODO: Add careful checking for
			 * OR'ed conditions...
			 */
			if (!examine_conditions(conditions, rtables,
									linitial(boolexpr->args)))
				return false;

			return examine_conditions(
							   conditions, rtables, lsecond(boolexpr->args));
		}
		else
			/* looks complicated, give up */
			return false;

		return true;
	}


	if (IsA(expr_node, OpExpr))
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

				ColumnBase *column_base = get_base_var(colvar, rtables);

				if (!column_base)
					return false;

				/* Look at other argument */
				checkexpr = arg2;

				/* We may have a cast or expression, try and handle it */
				if (!IsA(arg2, Const))
				{
					/* this gets freed when the memory context gets freed */
					Expr *eval_expr = eval_const_expressions(NULL, arg2);
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
						return false;

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

						conditions->partitioned_literal_comps = lappend(
									   conditions->partitioned_literal_comps,
																   lit_comp);

						return true;
					}
					else
					{
						/* unimportant comparison, just return */
						if (rel_loc_info1)
							FreeRelationLocInfo(rel_loc_info1);
						return true;
					}

				}
				else if (IsA(arg2, Var))
				{
					PGXC_Join   *pgxc_join;
					ColumnBase *column_base2;
					Var		   *colvar2 = (Var *) arg2;

					rel_loc_info1 = GetRelationLocInfo(column_base->relid);

					if (!rel_loc_info1)
						return false;

					column_base2 = get_base_var(colvar2, rtables);
					if (!column_base2)
						return false;
					rel_loc_info2 = GetRelationLocInfo(column_base2->relid);

					/* get data struct about these two relations joining */
					pgxc_join = find_or_create_pgxc_join(column_base->relid, column_base->relalias,
										 column_base2->relid, column_base2->relalias);

					/*
					 * pgxc_join->condition_list =
					 * lappend(pgxc_join->condition_list, opexpr);
					 */

					if (rel_loc_info1->locatorType == LOCATOR_TYPE_REPLICATED)
					{
						/* add to replicated join conditions */
						conditions->replicated_joins =
							lappend(conditions->replicated_joins, opexpr);

						if (rel_loc_info2->locatorType != LOCATOR_TYPE_REPLICATED)
						{
							/* Note other relation, saves us work later. */
							conditions->base_rel_name = column_base2->relname;
							conditions->base_rel_loc_info = rel_loc_info2;
							if (rel_loc_info1)
								FreeRelationLocInfo(rel_loc_info1);
						}

						if (conditions->base_rel_name == NULL)
						{
							conditions->base_rel_name = column_base->relname;
							conditions->base_rel_loc_info = rel_loc_info1;
							if (rel_loc_info2)
								FreeRelationLocInfo(rel_loc_info2);
						}

						/* note nature of join between the two relations */
						pgxc_join->join_type = JOIN_REPLICATED;
						return true;
					}

					if (rel_loc_info2->locatorType == LOCATOR_TYPE_REPLICATED)
					{
						/* add to replicated join conditions */
						conditions->replicated_joins =
							lappend(conditions->replicated_joins, opexpr);

						/* other relation not replicated, note it for later */
						conditions->base_rel_name = column_base->relname;
						conditions->base_rel_loc_info = rel_loc_info1;

						/* note nature of join between the two relations */
						pgxc_join->join_type = JOIN_REPLICATED;

						if (rel_loc_info2)
							FreeRelationLocInfo(rel_loc_info2);

						return true;
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
						conditions->partitioned_parent_child =
							lappend(conditions->partitioned_parent_child,
									opexpr);
						pgxc_join->join_type = JOIN_COLOCATED_PARTITIONED;
						return true;
					}

					/*
					 * At this point, there is some other type of join that
					 * can probably not be executed on only a single node.
					 * Just return. Important: We preserve previous
					 * pgxc_join->join_type value, there may be multiple
					 * columns joining two tables, and we want to make sure at
					 * least one of them make it colocated partitioned, in
					 * which case it will update it when examining another
					 * condition.
					 */
					return true;
				}
				else
					return true;

			}
		}
		/* PGXCTODO - need to more finely examine other operators */
	}

	return true;
}

/*
 * examine_conditions_fromlist - Examine FROM clause for joins
 *
 * Examine FROM clause join conditions to determine special conditions
 * to help us decide which nodes to execute on.
 */
static bool
examine_conditions_fromlist(Special_Conditions *conditions, List *rtables,
							Node *treenode)
{

	if (treenode == NULL)
		return true;

	if (rtables == NULL)
		return true;

	if (conditions == NULL)
		conditions = new_special_conditions();

	if (IsA(treenode, JoinExpr))
	{
		JoinExpr   *joinexpr = (JoinExpr *) treenode;

		/* recursively examine FROM join tree */
		if (!examine_conditions_fromlist(conditions, rtables, joinexpr->larg))
			return false;

		if (!examine_conditions_fromlist(conditions, rtables, joinexpr->rarg))
			return false;

		/* Now look at join condition */
		if (!examine_conditions(conditions, rtables, joinexpr->quals))
			return false;
		return true;
	}
	else if (IsA(treenode, RangeTblRef))
		return true;
	else if (IsA(treenode, BoolExpr) ||IsA(treenode, OpExpr))
	{
		/* check base condition, if possible */
		if (!examine_conditions(conditions, rtables, treenode))
			return false;
	}

	/* Some other more complicated beast */
	return false;
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
static Exec_Nodes *
get_plan_nodes(Query_Plan *query_plan, Query *query, bool isRead)
{
	RangeTblEntry *rte;
	ListCell   *lc,
			   *item;
	Special_Conditions *special_conditions;
	OpExpr 	   *opexpr;
	Var		   *colvar;
	RelationLocInfo *rel_loc_info;
	Exec_Nodes *test_exec_nodes = NULL;
	Exec_Nodes *exec_nodes = NULL;
	Exec_Nodes *current_nodes = NULL;
	Exec_Nodes *from_query_nodes = NULL;
	TableUsageType 	table_usage_type = TABLE_USAGE_TYPE_NO_TABLE;
	TableUsageType 	current_usage_type = TABLE_USAGE_TYPE_NO_TABLE;
	int 		from_subquery_count = 0;


	exec_nodes = NULL;
	join_list = NULL;

	/* If no tables, just return */
	if (query->rtable == NULL && query->jointree == NULL)
		return NULL;

	/* Alloc and init struct */
	special_conditions = new_special_conditions();

	/* Look for special conditions */

	/* Look for JOIN syntax joins */
	foreach(item, query->jointree->fromlist)
	{
		Node	   *treenode = (Node *) lfirst(item);

		if (IsA(treenode, JoinExpr))
		{
			if (!examine_conditions_fromlist(special_conditions, query->rtable,
											 treenode))
			{
				/* May be complicated. Before giving up, just check for pg_catalog usage */
				if (contains_only_pg_catalog (query->rtable))
				{	
					/* just pg_catalog tables */
					exec_nodes = (Exec_Nodes *) palloc0(sizeof(Exec_Nodes));
					exec_nodes->tableusagetype = TABLE_USAGE_TYPE_PGCATALOG;
					free_special_relations(special_conditions);
					return exec_nodes;
				}

				/* complicated */
				free_special_relations(special_conditions);
				return NULL;
			}
		}
		else if (IsA(treenode, RangeTblRef))
		{
			RangeTblRef *rtr = (RangeTblRef *) treenode;

			/* look at the entity */
			RangeTblEntry *rte = list_nth(query->rtable, rtr->rtindex - 1);

			if (rte->rtekind == RTE_SUBQUERY)
			{
				from_subquery_count++;
				/* 
				 * Recursively call for subqueries. 
				 * Note this also works for views, which are rewritten as subqueries.
				 */
				current_nodes = get_plan_nodes(query_plan, rte->subquery, isRead);
				if (current_nodes)
					current_usage_type = current_nodes->tableusagetype;
				else 
				{
					/* could be complicated */
					free_special_relations(special_conditions);
					return NULL;
				}

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
						{
							/* Complicated */
							free_special_relations(special_conditions);
							return NULL;
						}
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
					//current_usage_type = TABLE_USAGE_TYPE_USER;
					/* Complicated */
					free_special_relations(special_conditions);
					return NULL;
				}
			}
			else
			{
				/* could be complicated */
				free_special_relations(special_conditions);
				return NULL;
			}

			/* See if we have pg_catalog mixed with other tables */
			if (table_usage_type == TABLE_USAGE_TYPE_NO_TABLE)
				table_usage_type = current_usage_type;
			else if (current_usage_type != table_usage_type)
			{
				/* mixed- too complicated for us for now */
				free_special_relations(special_conditions);
				return NULL;
			}
		}
		else
		{
			/* could be complicated */
			free_special_relations(special_conditions);
			return NULL;
		}
	}

	/* If we are just dealing with pg_catalog, just return */
	if (table_usage_type == TABLE_USAGE_TYPE_PGCATALOG)
	{
		exec_nodes = (Exec_Nodes *) palloc0(sizeof(Exec_Nodes));
		exec_nodes->tableusagetype = TABLE_USAGE_TYPE_PGCATALOG;
		return exec_nodes;
	} 

	/* Examine the WHERE clause, too */
	if (!examine_conditions(special_conditions, query->rtable,
							query->jointree->quals))
	{
		/* if cross joins may exist, just return NULL */
		free_special_relations(special_conditions);
		return NULL;
	}

	/* Examine join conditions, see if each join is single-node safe */
	if (join_list != NULL)
	{
		foreach(lc, join_list)
		{
			PGXC_Join   *pgxcjoin = (PGXC_Join *) lfirst(lc);

			/* If it is not replicated or parent-child, not single-node safe */
			if (pgxcjoin->join_type == JOIN_OTHER)
			{
				free_special_relations(special_conditions);
				return NULL;
			}
		}
	}


	/* check for non-partitioned cases */
	if (special_conditions->partitioned_parent_child == NULL &&
		special_conditions->partitioned_literal_comps == NULL)
	{
		/*
		 * We have either a single table, just replicated tables, or a
		 * table that just joins with replicated tables, or something
		 * complicated.
		 */

		/* See if we noted a table earlier to use */
		rel_loc_info = special_conditions->base_rel_loc_info;

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
				if (!rte->inFromCl || rte->rtekind != RTE_RELATION)
					continue;

				/* PGXCTODO - handle RTEs that are functions */
				if (rtesave)
					/* 
					 * Too complicated, we have multiple relations that still 
					 * cannot be joined safely 
					 */
					return NULL;

				rtesave = rte;
			}

			if (rtesave)
			{
				/* a single table, just grab it */
				rel_loc_info = GetRelationLocInfo(rtesave->relid);

				if (!rel_loc_info)
					return NULL;

				exec_nodes = GetRelationNodes(rel_loc_info, NULL, isRead);
			}
		}
		else
		{
			exec_nodes = GetRelationNodes(rel_loc_info, NULL, isRead);
		}

		/* Note replicated table usage for determining safe queries */
		if (exec_nodes)
		{
			if (table_usage_type == TABLE_USAGE_TYPE_USER && IsReplicated(rel_loc_info))
				table_usage_type = TABLE_USAGE_TYPE_USER_REPLICATED;
			else
				exec_nodes->tableusagetype = table_usage_type;
		}
	}
	/* check for partitioned col comparison against a literal */
	else if (list_length(special_conditions->partitioned_literal_comps) > 0)
	{
		exec_nodes = NULL;

		/*
		 * Make sure that if there are multiple such comparisons, that they
		 * are all on the same nodes.
		 */
		foreach(lc, special_conditions->partitioned_literal_comps)
		{
			Literal_Comparison *lit_comp = (Literal_Comparison *) lfirst(lc);

			test_exec_nodes = GetRelationNodes(
						lit_comp->rel_loc_info, &(lit_comp->constant), true);

			test_exec_nodes->tableusagetype = table_usage_type;
			if (exec_nodes == NULL)
				exec_nodes = test_exec_nodes;
			else
			{
				if (!same_single_node(exec_nodes->nodelist, test_exec_nodes->nodelist))
				{
					free_special_relations(special_conditions);
					return NULL;
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
		ColumnBase *column_base;

		opexpr = (OpExpr *) linitial(special_conditions->partitioned_parent_child);

		colvar = (Var *) linitial(opexpr->args);

		/* get the RangeTableEntry */
		column_base = get_base_var(colvar, query->rtable);
		if (!column_base)
			return false;

		rel_loc_info = GetRelationLocInfo(column_base->relid);
		if (!rel_loc_info)
			return false;

		exec_nodes = GetRelationNodes(rel_loc_info, NULL, isRead);
		exec_nodes->tableusagetype = table_usage_type;
	}
	free_special_relations(special_conditions);

	if (from_query_nodes)
	{
		if (!exec_nodes) 
			return from_query_nodes;
		/* Just use exec_nodes if the from subqueries are all replicated or using the exact
		 * same node
		 */
		else if (from_query_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED
					|| (same_single_node(from_query_nodes->nodelist, exec_nodes->nodelist)))
				return exec_nodes;
		else 
		{
			/* We allow views, where the (rewritten) subquery may be on all nodes, but the parent 
			 * query applies a condition on the from subquery.
			 */
			if (list_length(query->jointree->fromlist) == from_subquery_count
					&& list_length(exec_nodes->nodelist) == 1)
				return exec_nodes;
		}
		/* Too complicated, give up */
		return NULL;
	}

	return exec_nodes;
}


/*
 * get_plan_nodes - determine the nodes to execute the plan on
 *
 * return NULL if it is not safe to be done in a single step.
 */
static Exec_Nodes *
get_plan_nodes_command(Query_Plan *query_plan, Query *query)
{
	Exec_Nodes *exec_nodes = NULL;

	switch (query->commandType)
	{
		case CMD_SELECT:
			exec_nodes = get_plan_nodes(query_plan, query, true);
			break;

		case CMD_INSERT:
			exec_nodes = get_plan_nodes_insert(query);
			break;

		case CMD_UPDATE:
		case CMD_DELETE:
			/* treat as a select */
			exec_nodes = get_plan_nodes(query_plan, query, false);
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
 * For now we only allow MAX in the first column, and return a list of one.
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
			query_step->exec_nodes =
				get_plan_nodes_command(query_plan, query);
			if (query_step->exec_nodes)
				query_step->combine_type = get_plan_combine_type(
						query, query_step->exec_nodes->baselocatortype);
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

					/*
					 * If the nodelist is NULL, it is not safe for us to
					 * execute
					 */
					if (!query_step->exec_nodes && StrictStatementChecking)
						ereport(ERROR,
								(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
								 (errmsg("Cannot safely execute statement in a single step."))));
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
				if (query->intoClause)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("INTO clause not yet supported"))));

				if (query->setOperations)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							 (errmsg("UNION, INTERSECT and EXCEPT are not yet supported"))));

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
		case T_CreateSeqStmt:
		case T_VariableShowStmt:
			query_plan->exec_loc_type = EXEC_ON_COORD;
			break;

			/* DROP */
		case T_DropStmt:
			if (((DropStmt *) parsetree)->removeType == OBJECT_SEQUENCE)
				query_plan->exec_loc_type = EXEC_ON_COORD;
			else
				query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
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
		case T_AlterObjectSchemaStmt:
			/* Sequences are just defined on coordinator */
			if (((AlterObjectSchemaStmt *) parsetree)->objectType == OBJECT_SEQUENCE)
				query_plan->exec_loc_type = EXEC_ON_COORD;
			else
				query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;
		case T_AlterSeqStmt:
			/* Alter sequence is not supported yet, it needs complementary interactions with GTM */
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 (errmsg("This command is not yet supported"))));
			break;
		case T_AlterTableStmt:
			/*
			 * ALTER SEQUENCE needs some interactions with GTM,
			 * this query is not supported yet.
			 */
			if (((AlterTableStmt *) parsetree)->relkind == OBJECT_SEQUENCE)
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("Cannot yet alter a sequence"))));
			else
				query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;
		case T_CommentStmt:
			/* Sequences are only defined on coordinator */
			if (((CommentStmt *) parsetree)->objtype == OBJECT_SEQUENCE)
				query_plan->exec_loc_type = EXEC_ON_COORD;
			else
				query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
			break;
		case T_RenameStmt:
			/* Sequences are only defined on coordinator */
			if (((RenameStmt *) parsetree)->renameType == OBJECT_SEQUENCE)
				/*
				 * Renaming a sequence requires interactions with GTM
				 * what is not supported yet
				 */
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 (errmsg("Sequence renaming not yet supported, you should drop it and created a new one"))));
			else
				query_plan->exec_loc_type = EXEC_ON_COORD | EXEC_ON_DATA_NODES;
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

			/*
			 * Statements that we execute on both the Coordinator and Data Nodes
			 */
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterDomainStmt:
		case T_AlterFdwStmt:
		case T_AlterForeignServerStmt:
		case T_AlterFunctionStmt:
		case T_AlterOpFamilyStmt:
		case T_AlterTSConfigurationStmt:
		case T_AlterTSDictionaryStmt:
		case T_ClosePortalStmt:    /* In case CLOSE ALL is issued */
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
		case T_CreateStmt:
		case T_CreateSchemaStmt:
		case T_DeallocateStmt:	/* Allow for DEALLOCATE ALL */
		case T_DiscardStmt:
		case T_DropCastStmt:
		case T_DropFdwStmt:
		case T_DropForeignServerStmt:
		case T_DropPLangStmt:
		case T_IndexStmt:
		case T_LockStmt:
		case T_ReindexStmt:
		case T_RemoveFuncStmt:
		case T_RemoveOpClassStmt:
		case T_RemoveOpFamilyStmt:
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
			if (((ExplainStmt *)	parsetree)->analyze)
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
		case T_RuleStmt:
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
