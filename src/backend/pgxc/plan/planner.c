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

/*
 * Convenient format for literal comparisons
 */
typedef struct
{
	Oid		relid;
	RelationLocInfo	*rel_loc_info;
	Oid		attrnum;
	char		*col_name;
	Datum		constValue;
	Oid		constType;
} Literal_Comparison;

/*
 * Comparison of partitioned column and expression
 * Expression can be evaluated at execution time to determine target nodes
 */
typedef struct
{
	Oid			relid;
	RelationLocInfo *rel_loc_info;
	Oid			attrnum;
	char	   *col_name;
	Expr	   *expr;		/* assume long PGXCTODO - should be Datum */
} Expr_Comparison;

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
	List	   *partitioned_expressions;		/* List of Expr_Comparison */
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
	JOIN_REPLICATED_ONLY,
	JOIN_REPLICATED_PARTITIONED,
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
	Query			*query;
	RelationAccessType	accessType;
	RemoteQuery		*query_step;	/* remote query step being analized */
	PlannerInfo		*root;		/* planner data for the subquery */
	Special_Conditions	*conditions;
	bool			multilevel_join;
	List 			*rtables;	/* a pointer to a list of rtables */
	int			varno;
	bool			within_or;
	bool			within_not;
	bool			exec_on_coord;	/* fallback to standard planner to have plan executed on coordinator only */
	List			*join_list;	/* A list of List*'s, one for each relation. */
} XCWalkerContext;


/* Forbid unsafe SQL statements */
bool		StrictStatementChecking = true;
/* fast query shipping is enabled by default */
bool		enable_fast_query_shipping = true;

static void get_plan_nodes(PlannerInfo *root, RemoteQuery *step, RelationAccessType accessType);
static bool get_plan_nodes_walker(Node *query_node, XCWalkerContext *context);
static bool examine_conditions_walker(Node *expr_node, XCWalkerContext *context);
static int handle_limit_offset(RemoteQuery *query_step, Query *query, PlannedStmt *plan_stmt);
static void InitXCWalkerContext(XCWalkerContext *context);
static RemoteQuery *makeRemoteQuery(void);
static void validate_part_col_updatable(const Query *query);
static bool contains_temp_tables(List *rtable);
static bool contains_only_pg_catalog(List *rtable);
static bool is_subcluster_mapping(List *rtable);
static void pgxc_handle_unsupported_stmts(Query *query);
static PlannedStmt *pgxc_fqs_planner(Query *query, int cursorOptions,
										ParamListInfo boundParams);
static bool pgxc_query_needs_coord(Query *query);

/*
 * Find nth position of specified substring in the string
 * All non-printable symbols of str treated as spaces, all letters as uppercase
 * Returns pointer to the beginning of the substring or NULL
 */
static char *
strpos(char *str, char *substr, int n_pos)
{
	char		copy[strlen(str) + 1];
	char	   *src = str;
	char	   *dst = copy;

	/*
	 * Initialize mutable copy, converting letters to uppercase and
	 * various whitespace characters to spaces
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

	dst = strstr(copy, substr);

	if (n_pos != 1)
		dst = strpos(dst + strlen(substr), substr, n_pos - 1);

	return dst ? str + (dst - copy) : NULL;
}

/*
 * Convert input string characters into uppercases and
 * replace "orig" string by "replace" string in given string input
 * Result has to be freed after calling this function.
 */
static char *
strreplace(char *str, char *orig, char *replace, int *replace_cnt)
{
	char		copy[strlen(str) + 1];
	char	   *src = str;
	char	   *dst = copy;
	char	   *buffer = NULL;

	*replace_cnt = 0;

	/* Convert input into correct format with uppercases */
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

	/* We are sure there is a least 1 replacement */
	buffer = pstrdup(copy);

	/* Then replace each occurence */
	while ((dst = strstr(buffer, orig)))
	{
		int	strdiff = strlen(str) - strlen(dst);

		(*replace_cnt)++;
		buffer = (char *) repalloc(buffer, strlen(str) +
								   (*replace_cnt) * (strlen(replace) - strlen(orig) + 1));

		strncpy(buffer, buffer, strdiff);
		buffer[strdiff] = '\0';
		sprintf(buffer + strdiff, "%s%s", replace, dst + strlen(orig));
	}

	return buffer;
}

/*
 * True if both lists contain only one node and are the same
 */
static bool
same_single_node(List *nodelist1, List *nodelist2)
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
	list_free_deep(special_conditions->partitioned_expressions);

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
 *
 * We handle INSERT ... VALUES.
 * If we have INSERT SELECT, we try and see if it is patitioned-based
 * inserting into a partitioned-based.
 *
 * We set step->exec_nodes if we determine the single-step execution
 * nodes. If it is still NULL after returning from this function,
 * then the caller should use the regular PG planner
 */
static void
get_plan_nodes_insert(PlannerInfo *root, RemoteQuery *step)
{
	Query	   	*query = root->parse;
	RangeTblEntry	*rte;
	RelationLocInfo	*rel_loc_info;
	Const	   	*constExpr = NULL;
	ListCell	*lc;
	Expr		*eval_expr = NULL;

	step->exec_nodes = NULL;

	rte = (RangeTblEntry *) list_nth(query->rtable, query->resultRelation - 1);

	if (rte != NULL && rte->rtekind != RTE_RELATION)
		/* Bad relation type */
		return;

	/* Get result relation info */
	rel_loc_info = GetRelationLocInfo(rte->relid);

	if (!rel_loc_info)
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				(errmsg("Could not find relation for oid = %d", rte->relid))));

	/*
	 * Evaluate expressions in target list before trying any optimisations.
	 * The following flow is respected depending on table distribution,
	 * target column (distribution column or not) and expression shippability.
	 * For a hash-distributed table:
	 * - Non-shippable expression whatever the target column, return exec_nodes
	 *	 as NULL and go through standard planner
	 * - Shippable expression on a distribution column, go through optimization
	 *   In this case if target column is distributable, node is determined
	 *   from expression if constant can be obtained. An expression can be used
	 *   also to determine a safe node list a execution time.
	 * For replicated or round robin tables (no distribution column):
	 * - Non-shippable expression, return exec_nodes as NULL and go through
	 *   standard planner
	 * - Shippable expression, go through the optimization process
	 * PGXCTODO: for the time being query goes through standard planner if at least
	 * one non-shippable expression is found, we should be able to partially push
	 * down foreign expressions.
	 */
	foreach(lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr = tle->expr;

		/* If expression is not shippable, go through standard planner */
		if (!is_foreign_expr((Node *) expr, NULL))
		{
			step->exec_nodes = NULL;
			return;
		}
	}

	/* Optimization is only done for distributed tables */
	if (query->jointree != NULL
		&& query->jointree->fromlist != NULL
		&& (rel_loc_info->locatorType == LOCATOR_TYPE_HASH ||
		    rel_loc_info->locatorType == LOCATOR_TYPE_MODULO))
	{
		/*
		 * See if it is "single-step"
		 * Optimize for just known common case with 2 RTE entries
		 */
		if (query->resultRelation == 1 && query->rtable->length == 2)
		{
			RangeTblEntry *sub_rte = list_nth(query->rtable, 1);

			/*
			 * Get step->exec_nodes for the SELECT part of INSERT-SELECT
			 * to see if it is single-step
			 */
			if (sub_rte->rtekind == RTE_SUBQUERY &&
						!sub_rte->subquery->limitCount &&
						!sub_rte->subquery->limitOffset)
				get_plan_nodes(root, step, RELATION_ACCESS_READ);
		}

		/* Send to general planner if the query is multiple step */
		if (!step->exec_nodes)
			return;

		/* If the source is not hash-based (eg, replicated) also send
		 * through general planner
		 */
		if (step->exec_nodes->baselocatortype != LOCATOR_TYPE_HASH &&
			step->exec_nodes->baselocatortype != LOCATOR_TYPE_MODULO)
		{
			step->exec_nodes = NULL;
			return;
		}

		/*
		 * If step->exec_nodes is not null, it is single step.
		 * Continue and check for destination table type cases below
		 */
	}

	/*
	 * Search for an expression value that can be used for
	 * distribute table optimisation.
	 */
	if ((rel_loc_info->partAttrName != NULL) &&
		((rel_loc_info->locatorType == LOCATOR_TYPE_HASH) ||
		 (rel_loc_info->locatorType == LOCATOR_TYPE_MODULO)))
	{
		Expr		*checkexpr;
		TargetEntry *tle = NULL;

		/* It is a partitioned table, get value by looking in targetList */
		foreach(lc, query->targetList)
		{
			tle = (TargetEntry *) lfirst(lc);

			if (tle->resjunk)
				continue;

			/*
			 * See if we have a constant expression comparing against the
			 * designated partitioned column
			 */
			if (strcmp(tle->resname, rel_loc_info->partAttrName) == 0)
				break;
		}

		if (!lc)
		{
			/* Skip rest, handle NULL */
			step->exec_nodes = GetRelationNodes(rel_loc_info, 0, UNKNOWNOID, RELATION_ACCESS_INSERT);
			return;
		}

		/* We found the TargetEntry for the partition column */
		checkexpr = tle->expr;

		/* Handle INSERT SELECT case */
		if (query->jointree != NULL && query->jointree->fromlist != NULL)
		{
			if (IsA(checkexpr,Var))
			{
				XCWalkerContext context;
				ColumnBase *col_base;
				RelationLocInfo *source_rel_loc_info;

				/* Look for expression populating partition column */
				InitXCWalkerContext(&context);
				context.query = query;
				context.rtables = lappend(context.rtables, query->rtable);
				col_base = get_base_var((Var*) checkexpr, &context);

				if (!col_base)
				{
					step->exec_nodes = NULL;
					return;
				}

				/* See if it is also a partitioned table */
				source_rel_loc_info = GetRelationLocInfo(col_base->relid);

				if (!source_rel_loc_info)
					ereport(ERROR,
							(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
							(errmsg("Could not find relation for oid = %d", rte->relid))));

				if (col_base->colname != NULL &&
					source_rel_loc_info->partAttrName != NULL &&
					strcmp(col_base->colname, source_rel_loc_info->partAttrName) == 0 &&
					(source_rel_loc_info->locatorType == LOCATOR_TYPE_HASH ||
					 source_rel_loc_info->locatorType == LOCATOR_TYPE_MODULO))
				{
					/*
					 * Partition columns match, we have a "single-step INSERT SELECT".
					 * It is OK to use step->exec_nodes
					 */
					return;
				}
			}
			/* Multi-step INSERT SELECT or some other case. Use general planner */
			step->exec_nodes = NULL;
			return;
		}
		else
		{
			/* Check for constant */

			/* We may have a cast, try and handle it */
			if (!IsA(tle->expr, Const))
			{
				eval_expr = (Expr *) eval_const_expressions(NULL, (Node *) tle->expr);
				checkexpr = get_numeric_constant(eval_expr);
			}

			if (checkexpr == NULL)
			{
				/* try and determine nodes on execution time */
				step->exec_nodes = makeNode(ExecNodes);
				step->exec_nodes->baselocatortype = rel_loc_info->locatorType;
				step->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_USER;
				step->exec_nodes->primarynodelist = NULL;
				step->exec_nodes->nodeList = NULL;
				step->exec_nodes->en_expr = eval_expr;
				step->exec_nodes->en_relid = rel_loc_info->relid;
				step->exec_nodes->accesstype = RELATION_ACCESS_INSERT;
				return;
			}

			constExpr = (Const *) checkexpr;
		}
	}
	if (constExpr == NULL)
		step->exec_nodes = GetRelationNodes(rel_loc_info, 0, InvalidOid, RELATION_ACCESS_INSERT);
	else
		step->exec_nodes = GetRelationNodes(rel_loc_info, constExpr->constvalue, constExpr->consttype, RELATION_ACCESS_INSERT);

	if (eval_expr)
		pfree(eval_expr);
}

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
 * make_ctid_const
 *
 * creates a Const expression representing a ctid value (?,?)
 */

static Const *
make_ctid_const(char *ctid_string)
{
	Datum			val;
	Const			*ctid_const;

	val = PointerGetDatum(ctid_string);

	ctid_const = makeConst(UNKNOWNOID,
				-1,
				InvalidOid,
				-2,
				val,
				false,
				false);
	ctid_const->location = -1;
	return ctid_const;
}

/*
 * IsRelSame
 *
 * Does the two query trees have a common relation
 */

static bool
IsRelSame(List *upd_qry_rte, List *sel_qry_rte)
{
	ListCell		*lc1, *lc2;
	RangeTblEntry		*rte1, *rte2;

	foreach(lc1, upd_qry_rte)
	{
		rte1 = (RangeTblEntry *) lfirst(lc1);

		if (rte1->rtekind == RTE_RELATION)
		{
			foreach(lc2, sel_qry_rte)
			{
				rte2 = (RangeTblEntry *) lfirst(lc2);
	
				if (rte2->rtekind == RTE_RELATION)
				{
					if (rte2->relid == rte1->relid)
					{
						return true;
					}
				}
			}
		}
	}
	return false;
}

/*
 * pgxc_handle_current_of
 *
 * Handles UPDATE/DELETE WHERE CURRENT OF
 */

static bool
pgxc_handle_current_of(Node *expr_node, XCWalkerContext *context)
{
	/* Find referenced portal and figure out what was the last fetch node */
	Portal			portal;
	QueryDesc		*queryDesc;
	CurrentOfExpr		*cexpr = (CurrentOfExpr *) expr_node;
	char			*cursor_name = cexpr->cursor_name;
	PlanState		*ps;
	TupleTableSlot		*slot;
	RangeTblEntry		*table = (RangeTblEntry *) linitial(context->query->rtable);
	ScanState		*ss;

	/* Find the cursor's portal */
	portal = GetPortalByName(cursor_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
					errmsg("cursor \"%s\" does not exist", cursor_name)));

	queryDesc = PortalGetQueryDesc(portal);
	if (queryDesc == NULL || queryDesc->estate == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
					errmsg("cursor \"%s\" is held from a previous transaction",
						cursor_name)));

	/*
		* The cursor must have a current result row: per the SQL spec, it's
		* an error if not.
		*/
	if (portal->atStart || portal->atEnd)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
					errmsg("cursor \"%s\" is not positioned on a row",
						cursor_name)));

	ps = queryDesc->planstate;
	slot = NULL;

	ss = search_plan_tree(ps, table->relid);
	if (ss != NULL)
	{
		slot = ss->ss_ScanTupleSlot;
	}

	if (slot != NULL)
	{
		MemoryContext		oldcontext;
		MemoryContext		tmpcontext;
		RelationLocInfo		*loc_info;

		tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
							"Temp Context",
							ALLOCSET_DEFAULT_MINSIZE,
							ALLOCSET_DEFAULT_INITSIZE,
							ALLOCSET_DEFAULT_MAXSIZE);
		oldcontext = MemoryContextSwitchTo(tmpcontext);

		loc_info = GetRelationLocInfo(table->relid);
		if (!loc_info)
		{
			MemoryContextSwitchTo(oldcontext);
			MemoryContextDelete(tmpcontext);
			return true;
		}

		switch (loc_info->locatorType)
		{
			case LOCATOR_TYPE_HASH:
			case LOCATOR_TYPE_RROBIN:
			case LOCATOR_TYPE_MODULO:
			{
				Query			*temp_qry;
				Var			*ctid_expr;
				bool			ctid_found, node_str_found;
				StringInfoData		qry;
				TupleDesc		slot_meta = slot->tts_tupleDescriptor;
				char			*ctid_str = NULL;
				int			node_index = -1;
				int			i;
				Const			*cons_ctid;

				/* make a copy of the query so as not to touch the original query tree */
				temp_qry = copyObject(context->query);

				/* Make sure the relation referenced in cursor query and UPDATE/DELETE query is the same */
				if ( ! IsRelSame(temp_qry->rtable, queryDesc->plannedstmt->rtable ) )
				{
					char *tableName = get_rel_name(table->relid);
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_CURSOR_STATE),
							errmsg("cursor \"%s\" does not have a FOR UPDATE/SHARE reference to table \"%s\"",
									cursor_name, tableName)));
				}

				/* Delete existing WHERE CURRENT OF qual from the query tree*/
				pfree(((CurrentOfExpr *)(temp_qry->jointree->quals))->cursor_name);
				pfree((CurrentOfExpr *)temp_qry->jointree->quals);

				/* Make a ctid column ref expr for LHS of the operator */
				ctid_expr = make_ctid_col_ref(temp_qry);
				if (ctid_expr == NULL)
				{
					MemoryContextSwitchTo(oldcontext);
					MemoryContextDelete(tmpcontext);
					return true;	/* Bail out */
				}

				/*
				* Iterate over attributes and find ctid and node index. 
				* These attributes are most likely at the end of the list, 
				* so iterate in reverse order to find them quickly.
				* If not found target table is not updatable through
				* the cursor, report problem to client
				*/
				ctid_found = false;
				node_str_found = false;
				for (i = slot_meta->natts - 1; i >= 0; i--)
				{
					Form_pg_attribute attr = slot_meta->attrs[i];

					if (ctid_found == false)
					{
						if (strcmp(attr->attname.data, "ctid") == 0)
						{
							Datum		ctid = 0;

							ctid = slot->tts_values[i];
							ctid_str = (char *) DirectFunctionCall1(tidout, ctid);
							ctid_found = true;
						}
					}
					if (node_str_found == false)
					{
						if (strcmp(attr->attname.data, "pgxc_node_str") == 0)
						{
							Datum		data_node = 0;
							char		*data_node_str = NULL;

							data_node = slot->tts_values[i];
							data_node_str = (char *) DirectFunctionCall1(nameout, data_node);
							node_index = PGXCNodeGetNodeIdFromName(data_node_str, PGXC_NODE_DATANODE);
							node_str_found = true;
						}
					}
					if (ctid_found && node_str_found)
						break;
				}

				if (ctid_str == NULL || node_index < 0)
				{
					char *tableName = get_rel_name(table->relid);
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_CURSOR_STATE),
							errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
									cursor_name, tableName)));
				}

				/* Make the ctid value constant expr for RHS of the operator */
				cons_ctid = make_ctid_const(ctid_str);

				/* Make the new qual ctid = (?,?) */
				temp_qry->jointree->quals = (Node *)make_op(NULL, list_make1(makeString("=")), (Node *)ctid_expr, (Node *)cons_ctid, -1);

				/* Now deparse the query tree */
				initStringInfo(&qry);
				deparse_query(temp_qry, &qry, NIL);

				MemoryContextSwitchTo(oldcontext);

				if ( context->query_step->sql_statement != NULL )
					pfree(context->query_step->sql_statement);
				context->query_step->sql_statement = pstrdup(qry.data);

				MemoryContextDelete(tmpcontext);

				context->query_step->exec_nodes = makeNode(ExecNodes);
				context->query_step->exec_nodes->nodeList = list_make1_int(node_index);
				context->query_step->read_only = false;
				context->query_step->force_autocommit = false;

				return false;
			}
			case LOCATOR_TYPE_REPLICATED:
				MemoryContextSwitchTo(oldcontext);
				MemoryContextDelete(tmpcontext);

				return false;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("the distribution type is not supported")));
				return false; // or true
		}
	}
	else
	{
		char *tableName = get_rel_name(table->relid);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
						cursor_name, tableName)));
	}
	return false; // or true
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
 * If we encounter an expression such as a cross-node join that cannot
 * be easily handled in a single step, we stop processing and return true,
 * otherwise false.
 *
 */
static bool
examine_conditions_walker(Node *expr_node, XCWalkerContext *context)
{
	RelationLocInfo *rel_loc_info1,
			*rel_loc_info2;
	Const		*constant;
	Expr		*checkexpr;
	bool		result = false;
	bool		is_and = false;

	Assert(context);

	if (expr_node == NULL)
		return false;

	if (!context->rtables)
		return true;

	if (!context->conditions)
		context->conditions = new_special_conditions();

	/* Check if expression is foreign-safe for UPDATE/DELETE */
	if (context->accessType == RELATION_ACCESS_UPDATE &&
		!is_foreign_expr(expr_node, NULL))
		return true;

	/* Handle UPDATE/DELETE ... WHERE CURRENT OF ... */
	if (IsA(expr_node, CurrentOfExpr))
	{
		return pgxc_handle_current_of(expr_node, context);
	}

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
		Node       *leftarg = linitial(opexpr->args);

		/* See if we can equijoin these */
		if (op_mergejoinable(opexpr->opno, exprType(leftarg)) &&
			opexpr->args->length == 2)
		{
			Expr	   *arg1 = linitial(opexpr->args);
			Expr	   *arg2 = lsecond(opexpr->args);
			RelabelType *rt;
			Expr	   *targ;

			if (IsA(arg1, RelabelType))
			{
				rt = (RelabelType *) arg1;
				arg1 = rt->arg;
			}

			if (IsA(arg2, RelabelType))
			{
				rt = (RelabelType *)arg2;
				arg2 = rt->arg;
			}

			/* Handle constant = var */
			if (IsA(arg2, Var))
			{
				targ = arg1;
				arg1 = arg2;
				arg2 = targ;
			}

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
					Expr *eval_expr = (Expr *) eval_const_expressions(context->root, (Node *) arg2);
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

					/* If hash or modulo partitioned, check if the part column was used */
					if (IsHashColumn(rel_loc_info1, column_base->colname) ||
						IsModuloColumn(rel_loc_info1, column_base->colname))
					{
						/* add to partitioned literal join conditions */
						Literal_Comparison *lit_comp =
						palloc(sizeof(Literal_Comparison));

						lit_comp->relid = column_base->relid;
						lit_comp->rel_loc_info = rel_loc_info1;
						lit_comp->col_name = column_base->colname;
						lit_comp->constValue = constant->constvalue;
						lit_comp->constType = constant->consttype;

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
					if (!rel_loc_info2)
						return true;


					/* get data struct about these two relations joining */
					pgxc_join = find_or_create_pgxc_join(column_base->relid, column_base->relalias,
										 column_base2->relid, column_base2->relalias, context);

					if (rel_loc_info1->locatorType == LOCATOR_TYPE_REPLICATED)
					{

						/* add to replicated join conditions */
						context->conditions->replicated_joins =
							lappend(context->conditions->replicated_joins, pgxc_join);

						if (colvar->varlevelsup != colvar2->varlevelsup)
							context->multilevel_join = true;

						if (rel_loc_info2->locatorType == LOCATOR_TYPE_REPLICATED)
						{
							/*
							 * This is a replicated/replicated join case
							 * and node lists are mapping so node selection for remote join
							 * is made based on the intersection list of the two node lists.
							 */
							int len1 = NumDataNodes;
							int len2 = NumDataNodes;

							if (rel_loc_info1)
								len1 = list_length(rel_loc_info1->nodeList);
							if (rel_loc_info2)
								len2 = list_length(rel_loc_info2->nodeList);

							pgxc_join->join_type = JOIN_REPLICATED_ONLY;

							/* Be sure that intersection list can be built */
							if ((len1 != NumDataNodes ||
								 len2 != NumDataNodes) &&
								rel_loc_info1 &&
								rel_loc_info2)
							{
								List *join_node_list = list_intersection_int(rel_loc_info1->nodeList,
																			 rel_loc_info2->nodeList);
								context->conditions->base_rel_name = column_base2->relname;
								context->conditions->base_rel_loc_info = rel_loc_info2;
								/* Set a modified node list holding the intersection node list */
								context->conditions->base_rel_loc_info->nodeList =
									list_copy(join_node_list);
								if (rel_loc_info1)
									FreeRelationLocInfo(rel_loc_info1);
							}
						}
						else
						{
							pgxc_join->join_type = JOIN_REPLICATED_PARTITIONED;

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

						return false;
					}
					else if (rel_loc_info2->locatorType == LOCATOR_TYPE_REPLICATED)
					{
						/* note nature of join between the two relations */
						pgxc_join->join_type = JOIN_REPLICATED_PARTITIONED;

						/* add to replicated join conditions */
						context->conditions->replicated_joins =
							lappend(context->conditions->replicated_joins, pgxc_join);

						/* other relation not replicated, note it for later */
						context->conditions->base_rel_name = column_base->relname;
						context->conditions->base_rel_loc_info = rel_loc_info1;

						if (rel_loc_info2)
							FreeRelationLocInfo(rel_loc_info2);

						return false;
					}
					/* Now check for a partitioned join */
					if (((IsHashColumn(rel_loc_info1, column_base->colname)) &&
						 (IsHashColumn(rel_loc_info2, column_base2->colname))) ||
						((IsModuloColumn(rel_loc_info1, column_base->colname)) &&
						(IsModuloColumn(rel_loc_info2, column_base2->colname))))
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

				/*
				 * Check if it is an expression like pcol = expr, where pcol is
				 * a partitioning column of the rel1 and planner could not
				 * evaluate expr. We probably can evaluate it at execution time.
				 * Save the expression, and if we do not have other hint,
				 * try and evaluate it at execution time
				 */
				rel_loc_info1 = GetRelationLocInfo(column_base->relid);

				if (!rel_loc_info1)
					return true;

				if (IsHashColumn(rel_loc_info1, column_base->colname) ||
				    IsModuloColumn(rel_loc_info1, column_base->colname))
				{
					Expr_Comparison *expr_comp =
						palloc(sizeof(Expr_Comparison));

					expr_comp->relid = column_base->relid;
					expr_comp->rel_loc_info = rel_loc_info1;
					expr_comp->col_name = column_base->colname;
					expr_comp->expr = arg2;
					context->conditions->partitioned_expressions =
						lappend(context->conditions->partitioned_expressions,
								expr_comp);
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
		ExecNodes *save_exec_nodes = context->query_step->exec_nodes; /* Save old exec_nodes */

		/* save parent-child count */
		if (context->query_step->exec_nodes)
			save_parent_child_count = list_length(context->conditions->partitioned_parent_child);

		context->query_step->exec_nodes = NULL;
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
		if (!context->query_step->exec_nodes)
			context->query_step->exec_nodes = save_exec_nodes;
		else
		{
			if (save_exec_nodes)
			{
				if (context->query_step->exec_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED)
				{
					context->query_step->exec_nodes = save_exec_nodes;
				}
				else
				{
					if (save_exec_nodes->tableusagetype != TABLE_USAGE_TYPE_USER_REPLICATED)
					{
						/* See if they run on the same node */
						if (same_single_node(context->query_step->exec_nodes->nodeList,
											 save_exec_nodes->nodeList))
							return false;
					}
					else
						/* use old value */
						context->query_step->exec_nodes = save_exec_nodes;
				}
			} else
			{
				if (context->query_step->exec_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED)
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

		/* Block FULL JOIN expressions until it is supported */
		if (joinexpr->jointype == JOIN_FULL)
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 (errmsg("FULL JOIN clause not yet supported"))));

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
 * Returns true if tables in this list have their node list mapping.
 * The node list determines on which subset of nodes table data is located.
 */
static bool
is_subcluster_mapping(List *rtable)
{
	ListCell *item;
	char	 global_locator_type = LOCATOR_TYPE_NONE;
	List	 *inter_nodelist = NIL;

	/* Single table, so do not matter */
	if (list_length(rtable) == 1)
		return true;

	/*
	 * Successive lists of node lists of rtables are checked by taking
	 * their successive intersections for mapping.
	 * As a global rule, distributed table mapping has to be at least
	 * in reference mapping.
	 *
	 * The following cases are considered:
	 * - replicated/replicated mapping
	 *	 Intersection of node lists cannot be disjointed.
	 * - replicated/distributed mapping
	 *	 Distributed mapping has to be included in replicated mapping
	 * - distributed/distributed mapping
	 *	 Node lists have to include each other, so they are equal.
	 */
	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);
		RelationLocInfo *rel_loc_info;

		/* Don't mind if it is not a relation */
		if (rte->rtekind != RTE_RELATION)
			continue;

		/* Time to get the necessary location data */
		rel_loc_info = GetRelationLocInfo(rte->relid);

		/*
		 * If no location info is found it means that relation is
		 * local like a catalog, this does not impact mapping check.
		 */
		if (!rel_loc_info)
			continue;

		/* Initialize location information to be checked */
		if (IsLocatorNone(global_locator_type))
		{
			global_locator_type = rel_loc_info->locatorType;
			inter_nodelist = list_copy(rel_loc_info->nodeList);
			continue;
		}

		/* Time for the real checking */
		if (IsLocatorReplicated(global_locator_type) &&
			IsLocatorReplicated(rel_loc_info->locatorType))
		{
			/*
			 * Replicated/replicated join case
			 * Check that replicated relation is not disjoint
			 * with initial relation which is also replicated.
			 * If there is a common portion of the node list between
			 * the two relations, other rtables have to be checked on
			 * this restricted list.
			 */
			inter_nodelist = list_intersection_int(inter_nodelist,
												   rel_loc_info->nodeList);
			/* No intersection, so has to go though standard planner... */
			if (!inter_nodelist)
				return false;
		}
		else if ((IsLocatorReplicated(global_locator_type) &&
				  IsLocatorColumnDistributed(rel_loc_info->locatorType)) ||
				 (IsLocatorColumnDistributed(global_locator_type) &&
				  IsLocatorReplicated(rel_loc_info->locatorType)))
		{
			/*
			 * Replicated/distributed join case.
			 * Node list of distributed table has to be included
			 * in node list of replicated table.
			 */
			List *diff_nodelist = NIL;

			/*
			 * In both cases, check that replicated table node list maps entirely
			 * distributed table node list.
			 */
			if (IsLocatorReplicated(global_locator_type))
			{
				diff_nodelist = list_difference_int(rel_loc_info->nodeList, inter_nodelist);

				/* Save new comparison info */
				inter_nodelist = list_copy(rel_loc_info->nodeList);
				global_locator_type = rel_loc_info->locatorType;
			}
			else
				diff_nodelist = list_difference_int(inter_nodelist, rel_loc_info->nodeList);

			/*
			 * If the difference list is not empty, this means that node list of
			 * distributed table is not completely mapped by node list of replicated
			 * table, so go through standard planner.
			 */
			if (diff_nodelist)
				return false;
		}
		else if (IsLocatorColumnDistributed(global_locator_type) &&
				 IsLocatorColumnDistributed(rel_loc_info->locatorType))
		{
			/*
			 * Distributed/distributed case.
			 * Tables have to map perfectly.
			 */
			List *diff_left = list_difference_int(inter_nodelist, rel_loc_info->nodeList);
			List *diff_right = list_difference_int(rel_loc_info->nodeList, inter_nodelist);

			/* Mapping is disjointed, so this goes through standard planner */
			if (diff_left || diff_right)
				return false;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Postgres-XC does not support this distribution type yet"),
					 errdetail("The feature is not currently supported")));
		}
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
	Query		*query;
	RangeTblEntry	*rte;
	ListCell	*lc, *item;
	RelationLocInfo	*rel_loc_info;
	ExecNodes	*test_exec_nodes = NULL;
	ExecNodes	*current_nodes = NULL;
	ExecNodes	*from_query_nodes = NULL;
	TableUsageType	table_usage_type = TABLE_USAGE_TYPE_NO_TABLE;
	TableUsageType	current_usage_type = TABLE_USAGE_TYPE_NO_TABLE;
	int		from_subquery_count = 0;

	if (!query_node && !IsA(query_node,Query))
		return true;

	/* if EXECUTE DIRECT, just return */
	if (context->query_step->exec_direct_type != EXEC_DIRECT_NONE)
		return false;

	query = (Query *) query_node;

	/* If no tables, just return */
	if (query->rtable == NULL && query->jointree == NULL)
		return false;

	/* Look for special conditions */

	/*
	 * Check node list for each table specified.
	 * Nodes where is located data have to map in case of queries
	 * involving multiple tables.
	 */
	if (!is_subcluster_mapping(query->rtable))
		return true; /* Run through standard planner */

	/*
	 * Examine projection list, to handle cases like
	 * SELECT col1, (SELECT col2 FROM non_replicated_table...), ...
	 * PGXCTODO: Improve this to allow for partitioned tables
	 * where all subqueries and the main query use the same single node
	 */
	if (query->targetList)
	{
		ExecNodes *save_nodes = context->query_step->exec_nodes;
		int save_varno = context->varno;

		foreach(item, query->targetList)
		{
			TargetEntry	   *target = (TargetEntry *) lfirst(item);

			context->query_step->exec_nodes = NULL;
			context->varno = 0;

			if (examine_conditions_walker((Node*)target->expr, context))
				return true;

			if (context->query_step->exec_nodes)
			{
				/*
				 * if it is not replicated, assume it is something complicated and go
				 * through standard planner
				 */
				if (context->query_step->exec_nodes->tableusagetype != TABLE_USAGE_TYPE_USER_REPLICATED)
					return true;

				pfree(context->query_step->exec_nodes);
			}
		}
		context->query_step->exec_nodes = save_nodes;
		context->varno = save_varno;
	}
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
					context->query_step->exec_nodes = makeNode(ExecNodes);
					context->query_step->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_PGCATALOG;
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
				ExecNodes *save_exec_nodes = context->query_step->exec_nodes;
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

				current_nodes = context->query_step->exec_nodes;
				context->query_step->exec_nodes = save_exec_nodes;

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
						if (!same_single_node(from_query_nodes->nodeList, current_nodes->nodeList))
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
					{
						/* Check if this relation is a sequence */
						if (get_rel_relkind(rte->relid) == RELKIND_SEQUENCE)
							current_usage_type = TABLE_USAGE_TYPE_SEQUENCE;
						else
							current_usage_type = TABLE_USAGE_TYPE_USER;
					}
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

	/* If we are just dealing with pg_catalog or a sequence, just return. */
	if (table_usage_type == TABLE_USAGE_TYPE_PGCATALOG ||
		table_usage_type == TABLE_USAGE_TYPE_SEQUENCE)
	{
		context->query_step->exec_nodes = makeNode(ExecNodes);
		context->query_step->exec_nodes->tableusagetype = table_usage_type;
		context->exec_on_coord = true;
		return false;
	}

	/*
	 * From this point onwards, opfuncids should be filled to determine
	 * immuability of functions. View RTE does not have function oids populated
	 * in its quals at this point.
	 */
	fix_opfuncids((Node *) query->jointree->quals);

	/* Examine the WHERE clause, too */
	if (examine_conditions_walker(query->jointree->quals, context) ||
		!is_foreign_expr(query->jointree->quals, NULL))
		return true;

	if (context->query_step->exec_nodes)
		return false;

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
				if ((!rte->inFromCl && query->commandType == CMD_SELECT) ||
					rte->rtekind != RTE_RELATION)
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
				/* a single table, just grab it */
				rel_loc_info = GetRelationLocInfo(rtesave->relid);
		}

		/* have complex case */
		if (!rel_loc_info)
			return true;

		if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH &&
			rel_loc_info->locatorType != LOCATOR_TYPE_MODULO)
		{
			/* do not need to determine partitioning expression */
			context->query_step->exec_nodes = GetRelationNodes(rel_loc_info, 0, UNKNOWNOID, context->accessType);
		}

		/* Note replicated table usage for determining safe queries */
		if (context->query_step->exec_nodes)
		{
			if (table_usage_type == TABLE_USAGE_TYPE_USER &&
				IsLocatorReplicated(rel_loc_info->locatorType))
				table_usage_type = TABLE_USAGE_TYPE_USER_REPLICATED;

			context->query_step->exec_nodes->tableusagetype = table_usage_type;
		}
		else if (context->conditions->partitioned_expressions)
		{
			/* probably we can determine nodes on execution time */
			foreach(lc, context->conditions->partitioned_expressions)
			{
				Expr_Comparison *expr_comp = (Expr_Comparison *) lfirst(lc);
				if (rel_loc_info->relid == expr_comp->relid)
				{
					context->query_step->exec_nodes = makeNode(ExecNodes);
					context->query_step->exec_nodes->baselocatortype = rel_loc_info->locatorType;
					context->query_step->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_USER;
					context->query_step->exec_nodes->primarynodelist = NULL;
					context->query_step->exec_nodes->nodeList = NULL;
					context->query_step->exec_nodes->en_expr = expr_comp->expr;
					context->query_step->exec_nodes->en_relid = expr_comp->relid;
					context->query_step->exec_nodes->accesstype = context->accessType;
					break;
				}
			}
		}
		else
		{
			/* run query on all nodes */
			context->query_step->exec_nodes = makeNode(ExecNodes);
			context->query_step->exec_nodes->baselocatortype = rel_loc_info->locatorType;
			context->query_step->exec_nodes->tableusagetype = TABLE_USAGE_TYPE_USER;
			context->query_step->exec_nodes->primarynodelist = NULL;
			context->query_step->exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
			context->query_step->exec_nodes->en_expr = NULL;
			context->query_step->exec_nodes->en_relid = InvalidOid;
			context->query_step->exec_nodes->accesstype = context->accessType;
		}
	}
	/* check for partitioned col comparison against a literal */
	else if (list_length(context->conditions->partitioned_literal_comps) > 0)
	{
		bool is_single_node_safe = true;

		context->query_step->exec_nodes = NULL;

		/*
		 * We may have a literal comparison with a parent-child join
		 * on a distributed table. In this case choose node targetting the column.
		 * But first check if it is targetting multiple distributed tables.
		 */
		if (list_length(context->query->rtable) > 1 &&
			!context->conditions->partitioned_parent_child)
		{
			ListCell *lc;
			RangeTblEntry *save_rte = NULL;
			RelationLocInfo *save_loc_info;

			foreach(lc, context->query->rtable)
			{
				RangeTblEntry *rte = lfirst(lc);

				/* This check is not necessary if rte is not a relation */
				if (rte->rtekind != RTE_RELATION)
					continue;

				if (!save_rte)
				{
					save_rte = rte;
					save_loc_info = GetRelationLocInfo(save_rte->relid);
				}
				else
				{
					/*
					 * If there are two distributed tables at least
					 * among target tables, push down the query to all nodes.
					 */
					if (save_rte->relid != rte->relid)
					{
						RelationLocInfo *loc_info = GetRelationLocInfo(rte->relid);

						if (loc_info->locatorType != LOCATOR_TYPE_REPLICATED &&
							save_loc_info->locatorType != LOCATOR_TYPE_REPLICATED)
							is_single_node_safe = false;
						if (loc_info->locatorType != LOCATOR_TYPE_REPLICATED &&
							save_loc_info->locatorType == LOCATOR_TYPE_REPLICATED)
						{
							save_rte = rte;
							save_loc_info = loc_info;
						}
					}
				}
			}
		}

		/*
		 * Make sure that if there are multiple such comparisons, that they
		 * are all on the same nodes.
		 */
		foreach(lc, context->conditions->partitioned_literal_comps)
		{
			Literal_Comparison *lit_comp = (Literal_Comparison *) lfirst(lc);

			test_exec_nodes = GetRelationNodes(lit_comp->rel_loc_info,
											   lit_comp->constValue,
											   lit_comp->constType,
											   RELATION_ACCESS_READ);

			test_exec_nodes->tableusagetype = table_usage_type;
			if (is_single_node_safe &&
				context->query_step->exec_nodes == NULL)
				context->query_step->exec_nodes = test_exec_nodes;
			else
			{
				if (context->query_step->exec_nodes == NULL ||
					!is_single_node_safe ||
					!same_single_node(context->query_step->exec_nodes->nodeList, test_exec_nodes->nodeList))
					return true;
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

		context->query_step->exec_nodes = GetRelationNodes(parent_child->rel_loc_info1,
														   0,
														   UNKNOWNOID,
														   context->accessType);
		context->query_step->exec_nodes->tableusagetype = table_usage_type;
	}

	if (from_query_nodes)
	{
		if (!context->query_step->exec_nodes)
		{
			context->query_step->exec_nodes = from_query_nodes;
			return false;
		}
		/* Just use exec_nodes if the from subqueries are all replicated or using the exact
		 * same node
		 */
		else if (from_query_nodes->tableusagetype == TABLE_USAGE_TYPE_USER_REPLICATED
					|| (same_single_node(from_query_nodes->nodeList, context->query_step->exec_nodes->nodeList)))
				return false;
		else
		{
			/* We allow views, where the (rewritten) subquery may be on all nodes,
			 * but the parent query applies a condition on the from subquery.
			 */
			if (list_length(query->jointree->fromlist) == from_subquery_count
					&& list_length(context->query_step->exec_nodes->nodeList) == 1)
				return false;
		}
		/* Too complicated, give up */
		return true;
	}

	return false;
}

/*
 * Set initial values for expression walker
 */
static void
InitXCWalkerContext(XCWalkerContext *context)
{
	context->query = NULL;
	context->accessType = RELATION_ACCESS_READ;
	context->query_step = NULL;
	context->root = NULL;
	context->conditions = (Special_Conditions *) palloc0(sizeof(Special_Conditions));
	context->rtables = NIL;
	context->multilevel_join = false;
	context->varno = 0;
	context->within_or = false;
	context->within_not = false;
	context->exec_on_coord = false;
	context->join_list = NIL;
}


/*
 * Create an instance of RemoteQuery and initialize fields
 */
static RemoteQuery *
makeRemoteQuery(void)
{
	RemoteQuery *result = makeNode(RemoteQuery);
	result->exec_nodes = NULL;
	result->combine_type = COMBINE_TYPE_NONE;
	result->sort = NULL;
	result->distinct = NULL;
	result->read_only = true;
	result->force_autocommit = false;
	result->cursor = NULL;
	result->exec_type = EXEC_ON_DATANODES;
	result->exec_direct_type = EXEC_DIRECT_NONE;
	result->is_temp = false;

	result->relname = NULL;
	result->remotejoin = false;
	result->partitioned_replicated = false;
	result->reduce_level = 0;
	result->base_tlist = NIL;
	result->outer_alias = NULL;
	result->inner_alias = NULL;
	result->outer_reduce_level = 0;
	result->inner_reduce_level = 0;
	result->outer_relids = NULL;
	result->inner_relids = NULL;
	result->inner_statement = NULL;
	result->outer_statement = NULL;
	result->join_condition = NULL;
	result->sql_statement = NULL;
	return result;
}

/*
 * Top level entry point before walking query to determine plan nodes
 *
 */
static void
get_plan_nodes(PlannerInfo *root, RemoteQuery *step, RelationAccessType accessType)
{
	Query	   *query = root->parse;
	XCWalkerContext context;

	InitXCWalkerContext(&context);
	context.query = query;
	context.accessType = accessType;
	context.query_step = step;
	context.root = root;
	context.rtables = lappend(context.rtables, query->rtable);
	if ((get_plan_nodes_walker((Node *) query, &context)
		 || context.exec_on_coord) && context.query_step->exec_nodes)
	{
		pfree(context.query_step->exec_nodes);
		context.query_step->exec_nodes = NULL;
	}
	free_special_relations(context.conditions);
	free_join_list(context.join_list);
}


/*
 * get_plan_nodes_command - determine the nodes to execute the plan on
 *
 */
static void
get_plan_nodes_command(RemoteQuery *step, PlannerInfo *root)
{
	switch (root->parse->commandType)
	{
		case CMD_SELECT:
			get_plan_nodes(root, step, root->parse->rowMarks ?
											RELATION_ACCESS_READ_FOR_UPDATE :
												RELATION_ACCESS_READ);
			break;

		case CMD_INSERT:
			get_plan_nodes_insert(root, step);
			break;

		case CMD_UPDATE:
		case CMD_DELETE:
			/* treat as a select */
			get_plan_nodes(root, step, RELATION_ACCESS_UPDATE);
			break;

		default:
			break;
	}
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
	char	   *sql_from;
	int			count_from = 1;

	context = deparse_context_for_plan((Node *) step, NULL, rtable);
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

		if (tle->resname != NULL)
		{
			/*
			 * Check if relation aliases are using keyword FROM
			 * If yes, replace that with _FROM_ to avoid conflicts with
			 * ORDER BY query reconstruction below.
			 */
			if (strpos((char *)quote_identifier(tle->resname), " FROM ", 1))
			{
				char   *buffer;
				int		cnt;
				buffer = strreplace((char *)quote_identifier(tle->resname),
									" FROM ", "_FROM_", &cnt);
				appendStringInfo(buf, " AS %s", buffer);
				pfree(buffer);
				count_from += cnt;
			}
			else
				appendStringInfo(buf, " AS %s", quote_identifier(tle->resname));
		}
	}

	/*
	 * A kind of dummy
	 * Do not reconstruct remaining query, just search original statement
	 * for " FROM " and append remainder to the target list we just generated.
	 * Do not handle the case if " FROM " we found is not a "FROM" keyword, but,
	 * for example, a part of string constant.
	 */
	sql_from = strpos(step->sql_statement, " FROM ", count_from++);
	if (sql_from)
	{
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

		appendStringInfoString(buf, sql_from);
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

	/* free previous query */
	pfree(step->sql_statement);
	/* get a copy of new query */
	step->sql_statement = pstrdup(buf->data);
	/* free the query buffer */
	pfree(buf->data);
	pfree(buf);
}


/*
 * Traverse the plan subtree and set cursor name for RemoteQuery nodes
 * Cursor names must be unique, so append step_no parameter to the initial
 * cursor name. Returns next step_no to be assigned
 */
static int
set_cursor_name(Plan *subtree, char *cursor, int step_no)
{
	if (innerPlan(subtree))
		step_no = set_cursor_name(innerPlan(subtree), cursor, step_no);
	if (outerPlan(subtree))
		step_no = set_cursor_name(outerPlan(subtree), cursor, step_no);
	if (IsA(subtree, RemoteQuery))
	{
		RemoteQuery *step = (RemoteQuery *) subtree;
		/*
		 * Keep the name for the very first step, hoping it is the only step and
		 * we do not have to modify WHERE CURRENT OF
		 */
		if (step_no)
		{
			StringInfoData buf;
			initStringInfo(&buf);
			appendStringInfo(&buf, "%s%d", cursor, step_no++);
			/* make a copy before overwriting */
			step->cursor = buf.data;
		}
		else
		{
			step_no++;
			step->cursor = pstrdup(cursor);
		}
	}
	return step_no;
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
 * to find target tuple
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
		 * 4.  DISTINCT and ORDER BY are compatible, if we have remaining items
		 * 	   in the working copy we should append it to the order by list
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
				list_length(query_step->exec_nodes->nodeList) <= 1)
		return 0;

	/* if order by and limit are present, do not optimize yet */
	if ((query->limitCount || query->limitOffset) && query->sortClause)
		return 1;

	/*
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
			sprintf(newchar, "LIMIT " INT64_FORMAT, newLimit);
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

	/* see if can ship the query completely */
	result = pgxc_fqs_planner(query, cursorOptions, boundParams);

	/* we need coordinator for evaluation, invoke standard planner */
	if (!result)
		result = standard_planner(query, cursorOptions, boundParams);

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
 * pgxc_fqs_planner
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
pgxc_fqs_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;
	PlannerGlobal *glob;
	PlannerInfo *root;
	RemoteQuery *query_step;
	StringInfoData buf;

	/* Try by-passing standard planner, if fast query shipping is enabled */
	if (!enable_fast_query_shipping)
		return NULL;
	/*
	 * If the query needs coordinator for evaluation or the query can be
	 * completed on coordinator itself, we plan it through standard_planner()
	 */
	if (pgxc_query_needs_coord(query))
		return NULL;

	/*
	 * Set up global state for this planner invocation.  This data is needed
	 * across all levels of sub-Query that might exist in the given command,
	 * so we keep it in a separate struct that's linked to by each per-Query
	 * PlannerInfo.
	 */
	glob = makeNode(PlannerGlobal);

	glob->boundParams = boundParams;
	glob->paramlist = NIL;
	glob->subplans = NIL;
	glob->subrtables = NIL;
	glob->rewindPlanIDs = NULL;
	glob->finalrtable = NIL;
	glob->relationOids = NIL;
	glob->invalItems = NIL;
	glob->lastPHId = 0;
	glob->transientPlan = false;

	/* Create a PlannerInfo data structure, usually it is done for a subquery */
	root = makeNode(PlannerInfo);
	root->parse = query;
	root->glob = glob;
	root->query_level = 1;
	root->parent_root = NULL;
	root->planner_cxt = CurrentMemoryContext;
	root->init_plans = NIL;
	root->cte_plan_ids = NIL;
	root->eq_classes = NIL;
	root->append_rel_list = NIL;

	/* build the PlannedStmt result */
	result = makeNode(PlannedStmt);

	/* Try and set what we can */
	result->commandType = query->commandType;
	result->canSetTag = query->canSetTag;
	result->utilityStmt = query->utilityStmt;
	result->intoClause = query->intoClause;
	result->rtable = query->rtable;

	/* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
	if (query->utilityStmt
		&& IsA(query->utilityStmt, RemoteQuery))
	{
		RemoteQuery *stmt = (RemoteQuery *) query->utilityStmt;
		if (stmt->exec_direct_type != EXEC_DIRECT_NONE)
		{
			query_step = stmt;
			query->utilityStmt = NULL;
			result->utilityStmt = NULL;
		}
		else
		{
			query_step = makeRemoteQuery();
			query_step->exec_nodes = query->execNodes;
		}
	}
	else
	{
		query_step = makeRemoteQuery();
		query_step->exec_nodes = query->execNodes;
	}

	/* Optimize multi-node handling */
	query_step->read_only = query->commandType == CMD_SELECT;

	if (query->utilityStmt &&
		IsA(query->utilityStmt, DeclareCursorStmt))
			cursorOptions |= ((DeclareCursorStmt *) query->utilityStmt)->options;

	result->planTree = (Plan *) query_step;

	/* Set result relations */
	if (query->commandType != CMD_SELECT)
		result->resultRelations = list_make1_int(query->resultRelation);

	/* Check if temporary tables are in use in target list */
	if (contains_temp_tables(query->rtable))
		query_step->is_temp = true;

	if (query_step->exec_nodes == NULL)
		get_plan_nodes_command(query_step, root);

	if (query_step->exec_nodes == NULL)
	{
		/*
		 * Processing guery against catalog tables, or multi-step command.
		 * Need both coordinator and datanodes. Run through standard planner.
		 */
		return NULL;
	}
	/* Datanodes should finalise the results of this query */
	query->qry_finalise_aggs = true;

	/*
	 * Deparse query tree to get step query. It may be modified later on
	 */
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
	 * structure through the standard_planner
	 */
	query->qry_finalise_aggs = false;
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
	/*
	 * Add sorting to the step
	 */
	if (list_length(query_step->exec_nodes->nodeList) > 1 &&
			(query->sortClause || query->distinctClause))
		make_simple_sort_from_sortclauses(query, query_step);

	/* Handle LIMIT and OFFSET for single-step queries on multiple nodes */
	if (handle_limit_offset(query_step, query, result))
		return NULL;

	/*
	 * If creating a plan for a scrollable cursor, make sure it can run
	 * backwards on demand.  Add a Material node at the top at need.
	 */
	if ((cursorOptions & CURSOR_OPT_SCROLL) &&
		!ExecSupportsBackwardScan(result->planTree))
			result->planTree = materialize_finished_plan(result->planTree);

	/*
	 * Support for multi-step cursor.
	 * Ensure uniqueness of remote cursor name
	 * Small optimization for SCROLL (read-only) cursors: do not use Extended
	 * Query protocol
	 */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, DeclareCursorStmt) &&
		(cursorOptions & CURSOR_OPT_SCROLL) == 0)
	{
		DeclareCursorStmt *stmt = (DeclareCursorStmt *) query->utilityStmt;
		set_cursor_name(result->planTree, stmt->portalname, 0);
	}

	/*
	 * If query is DECLARE CURSOR fetch CTIDs and node names from the remote node
	 * Use CTID as a key to update/delete tuples on remote nodes when handling
	 * WHERE CURRENT OF
	 */
	if ( query->utilityStmt && IsA(query->utilityStmt, DeclareCursorStmt) )
	{
		fetch_ctid_of(result->planTree, query);
	}

	return result;
}

/*
 * pgxc_query_needs_coord
 * Check if the query needs coordinator for evaluation or it can be completely
 * evaluated on coordinator. Return true if so, otherwise return false
 */
static bool
pgxc_query_needs_coord(Query *query)
{
	/*
	 * if the query has its utility set, it could be an EXEC_DIRECT statement,
	 * check if it needs to be executed on coordinator
	 */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, RemoteQuery))
	{
		RemoteQuery *node = (RemoteQuery *)query->utilityStmt;

		/* EXECUTE DIRECT statements on coordinator will need coordinator */
		if(node->exec_direct_type == EXEC_DIRECT_LOCAL ||
			node->exec_direct_type == EXEC_DIRECT_LOCAL_UTILITY)
			return true;

		/* EXECUTE DIRECT statements on remote nodes don't need coordinator */
		if (node->exec_direct_type != EXEC_DIRECT_NONE)
			return false;
	}

	/*
	 * If the query involves just the catalog tables, and is not an EXEC DIRECT
	 * statement, it can be evaluated completely on the coordinator. No need to
	 * involve datanodes.
	 */
	if (contains_only_pg_catalog(query->rtable))
		return true;

	/* correlated UPDATE or DELETE needs to be handled at the coordinator */
	if ((query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE)
			&& list_length(query->rtable) > 1)
		return true;

	/*
	 * PGXCTODO - this could be improved to check if the first
	 * group by expression is the partitioning column, in which
	 * case it is ok to treat as a single step.
	 * PGXCTODO - whatever number of nodes involved in the query, grouping,
	 * windowing and recursive queries take place at the coordinator. The
	 * corresponding planner should be able to optimize the queries such that
	 * most of the query is pushed to datanode, based on the kind of
	 * distribution the table has.
	 */
	if (query->commandType == CMD_SELECT
					&& (query->hasAggs ||
						query->groupClause ||
						query->havingQual ||
						query->hasWindowFuncs ||
						query->hasRecursive ||
						query->intoClause))
		return true;

	/*
	 * Handle INSERT INTO .. SELECT through standard planner. There might be
	 * some cases where we should be able to push down these queries to the
	 * data node, but that needs to be thouroughly tested. For example, if the
	 * source and target tables are distributed on the same column, then we
	 * should be able to push that down. Similarly, if both the tables are
	 * replicated, we should be able to push down the queries. And we have some
	 * of these checks in the pgxc_planner. But I am not sure if its well
	 * tested and hence closing that path for now
	 */
	if (query->commandType == CMD_INSERT)
	{
		ListCell *l;

		foreach (l, query->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
			if (rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_VALUES)
				return true;
		}
	}

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
bool
IsJoinReducible(RemoteQuery *innernode, RemoteQuery *outernode,
			List *rtable_list, JoinPath *join_path, JoinReduceInfo *join_info)
{
	XCWalkerContext context;
	ListCell *cell;
	bool maybe_reducible = false;
	bool result = false;

	Assert(join_info);
	join_info->partitioned_replicated = false;
	join_info->exec_nodes = NULL;

	/*
	 * Before examining such conditions, we need to be sure that node lists
	 * of tables are mapping.
	 */
	if (!is_subcluster_mapping(rtable_list))
		return false;

	InitXCWalkerContext(&context);
	context.accessType = RELATION_ACCESS_READ; /* PGXCTODO - determine */
	context.rtables = NIL;
	context.rtables = lappend(context.rtables, rtable_list); /* add to list of lists */

	foreach(cell, join_path->joinrestrictinfo)
	{
		RestrictInfo       *node = (RestrictInfo *) lfirst(cell);

		/*
		* Check if we can fold these safely.
		*
		* If examine_conditions_walker() returns true,
		* then it definitely is not collapsable.
		* If it returns false, it may or may not be, we have to check
		* context.conditions at the end.
		* We keep trying, because another condition may fulfill the criteria.
		*/
		maybe_reducible = !examine_conditions_walker((Node *) node->clause, &context);

		if (!maybe_reducible)
			break;

	}

	/* check to see if we found any partitioned or replicated joins */
	if (maybe_reducible &&
				(context.conditions->partitioned_parent_child
			  || context.conditions->replicated_joins))
	{
		/*
		 * If we get here, we think that we can fold the
		 * RemoteQuery nodes into a single one.
		 */
		result = true;

		/* Check replicated-replicated and replicated-partitioned joins */
		if (context.conditions->replicated_joins)
		{
			ListCell *cell;

			/* if we already reduced with replicated tables already, we
			 * cannot here.
			 * PGXCTODO - handle more cases and use outer_relids and inner_relids
			 * For now we just give up.
			 */
			if ((innernode->remotejoin && innernode->partitioned_replicated) &&
						(outernode->remotejoin && outernode->partitioned_replicated))
			{
				/* not reducible after all */
				return false;
			}

			foreach(cell, context.conditions->replicated_joins)
			{
				PGXC_Join *pgxc_join = (PGXC_Join *) lfirst(cell);

				if (pgxc_join->join_type == JOIN_REPLICATED_PARTITIONED)
				{
					join_info->partitioned_replicated = true;

					/*
					 * If either of these already have such a join, we do not
					 * want to add it a second time.
					 */
					if ((innernode->remotejoin && innernode->partitioned_replicated) ||
						(outernode->remotejoin && outernode->partitioned_replicated))
					{
						/* not reducible after all */
						return false;
					}
				}
			}
		}
	}

	if (result)
	{
		/*
		 * Set exec_nodes from walker if it was set.
		 * If not, it is replicated and we can use existing
		 */
		if (context.query_step)
			join_info->exec_nodes = copyObject(context.query_step->exec_nodes);
		else
			join_info->exec_nodes = copyObject(outernode->exec_nodes);
	}

	return result;
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


	if (rte != NULL && rte->rtekind != RTE_RELATION)
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
 * GetHashExecNodes -
 *	Get hash key of execution nodes according to the expression value
 *
 * Input parameters: 
 *	rel_loc_info is a locator function. It contains distribution information.
 *	exec_nodes is the list of nodes to be executed
 *	expr is the partition column value
 *
 * code is borrowed from get_plan_nodes_insert
 */
void
GetHashExecNodes(RelationLocInfo *rel_loc_info, ExecNodes **exec_nodes, const Expr *expr)
{
	/* We may have a cast, try and handle it */
	Expr	   *checkexpr;
	Expr	   *eval_expr = NULL;
	Const	   *constant;

	eval_expr = (Expr *) eval_const_expressions(NULL, (Node *)expr);
	checkexpr = get_numeric_constant(eval_expr);

	if (checkexpr == NULL)
		return;

	constant = (Const *) checkexpr;

	/* single call handles both replicated and partitioned types */
	*exec_nodes = GetRelationNodes(rel_loc_info,
								   constant->constvalue,
								   constant->consttype,
								   RELATION_ACCESS_INSERT);
	if (eval_expr)
		pfree(eval_expr);

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
