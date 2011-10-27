/*-------------------------------------------------------------------------
 *
 * postgresql_fdw.c
 *		  foreign-data wrapper for PostgreSQL
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "pgxc/postgresql_fdw.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define DEBUG_FDW

/*
 * WHERE clause optimization level
 */
#define EVAL_QUAL_LOCAL		0	/* evaluate none in foreign, all in local */
#define EVAL_QUAL_BOTH		1	/* evaluate some in foreign, all in local */
#define EVAL_QUAL_FOREIGN	2	/* evaluate some in foreign, rest in local */

#define OPTIMIZE_WHERE_CLAUSE	EVAL_QUAL_FOREIGN

/* deparse SQL from the request */
static bool foreign_qual_walker(Node *node, foreign_qual_context *context);

/*
 * Check whether the function is IMMUTABLE.
 */
bool
is_immutable_func(Oid funcid)
{
	HeapTuple		tp;
	bool			isnull;
	Datum			datum;

	tp = SearchSysCache(PROCOID, ObjectIdGetDatum(funcid), 0, 0, 0);
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for function %u", funcid);

#ifdef DEBUG_FDW
	/* print function name and its immutability */
	{
		char		   *proname;
		datum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_proname, &isnull);
		proname = pstrdup(DatumGetName(datum)->data);
		elog(DEBUG1, "func %s(%u) is%s immutable", proname, funcid,
			(DatumGetChar(datum) == PROVOLATILE_IMMUTABLE) ? "" : " not");
		pfree(proname);
	}
#endif

	datum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_provolatile, &isnull);
	ReleaseSysCache(tp);

	return (DatumGetChar(datum) == PROVOLATILE_IMMUTABLE);
}

/*
 * Check whether the ExprState node should be evaluated in foreign server.
 *
 * An expression which consists of expressions below will be evaluated in
 * the foreign server.
 *  - constant value
 *  - variable (foreign table column)
 *  - external parameter (parameter of prepared statement)
 *  - array
 *  - bool expression (AND/OR/NOT)
 *  - NULL test (IS [NOT] NULL)
 *  - operator
 *    - IMMUTABLE only
 *    - It is required that the meaning of the operator be the same as the
 *      local server in the foreign server. 
 *  - function
 *    - IMMUTABLE only
 *    - It is required that the meaning of the operator be the same as the
 *      local server in the foreign server. 
 *  - scalar array operator (ANY/ALL)
 */
bool
is_foreign_expr(Node *node, foreign_qual_context *context)
{
	return !foreign_qual_walker(node, context);
}

void
pgxc_foreign_qual_context_init(foreign_qual_context *context)
{
	context->collect_vars = false;
	context->vars = NIL;
	context->aggs = NIL;
}

void
pgxc_foreign_qual_context_free(foreign_qual_context *context)
{
	list_free(context->vars);
	context->vars = NIL;
	list_free(context->aggs);
	context->aggs = NIL;
}
/*
 * return true if node cannot be evaluatated in foreign server.
 */
static bool
foreign_qual_walker(Node *node, foreign_qual_context *context)
{
	bool	ret_val;
	bool	saved_collect_vars;
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_ExprState:
			return foreign_qual_walker((Node *) ((ExprState *) node)->expr, NULL);

		case T_Param:
			/* TODO: pass internal parameters to the foreign server */
			if (((Param *) node)->paramkind != PARAM_EXTERN)
				return true;
			break;
		case T_DistinctExpr:
		case T_OpExpr:
			/*
			 * An operator which uses IMMUTABLE function can be evaluated in
			 * foreign server . It is not necessary to worry about oprrest
			 * and oprjoin here because they are invoked by planner but not
			 * executor. DistinctExpr is a typedef of OpExpr.
			 */
			if (!is_immutable_func(((OpExpr*) node)->opfuncid))
				return true;
			break;
		case T_ScalarArrayOpExpr:
			if (!is_immutable_func(((ScalarArrayOpExpr*) node)->opfuncid))
				return true;
			break;
		case T_FuncExpr:
			/* IMMUTABLE function can be evaluated in foreign server */
			if (!is_immutable_func(((FuncExpr*) node)->funcid))
				return true;
			break;
		case T_Aggref:
			{
				Aggref *aggref = (Aggref *)node;
				/*
				 * An aggregate with ORDER BY, DISTINCT directives need to be
				 * computed at coordinator using all the rows. An aggregate
				 * without collection function needs to be computed at
				 * coordinator.
				 * PGXCTODO: polymorphic transition types need to be resolved to
				 * correctly interpret the transition results from data nodes.
				 * For now compute such aggregates at coordinator.
				 */
				if (aggref->aggorder ||
					aggref->aggdistinct ||
					aggref->agglevelsup ||
					!aggref->agghas_collectfn ||
					IsPolymorphicType(aggref->aggtrantype))
					return true;
				/*
				 * data node can compute transition results, so, add the
				 * aggregate to the context if context is present
				 */
				if (context)
				{
					/*
					 * Don't collect VARs under the Aggref node. See
					 * pgxc_process_grouping_targetlist() for details.
					 */
					saved_collect_vars = context->collect_vars;
					context->collect_vars = false;
					context->aggs = lappend(context->aggs, aggref);
				}
			}
			break;
		case T_Var:
			if (context && context->collect_vars)
				context->vars = lappend(context->vars, node);
			break;
		case T_PlaceHolderVar:
		case T_AppendRelInfo:
		case T_PlaceHolderInfo:
			/* TODO: research whether those complex nodes are evaluatable. */
			return true;
		default:
			break;
	}

	ret_val = expression_tree_walker(node, foreign_qual_walker, context);

	/*
	 * restore value of collect_vars in the context, since we have finished
	 * traversing tree rooted under and Aggref node
	 */
	if (context && IsA(node, Aggref))
		context->collect_vars = saved_collect_vars;

	return ret_val;
}

/*
 * Deparse SQL string from query request.
 *
 * The expressions in Plan.qual are deparsed when it satisfies is_foreign_expr()
 * and removed.
 */
char *
deparseSql(RemoteQueryState *scanstate)
{
	EState		   *estate = scanstate->ss.ps.state;
	bool			prefix;
	List		   *context;
	StringInfoData	sql;
	RemoteQuery	   *scan;
	RangeTblEntry  *rte;
	Oid				nspid;
	char		   *nspname;
	char		   *relname;
	const char	   *nspname_q;
	const char	   *relname_q;
	const char	   *aliasname_q;
	int				i;
	TupleDesc		tupdesc;
	bool			first;

	/* extract RemoteQuery and RangeTblEntry */
	scan = (RemoteQuery *)scanstate->ss.ps.plan;
	rte = list_nth(estate->es_range_table, scan->scan.scanrelid - 1);

	/* prepare to deparse plan */
	initStringInfo(&sql);
	context = deparse_context_for_plan((Node *)scan, NULL,
											estate->es_range_table);

	/*
	 * Scanning multiple relations in a RemoteQuery node is not supported.
	 */
	prefix = false;
#if 0
	prefix = list_length(estate->es_range_table) > 1;
#endif

	/* Get quoted names of schema, table and alias */
	nspid = get_rel_namespace(rte->relid);
	nspname = get_namespace_name(nspid);
	relname = get_rel_name(rte->relid);
	nspname_q = quote_identifier(nspname);
	relname_q = quote_identifier(relname);
	aliasname_q = quote_identifier(rte->eref->aliasname);

	/* deparse SELECT clause */
	appendStringInfo(&sql, "SELECT ");

	/*
	 * TODO: omit (deparse to "NULL") columns which are not used in the
	 * original SQL.
	 *
	 * We must parse nodes parents of this RemoteQuery node to determine unused
	 * columns because some columns may be used only in parent Sort/Agg/Limit
	 * nodes.
	 */
	tupdesc = scanstate->ss.ss_currentRelation->rd_att;
	first = true;
	for (i = 0; i < tupdesc->natts; i++)
	{
		/* skip dropped attributes */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		if (!first)
			appendStringInfoString(&sql, ", ");

		if (prefix)
			appendStringInfo(&sql, "%s.%s",
							aliasname_q, tupdesc->attrs[i]->attname.data);
		else
			appendStringInfo(&sql, "%s", tupdesc->attrs[i]->attname.data);
		first = false;
	}

	/* if target list is composed only of system attributes, add dummy column */
	if (first)
		appendStringInfo(&sql, "NULL");

	/* deparse FROM clause */
	appendStringInfo(&sql, " FROM ");
	/*
	 * XXX: should use GENERIC OPTIONS like 'foreign_relname' or something for
	 * the foreign table name instead of the local name ?
	 */
	appendStringInfo(&sql, "%s.%s %s", nspname_q, relname_q, aliasname_q);
	pfree(nspname);
	pfree(relname);
	if (nspname_q != nspname_q)
		pfree((char *) nspname_q);
	if (relname_q != relname_q)
		pfree((char *) relname_q);
	if (aliasname_q != rte->eref->aliasname)
		pfree((char *) aliasname_q);

	/*
	 * deparse WHERE cluase
	 *
	 * The expressions which satisfy is_foreign_expr() are deparsed into WHERE
	 * clause of result SQL string, and they could be removed from qual of
	 * PlanState to avoid duplicate evaluation at ExecScan().
	 *
	 * The Plan.qual is never changed, so multiple use of the Plan with
	 * PREPARE/EXECUTE work properly.
	 */
#if OPTIMIZE_WHERE_CLAUSE > EVAL_QUAL_LOCAL
	if (scanstate->ss.ps.plan->qual)
	{
		List	   *local_qual = NIL;
		List	   *foreign_qual = NIL;
		List	   *foreign_expr = NIL;
		ListCell   *lc;

		/*
		 * Divide qual of PlanState into two lists, one for local evaluation
		 * and one for foreign evaluation.
		 */
		foreach (lc, scanstate->ss.ps.qual)
		{
			ExprState	   *state = lfirst(lc);

			if (is_foreign_expr((Node *) state, NULL))
			{
				elog(DEBUG1, "foreign qual: %s", nodeToString(state->expr));
				foreign_qual = lappend(foreign_qual, state);
				foreign_expr = lappend(foreign_expr, state->expr);
			}
			else
			{
				elog(DEBUG1, "local qual: %s", nodeToString(state->expr));
				local_qual = lappend(local_qual, state);
			}
		}
#if OPTIMIZE_WHERE_CLAUSE == EVAL_QUAL_FOREIGN
		/*
		 * If the optimization level is EVAL_QUAL_FOREIGN, replace the original
		 * qual with the list of ExprStates which should be evaluated in the
		 * local server.
		 */
		scanstate->ss.ps.qual = local_qual;
#endif

		/*
		 * Deparse quals to be evaluated in the foreign server if any.
		 * TODO: modify deparse_expression() to deparse conditions which use
		 * internal parameters.
		 */
		if (foreign_expr != NIL)
		{
			Node   *node;
			node = (Node *) make_ands_explicit(foreign_expr);
			appendStringInfo(&sql, " WHERE ");
			appendStringInfo(&sql, "%s",
				deparse_expression(node, context, prefix, false));
			/*
			 * The contents of the list MUST NOT be free-ed because they are
			 * referenced from Plan.qual list.
			 */
			list_free(foreign_expr);
		}
	}
#endif

	elog(DEBUG1, "deparsed SQL is \"%s\"", sql.data);

	return sql.data;
}

