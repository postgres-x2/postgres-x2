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
#include "optimizer/planmain.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define DEBUG_FDW

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
pgxc_is_expr_shippable(Expr *node, bool *has_aggs)
{
	Shippability_context sc_context;

	/* Create the FQS context */
	memset(&sc_context, 0, sizeof(sc_context));
	sc_context.sc_query = NULL;
	sc_context.sc_query_level = 0;
	sc_context.sc_for_expr = true;

	/* Walk the expression to check its shippability */
	pgxc_shippability_walker((Node *)node, &sc_context);

	/*
	 * If caller is interested in knowing, whether the expression has aggregets
	 * let the caller know about it. The caller is capable of handling such
	 * expressions. Otherwise assume such an expression as unshippable.
	 */
	if (has_aggs)
		*has_aggs = pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);
	else if (pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR))
		return false;
	/* Done with aggregate expression shippability. Delete the status */
	pgxc_reset_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);

	/* If there are reasons why the expression is unshippable, return false */
	if (!bms_is_empty(sc_context.sc_shippability))
		return false;

	/* If nothing wrong found, the expression is shippable */
	return true;
}
