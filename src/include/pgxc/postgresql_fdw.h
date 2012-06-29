/*-------------------------------------------------------------------------
 *
 * postgresql_fdw.h
 *
 *		foreign-data wrapper for PostgreSQL
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	$PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#ifndef POSTGRES_FDW_H
#define POSTGRES_FDW_H

#include "postgres.h"
#include "pgxc/execRemote.h"

bool is_immutable_func(Oid funcid);
char *deparseSql(RemoteQueryState *scanstate);
bool pgxc_is_expr_shippable(Expr *node, bool *has_aggs);
#endif
