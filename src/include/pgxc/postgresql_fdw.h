/*-------------------------------------------------------------------------
 *
 * postgresql_fdw.h
 *
 *		foreign-data wrapper for PostgreSQL
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 *
 * src/include/pgxc/postgresql_fdw.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POSTGRES_FDW_H
#define POSTGRES_FDW_H

#include "postgres.h"
#include "pgxc/execRemote.h"

bool is_immutable_func(Oid funcid);
bool pgxc_is_expr_shippable(Expr *node, bool *has_aggs);
#endif
