/*-------------------------------------------------------------------------
 *
 * rewriteHandler.h
 *		External interface to query rewriter.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/rewrite/rewriteHandler.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REWRITEHANDLER_H
#define REWRITEHANDLER_H

#include "utils/relcache.h"
#include "nodes/parsenodes.h"

extern List *QueryRewrite(Query *parsetree);
extern void AcquireRewriteLocks(Query *parsetree, bool forUpdatePushedDown);

extern Node *build_column_default(Relation rel, int attrno);
extern int	relation_is_updatable(Oid reloid, bool include_triggers);
<<<<<<< HEAD

#ifdef PGXC
extern List *QueryRewriteCTAS(Query *parsetree);
#endif
=======
>>>>>>> e472b921406407794bab911c64655b8b82375196

#endif   /* REWRITEHANDLER_H */
