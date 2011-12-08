/*-------------------------------------------------------------------------
 *
 * nodemgr.h
 *  Routines for node management
 *
 *
 * Portions Copyright (c) 1996-2010  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * src/include/pgxc/nodemgr.h
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMGR_H
#define NODEMGR_H

#include "nodes/parsenodes.h"

/* Global number of nodes */
extern int  NumDataNodes;
extern int  NumCoords;
extern int  NumCoordSlaves;
extern int  NumDataNodeSlaves;

extern void PgxcNodeListAndCount(Oid **coOids, Oid **dnOids,
								 int *num_coords, int *num_dns);
extern void PgxcNodeAlter(AlterNodeStmt *stmt);
extern void PgxcNodeCreate(CreateNodeStmt *stmt);
extern void PgxcNodeRemove(DropNodeStmt *stmt);

#endif	/* NODEMGR_H */
