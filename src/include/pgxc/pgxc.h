/*-------------------------------------------------------------------------
 *
 * pgxc.h
 *		PG-XC
 *
 *
 * Portions Copyright (c) 1996-2010  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */
#ifdef PGXC

extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;

#define IS_PGXC_COORDINATOR isPGXCCoordinator
#define IS_PGXC_DATANODE isPGXCDataNode

#endif   /* PGXC */
