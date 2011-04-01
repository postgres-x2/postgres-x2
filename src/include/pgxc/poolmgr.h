/*-------------------------------------------------------------------------
 *
 * poolmgr.h
 *
 *	  Definitions for the data nodes connection pool.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef POOLMGR_H
#define POOLMGR_H
#include <sys/time.h>
#include "pgxcnode.h"
#include "poolcomm.h"
#include "storage/pmsignal.h"

#define MAX_IDLE_TIME 60

/* TODO move? */
typedef struct
{
	char	   *host;
	char	   *port;
} PGXCNodeConnectionInfo;

/* Connection pool entry */
typedef struct
{
	struct timeval released;
	NODE_CONNECTION *conn;
} PGXCNodePoolSlot;

/* Pool of connections to specified pgxc node */
typedef struct
{
	char	   *connstr;
	int			freeSize;	/* available connections */
	int			size;  		/* total pool size */
	PGXCNodePoolSlot **slot;
} PGXCNodePool;

/* All pools for specified database */
typedef struct databasepool
{
	char	   *database;
	char	   *user_name;
	PGXCNodePool **dataNodePools;	/* one for each Datanode */
	PGXCNodePool **coordNodePools;	/* one for each Coordinator */
	struct databasepool *next;
} DatabasePool;

/* Agent of client session (Pool Manager side)
 * Acts as a session manager, grouping connections together
 */
typedef struct
{
	/* Process ID of postmaster child process associated to pool agent */
	int			pid;
	/* communication channel */
	PoolPort	port;
	DatabasePool *pool;
	PGXCNodePoolSlot **dn_connections; /* one for each Datanode */
	PGXCNodePoolSlot **coord_connections; /* one for each Coordinator */
} PoolAgent;

/* Handle to the pool manager (Session's side) */
typedef struct
{
	/* communication channel */
	PoolPort	port;
} PoolHandle;

extern int	NumDataNodes;
extern int	NumCoords;
extern int	MinPoolSize;
extern int	MaxPoolSize;
extern int	PoolerPort;

extern bool PersistentConnections;

extern char *DataNodeHosts;
extern char *DataNodePorts;

extern char *CoordinatorHosts;
extern char *CoordinatorPorts;

/* Initialize internal structures */
extern int	PoolManagerInit(void);

/* Destroy internal structures */
extern int	PoolManagerDestroy(void);

/*
 * Get handle to pool manager. This function should be called just before
 * forking off new session. It creates PoolHandle, PoolAgent and a pipe between
 * them. PoolAgent is stored within Postmaster's memory context and Session
 * closes it later. PoolHandle is returned and should be store in a local
 * variable. After forking off it can be stored in global memory, so it will
 * only be accessible by the process running the session.
 */
extern PoolHandle *GetPoolManagerHandle(void);

/*
 * Called from Postmaster(Coordinator) after fork. Close one end of the pipe and
 * free memory occupied by PoolHandler
 */
extern void PoolManagerCloseHandle(PoolHandle *handle);

/*
 * Gracefully close connection to the PoolManager
 */
extern void PoolManagerDisconnect(PoolHandle *handle);

/*
 * Called from Session process after fork(). Associate handle with session
 * for subsequent calls. Associate session with specified database and
 * initialize respective connection pool
 */
extern void PoolManagerConnect(PoolHandle *handle, const char *database, const char *user_name);

/* Get pooled connections */
extern int *PoolManagerGetConnections(List *datanodelist, List *coordlist);

/* Clean pool connections */
extern void PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname);

/* Send Abort signal to transactions being run */
extern int	PoolManagerAbortTransactions(char *dbname, int **proc_pids);

/* Return connections back to the pool, for both Coordinator and Datanode connections */
extern void PoolManagerReleaseConnections(int dn_ndisc, int* dn_discard, int co_ndisc, int* co_discard);

#endif
