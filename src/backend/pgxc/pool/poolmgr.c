/*-------------------------------------------------------------------------
 *
 * poolmgr.c
 *
 *	  Connection pool manager handles connections to DataNodes
 *
 * The pooler runs as a separate process and is forked off from a
 * coordinator postmaster. If the coordinator needs a connection from a
 * data node, it asks for one from the pooler, which maintains separate
 * pools for each data node. A group of connections can be requested in
 * a single request, and the pooler returns a list of file descriptors
 * to use for the connections.
 *
 * Note the current implementation does not yet shrink the pool over time
 * as connections are idle.  Also, it does not queue requests; if a
 * connection is unavailable, it will simply fail. This should be implemented
 * one day, although there is a chance for deadlocks. For now, limiting
 * connections should be done between the application and coordinator.
 * Still, this is useful to avoid having to re-establish connections to the
 * data nodes all the time for multiple coordinator backend sessions.
 *
 * The term "agent" here refers to a session manager, one for each backend
 * coordinator connection to the pooler. It will contain a list of connections
 * allocated to a session, at most one per data node.
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

#include "postgres.h"

#include <signal.h>
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "../interfaces/libpq/libpq-int.h"
#include "postmaster/postmaster.h"		/* For UnixSocketDir */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>

/* Configuration options */
int			MinPoolSize = 1;
int			MaxPoolSize = 100;
int			PoolerPort = 6667;

bool		PersistentConnections = false;

/* Flag to tell if we are Postgres-XC pooler process */
static bool am_pgxc_pooler = false;

/* Connection information cached */
typedef struct
{
	Oid		nodeoid;
    char   *host;
    int		port;
} PGXCNodeConnectionInfo;

/* The memory context */
static MemoryContext PoolerMemoryContext = NULL;

/* PGXC Nodes info list */
static PGXCNodeConnectionInfo *datanode_connInfos;
static PGXCNodeConnectionInfo *coord_connInfos;

/* Pool to all the databases (linked list) */
static DatabasePool *databasePools = NULL;

/* PoolAgents */
static int	agentCount = 0;
static PoolAgent **poolAgents;

static PoolHandle *poolHandle = NULL;

static int	is_pool_locked = false;
static int	server_fd = -1;

static void node_info_free(void);
static void node_info_load(void);
static int	node_info_check(void);
static void agent_init(PoolAgent *agent, const char *database, const char *user_name);
static void agent_destroy(PoolAgent *agent);
static void agent_create(void);
static void agent_handle_input(PoolAgent *agent, StringInfo s);
static int agent_session_command(PoolAgent *agent,
								 const char *set_command,
								 PoolCommandType command_type);
static int agent_set_command(PoolAgent *agent,
							 const char *set_command,
							 PoolCommandType command_type);
static int agent_temp_command(PoolAgent *agent);
static DatabasePool *create_database_pool(const char *database, const char *user_name);
static void insert_database_pool(DatabasePool *pool);
static int	destroy_database_pool(const char *database, const char *user_name);
static void reload_database_pools(void);
static DatabasePool *find_database_pool(const char *database, const char *user_name);
static DatabasePool *find_database_pool_to_clean(const char *database,
												 const char *user_name,
												 List *dn_list,
												 List *co_list);
static DatabasePool *remove_database_pool(const char *database, const char *user_name);
static int *agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist);
static int cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist);
static PGXCNodePoolSlot *acquire_connection(DatabasePool *dbPool, int node, char client_conn_type);
static void agent_release_connections(PoolAgent *agent, bool force_destroy);
static void agent_reset_session(PoolAgent *agent);
static void release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot, int index,
							   bool force_destroy, char client_conn_type);
static void destroy_slot(PGXCNodePoolSlot *slot);
static void grow_pool(DatabasePool *dbPool, int index, char client_conn_type);
static void destroy_node_pool(PGXCNodePool *node_pool);
static void PoolerLoop(void);
static int clean_connection(List *dn_discard,
							List *co_discard,
							const char *database,
							const char *user_name);
static int *abort_pids(int *count,
					   int pid,
					   const char *database,
					   const char *user_name);

/* Signal handlers */
static void pooler_die(SIGNAL_ARGS);
static void pooler_quickdie(SIGNAL_ARGS);

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t shutdown_requested = false;

void
PGXCPoolerProcessIam(void)
{
	am_pgxc_pooler = true;
}

bool
IsPGXCPoolerProcess(void)
{
    return am_pgxc_pooler;
}

/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
	elog(DEBUG1, "Pooler process is started: %d", getpid());

	/*
	 * Set up memory context for the pooler
	 */
	PoolerMemoryContext = AllocSetContextCreate(TopMemoryContext,
												"PoolerMemoryContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(pool manager probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif
	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGINT, pooler_die);
	pqsignal(SIGTERM, pooler_die);
	pqsignal(SIGQUIT, pooler_quickdie);
	pqsignal(SIGHUP, SIG_IGN);
	/* TODO other signal handlers */

	/* We allow SIGQUIT (quickdie) at all times */
#ifdef HAVE_SIGPROCMASK
	sigdelset(&BlockSig, SIGQUIT);
#else
	BlockSig &= ~(sigmask(SIGQUIT));
#endif

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/* Allocate pooler structures in the Pooler context */
	MemoryContextSwitchTo(PoolerMemoryContext);

	poolAgents = (PoolAgent **) palloc(MaxConnections * sizeof(PoolAgent *));
	if (poolAgents == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* Initialize process ressources */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForPoolerInfo");

	/* Initialize pooler in Postgres-way */
	InitPostgres(NULL, InvalidOid, NULL, NULL);

	/* Initialize pooler connection info */
	node_info_load();

	PoolerLoop();
	return 0;
}

/*
 * Free connection info cached
 */
static void
node_info_free(void)
{
	int count;

	for (count = 0; count < NumCoords; count++)
		pfree(coord_connInfos[count].host);
	for (count = 0; count < NumDataNodes; count++)
		pfree(datanode_connInfos[count].host);

	if (datanode_connInfos)
		pfree(datanode_connInfos);
	if (coord_connInfos)
		pfree(coord_connInfos);

	NumCoords = 0;
	NumDataNodes = 0;
	coord_connInfos = NULL;
	datanode_connInfos = NULL;
}

/*
 * Load node info cached by scanning PGXC node catalog
 */
static void
node_info_load(void)
{
	int	count;
	Oid *coOids = NULL;
	Oid *dnOids = NULL;

	/* Update number of PGXC nodes saved in cache */
	PgxcNodeListAndCount(&coOids, &dnOids, &NumCoords, &NumDataNodes);

	/* Then initialize the node informations */
	if (NumDataNodes != 0)
		datanode_connInfos = (PGXCNodeConnectionInfo *)
			palloc(NumDataNodes * sizeof(PGXCNodeConnectionInfo));
	if (NumCoords != 0)
		coord_connInfos = (PGXCNodeConnectionInfo *)
			palloc(NumCoords * sizeof(PGXCNodeConnectionInfo));

	/* Fill in connection info structures */
	for (count = 0; count < NumCoords; count++)
	{
		coord_connInfos[count].nodeoid = coOids[count];
		coord_connInfos[count].port = get_pgxc_nodeport(coOids[count]);
		coord_connInfos[count].host = get_pgxc_nodehost(coOids[count]);
	}
	for (count = 0; count < NumDataNodes; count++)
	{
		datanode_connInfos[count].nodeoid = dnOids[count];
		datanode_connInfos[count].port = get_pgxc_nodeport(dnOids[count]);
		datanode_connInfos[count].host = get_pgxc_nodehost(dnOids[count]);
	}

	/* Clean up resources */
	if (coOids)
		pfree(coOids);
	if (dnOids)
		pfree(dnOids);
}

/*
 * Check connection info consistency with system catalogs
 */
static int
node_info_check(void)
{
	int res = POOL_CHECK_SUCCESS;
	int	num_coord, num_dn, i, j;
	Oid *coOids = NULL;
	Oid *dnOids = NULL;

	/* Update number of PGXC nodes saved in cache */
	PgxcNodeListAndCount(&coOids, &dnOids,
						 &num_coord, &num_dn);

	/* Check first if node numbers are consistent */
	if (NumCoords != num_coord ||
		NumDataNodes != num_dn)
	{
		res = POOL_CHECK_FAILED;
		goto finish;
	}

	/* Now do a check element by element */
	for (i = 0; i < 2; i++)
	{
		int numnodes;
		PGXCNodeConnectionInfo *conninfo;
		Oid *oid_vector;

		/* Take all the elements necessary for check */
		switch(i)
		{
			case 0:
				numnodes = NumCoords;
				oid_vector = coOids;
				conninfo = coord_connInfos;
				break;
			case 1:
				numnodes = NumDataNodes;
				oid_vector = dnOids;
				conninfo = datanode_connInfos;
				break;
			default:
				Assert(0);
		}

		/* Then check data consistency for port, host and node Oid */
		for (j = 0; j < numnodes; j++)
		{
			if (conninfo[j].nodeoid != oid_vector[j] ||
				conninfo[j].port != get_pgxc_nodeport(oid_vector[j]) ||
				strcmp(conninfo[j].host, get_pgxc_nodehost(oid_vector[j])))
			{
				res = POOL_CHECK_FAILED;
				goto finish;
			}
		}
	}

finish:
	/* Clean everything */
	if (coOids)
		pfree(coOids);
	if (dnOids)
		pfree(dnOids);
	return res;
}

/*
 * Destroy internal structures
 */
int
PoolManagerDestroy(void)
{
	int			status = 0;

	if (PoolerMemoryContext)
	{
		MemoryContextDelete(PoolerMemoryContext);
		PoolerMemoryContext = NULL;
	}

	return status;
}


/*
 * Get handle to pool manager
 * Invoked from Postmaster's main loop just before forking off new session
 * Returned PoolHandle structure will be inherited by session process
 */
PoolHandle *
GetPoolManagerHandle(void)
{
	PoolHandle *handle;
	int			fdsock;

	/* Connect to the pooler */
	fdsock = pool_connect(PoolerPort, UnixSocketDir);
	if (fdsock < 0)
	{
		int			saved_errno = errno;

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to connect to pool manager: %m")));
		errno = saved_errno;
		return NULL;
	}

	/* Allocate handle */
	/*
	 * XXX we may change malloc here to palloc but first ensure
	 * the CurrentMemoryContext is properly set.
	 * The handle allocated just before new session is forked off and
	 * inherited by the session process. It should remain valid for all
	 * the session lifetime.
	 */
	handle = (PoolHandle *) malloc(sizeof(PoolHandle));
	if (!handle)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return NULL;
	}

	handle->port.fdsock = fdsock;
	handle->port.RecvLength = 0;
	handle->port.RecvPointer = 0;
	handle->port.SendPointer = 0;

	return handle;
}


/*
 * Close handle
 */
void
PoolManagerCloseHandle(PoolHandle *handle)
{
	close(Socket(handle->port));
	free(handle);
	handle = NULL;
}


/*
 * Create agent
 */
static void
agent_create(void)
{
	int			new_fd;
	PoolAgent  *agent;

	new_fd = accept(server_fd, NULL, NULL);
	if (new_fd < 0)
	{
		int			saved_errno = errno;

		ereport(LOG,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("pool manager failed to accept connection: %m")));
		errno = saved_errno;
		return;
	}

	/* Allocate agent */
	agent = (PoolAgent *) palloc(sizeof(PoolAgent));
	if (!agent)
	{
		close(new_fd);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return;
	}

	agent->port.fdsock = new_fd;
	agent->port.RecvLength = 0;
	agent->port.RecvPointer = 0;
	agent->port.SendPointer = 0;
	agent->pool = NULL;
	agent->num_dn_connections = NumDataNodes;
	agent->num_coord_connections = NumCoords;
	agent->dn_connections = NULL;
	agent->coord_connections = NULL;
	agent->session_params = NULL;
	agent->local_params = NULL;
	agent->is_temp = false;
	agent->pid = 0;

	/* Append new agent to the list */
	poolAgents[agentCount++] = agent;
}


/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
void
PoolManagerConnect(PoolHandle *handle, const char *database, const char *user_name)
{
	int n32;
	char msgtype = 'c';

	Assert(handle);
	Assert(database);
	Assert(user_name);

	/* Save the handle */
	poolHandle = handle;

	/* Message type */
	pool_putbytes(&handle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(strlen(database) + strlen(user_name) + 18);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* PID number */
	n32 = htonl(MyProcPid);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = htonl(strlen(database) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send database name followed by \0 terminator */
	pool_putbytes(&handle->port, database, strlen(database) + 1);
	pool_flush(&handle->port);

	/* Length of user name string */
	n32 = htonl(strlen(user_name) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send user name followed by \0 terminator */
	pool_putbytes(&handle->port, user_name, strlen(user_name) + 1);
	pool_flush(&handle->port);
}

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void
PoolManagerReconnect(void)
{
	PoolHandle *handle;

	Assert(poolHandle);

	PoolManagerDisconnect();
	handle = GetPoolManagerHandle();
	PoolManagerConnect(handle,
					   get_database_name(MyDatabaseId),
					   GetUserNameFromId(GetUserId()));
}

int
PoolManagerSetCommand(PoolCommandType command_type, const char *set_command)
{
	int n32, res;
	char msgtype = 's';

	Assert(poolHandle);

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	if (set_command)
		n32 = htonl(strlen(set_command) + 13);
	else
		n32 = htonl(12);

	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* LOCAL or SESSION parameter ? */
	n32 = htonl(command_type);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	if (set_command)
	{
		/* Length of SET command string */
		n32 = htonl(strlen(set_command) + 1);
		pool_putbytes(&poolHandle->port, (char *) &n32, 4);

		/* Send command string followed by \0 terminator */
		pool_putbytes(&poolHandle->port, set_command, strlen(set_command) + 1);
	}
	else
	{
		/* Send empty command */
		n32 = htonl(0);
		pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	}

	pool_flush(&poolHandle->port);

	/* Get result */
	res = pool_recvres(&poolHandle->port);

	return res;
}

/*
 * Lock/unlock pool manager
 * During locking, the only operations not permitted are abort, connection and
 * connection obtention.
 */
void
PoolManagerLock(bool is_lock)
{
	char msgtype = 'o';
	int n32;
	int msglen = 8;
	Assert(poolHandle);

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Lock information */
	n32 = htonl((int) is_lock);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	pool_flush(&poolHandle->port);
}

/*
 * Init PoolAgent
 */
static void
agent_init(PoolAgent *agent, const char *database, const char *user_name)
{
	Assert(agent);
	Assert(database);
	Assert(user_name);

	/* disconnect if we are still connected */
	if (agent->pool)
		agent_release_connections(agent, false);

	/* find database */
	agent->pool = find_database_pool(database, user_name);

	/* create if not found */
	if (agent->pool == NULL)
		agent->pool = create_database_pool(database, user_name);

	return;
}

/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{
	int	i;

	Assert(agent);

	close(Socket(agent->port));

	/* Discard connections if any remaining */
	if (agent->pool)
	{
		/*
		 * Agent is being destroyed, so reset session parameters
		 * before putting back connections to pool.
		 */
		agent_reset_session(agent);

		/*
		 * Release them all.
		 * Force disconnection if there are temporary objects on agent.
		 */
		agent_release_connections(agent, agent->is_temp);
	}

	/* find agent in the list */
	for (i = 0; i < agentCount; i++)
	{
		if (poolAgents[i] == agent)
		{
			/* Free memory. All connection slots are NULL at this point */
			if (agent->dn_connections)
			{
				pfree(agent->dn_connections);
				agent->dn_connections = NULL;
			}
			if (agent->coord_connections)
			{
				pfree(agent->coord_connections);
				agent->coord_connections = NULL;
			}
			if (agent->local_params)
			{
				pfree(agent->local_params);
				agent->local_params = NULL;
			}
			if (agent->session_params)
			{
				pfree(agent->session_params);
				agent->session_params = NULL;
			}
			pfree(agent);
			/* shrink the list and move last agent into the freed slot */
			if (i < --agentCount)
				poolAgents[i] = poolAgents[agentCount];
			/* only one match is expected so exit */
			break;
		}
	}
}


/*
 * Release handle to pool manager
 */
void
PoolManagerDisconnect(void)
{
	Assert(poolHandle);

	pool_putmessage(&poolHandle->port, 'd', NULL, 0);
	pool_flush(&poolHandle->port);

	close(Socket(poolHandle->port));
	poolHandle = NULL;
}


/*
 * Get pooled connections
 */
int *
PoolManagerGetConnections(List *datanodelist, List *coordlist)
{
	int			i;
	ListCell   *nodelist_item;
	int		   *fds;
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];

	Assert(poolHandle);

	/*
	 * Prepare end send message to pool manager.
	 * First with Datanode list.
	 * This list can be NULL for a query that does not need
	 * Datanode Connections (Sequence DDLs)
	 */
	nodes[0] = htonl(list_length(datanodelist));
	i = 1;
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}
	/* Then with Coordinator list (can be nul) */
	nodes[i++] = htonl(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}

	pool_putmessage(&poolHandle->port, 'g', (char *) nodes, sizeof(int) * (totlen + 2));
	pool_flush(&poolHandle->port);

	/* Receive response */
	fds = (int *) palloc(sizeof(int) * totlen);
	if (fds == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}
	if (pool_recvfds(&poolHandle->port, fds, totlen))
	{
		pfree(fds);
		return NULL;
	}

	return fds;
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int
PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids)
{
	int		num_proc_ids = 0;
	int		n32, msglen;
	char		msgtype = 'a';
	int		dblen = dbname ? strlen(dbname) + 1 : 0;
	int		userlen = username ? strlen(username) + 1 : 0;

	Assert(poolHandle);

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = dblen + userlen + 12;
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = htonl(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = htonl(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Then Get back Pids from Pooler */
	num_proc_ids = pool_recvpids(&poolHandle->port, proc_pids);

	return num_proc_ids;
}


/*
 * Clean up Pooled connections
 */
void
PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username)
{
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];
	ListCell		*nodelist_item;
	int			i, n32, msglen;
	char			msgtype = 'f';
	int			userlen = username ? strlen(username) + 1 : 0;
	int			dblen = dbname ? strlen(dbname) + 1 : 0;

	nodes[0] = htonl(list_length(datanodelist));
	i = 1;
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}
	/* Then with Coordinator list (can be nul) */
	nodes[i++] = htonl(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = sizeof(int) * (totlen + 2) + dblen + userlen + 12;
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send list of nodes */
	pool_putbytes(&poolHandle->port, (char *) nodes, sizeof(int) * (totlen + 2));

	/* Length of Database string */
	n32 = htonl(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = htonl(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Receive result message */
	if (pool_recvres(&poolHandle->port) != CLEAN_CONNECTION_COMPLETED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Clean connections not completed")));
}


/*
 * Check connection information consistency cached in pooler with catalog information
 */
bool
PoolManagerCheckConnectionInfo(void)
{
	int res;

	Assert(poolHandle);
	pool_putmessage(&poolHandle->port, 'q', NULL, 0);
	pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port);

	if (res == POOL_CHECK_SUCCESS)
		return true;

	return false;
}


/*
 * Reload connection data in pooler and drop all the existing connections of pooler
 */
void
PoolManagerReloadConnectionInfo(void)
{
	Assert(poolHandle);
	pool_putmessage(&poolHandle->port, 'p', NULL, 0);
	pool_flush(&poolHandle->port);
}


/*
 * Handle messages to agent
 */
static void
agent_handle_input(PoolAgent * agent, StringInfo s)
{
	int			qtype;

	qtype = pool_getbyte(&agent->port);
	/*
	 * We can have multiple messages, so handle them all
	 */
	for (;;)
	{
		const char	*database = NULL;
		const char	*user_name = NULL;
		const char	*set_command = NULL;
		PoolCommandType	command_type;
		int		datanodecount;
		int		coordcount;
		List		*datanodelist = NIL;
		List		*coordlist = NIL;
		int		*fds;
		int		*pids;
		int		i, len, res;

		/*
		 * During a pool cleaning, Abort, Connect and Get Connections messages
		 * are not allowed on pooler side.
		 * It avoids to have new backends taking connections
		 * while remaining transactions are aborted during FORCE and then
		 * Pools are being shrinked.
		 */
		if (is_pool_locked && (qtype == 'a' || qtype == 'c' || qtype == 'g'))
			elog(WARNING,"Pool operation cannot run during pool lock");

		switch (qtype)
		{
			case 'a':			/* ABORT */
				pool_getmessage(&agent->port, s, 0);
				len = pq_getmsgint(s, 4);
				if (len > 0)
					database = pq_getmsgbytes(s, len);

				len = pq_getmsgint(s, 4);
				if (len > 0)
					user_name = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				pids = abort_pids(&len, agent->pid, database, user_name);

				pool_sendpids(&agent->port, pids, len);
				if (pids)
					pfree(pids);
				break;
			case 'c':			/* CONNECT */
				pool_getmessage(&agent->port, s, 0);
				agent->pid = pq_getmsgint(s, 4);
				len = pq_getmsgint(s, 4);
				database = pq_getmsgbytes(s, len);
				len = pq_getmsgint(s, 4);
				user_name = pq_getmsgbytes(s, len);
				/*
				 * Coordinator pool is not initialized.
				 * With that it would be impossible to create a Database by default.
				 */
				agent_init(agent, database, user_name);
				pq_getmsgend(s);
				break;
			case 'd':			/* DISCONNECT */
				pool_getmessage(&agent->port, s, 4);
				agent_destroy(agent);
				pq_getmsgend(s);
				break;
			case 'f':			/* CLEAN CONNECTION */
				pool_getmessage(&agent->port, s, 0);
				datanodecount = pq_getmsgint(s, 4);
				/* It is possible to clean up only Coordinators connections */
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible to clean up only Datanode connections */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				len = pq_getmsgint(s, 4);
				if (len > 0)
					database = pq_getmsgbytes(s, len);
				len = pq_getmsgint(s, 4);
				if (len > 0)
					user_name = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				/* Clean up connections here */
				res = clean_connection(datanodelist, coordlist, database, user_name);

				list_free(datanodelist);
				list_free(coordlist);

				/* Send success result */
				pool_sendres(&agent->port, res);
				break;
			case 'g':			/* GET CONNECTIONS */
				/*
				 * Length of message is caused by:
				 * - Message header = 4bytes
				 * - List of datanodes = NumDataNodes * 4bytes (max)
				 * - List of coordinators = NumCoords * 4bytes (max)
				 * - Number of Datanodes sent = 4bytes
				 * - Number of Coordinators sent = 4bytes
				 * It is better to send in a same message the list of Co and Dn at the same
				 * time, this permits to reduce interactions between postmaster and pooler
				 */
				pool_getmessage(&agent->port, s, 4 * NumDataNodes + 4 * NumCoords + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);

				/*
				 * In case of error agent_acquire_connections will log
				 * the error and return NULL
				 */
				fds = agent_acquire_connections(agent, datanodelist, coordlist);
				list_free(datanodelist);
				list_free(coordlist);

				pool_sendfds(&agent->port, fds, fds ? datanodecount + coordcount : 0);
				if (fds)
					pfree(fds);
				break;

			case 'h':			/* Cancel SQL Command in progress on specified connections */
				/*
				 * Length of message is caused by:
				 * - Message header = 4bytes
				 * - List of datanodes = NumDataNodes * 4bytes (max)
				 * - List of coordinators = NumCoords * 4bytes (max)
				 * - Number of Datanodes sent = 4bytes
				 * - Number of Coordinators sent = 4bytes
				 */
				pool_getmessage(&agent->port, s, 4 * NumDataNodes + 4 * NumCoords + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);

				cancel_query_on_connections(agent, datanodelist, coordlist);
				list_free(datanodelist);
				list_free(coordlist);
				break;
			case 'o':			/* Lock/unlock pooler */
				pool_getmessage(&agent->port, s, 8);
				is_pool_locked = pq_getmsgint(s, 4);
				pq_getmsgend(s);
				break;
			case 'p':			/* Reload connection info */
				/*
				 * Connection information reloaded concerns all the database pools.
				 * A database pool is reloaded as follows for each remote node:
				 * - node pool is deleted if the node has been deleted from catalog.
				 *   Subsequently all its connections are dropped.
				 * - node pool is deleted if its port or host information is changed.
				 *   Subsequently all its connections are dropped.
				 * - node pool is kept unchanged with existing connection information
				 *   is not changed. However its index position in node pool is changed
				 *   according to the alphabetical order of the node name in new
				 *   cluster configuration.
				 * Backend sessions are responsible to reconnect to the pooler to update
				 * their agent with newest connection information.
				 * The session invocating connection information reload is reconnected
				 * and uploaded automatically after database pool reload.
				 * Other server sessions are signaled to reconnect to pooler and update
				 * their connection information separately.
				 * During reload process done internally on pooler, pooler is locked
				 * to forbid new connection requests.
				 */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* Free all the node info before reloading */
				node_info_free();

				/* Reload node information */
				node_info_load();

				/* First update all the pools */
				reload_database_pools();
				break;
			case 'q':			/* Check connection info consistency */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* Check cached info consistency */
				res = node_info_check();

				/* Send result */
				pool_sendres(&agent->port, res);
				break;
			case 'r':			/* RELEASE CONNECTIONS */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);
				agent_release_connections(agent, false);
				break;
			case 's':			/* Session-related COMMAND */
				pool_getmessage(&agent->port, s, 0);
				/* Determine if command is local or session */
				command_type = (PoolCommandType) pq_getmsgint(s, 4);
				/* Get the SET command if necessary */
				len = pq_getmsgint(s, 4);
				if (len != 0)
					set_command = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				/* Manage command depending on its type */
				res = agent_session_command(agent, set_command, command_type);

				/* Send success result */
				pool_sendres(&agent->port, res);
				break;
			default:			/* EOF or protocol violation */
				agent_destroy(agent);
				return;
		}
		/* avoid reading from connection */
		if ((qtype = pool_pollbyte(&agent->port)) == EOF)
			break;
	}
}

/*
 * Manage a session command for pooler
 */
static int
agent_session_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type)
{
	int res;

	switch (command_type)
	{
		case POOL_CMD_LOCAL_SET:
		case POOL_CMD_GLOBAL_SET:
			res = agent_set_command(agent, set_command, command_type);
			break;
		case POOL_CMD_TEMP:
			res = agent_temp_command(agent);
			break;
		default:
			res = -1;
			break;
	}

	return res;
}

/*
 * Set agent flag that a temporary object is in use.
 */
static int
agent_temp_command(PoolAgent *agent)
{
	agent->is_temp = true;
	return 0;
}

/*
 * Save a SET command and distribute it to the agent connections
 * already in use.
 */
static int
agent_set_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type)
{
	char   *params_string;
	int		i;
	int		res = 0;

	Assert(agent);
	Assert(set_command);
	Assert(command_type == POOL_CMD_LOCAL_SET || command_type == POOL_CMD_GLOBAL_SET);

	if (command_type == POOL_CMD_LOCAL_SET)
		params_string = agent->local_params;
	else if (command_type == POOL_CMD_GLOBAL_SET)
		params_string = agent->session_params;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Set command process failed")));

	/* First command recorded */
	if (!params_string)
	{
		params_string = pstrdup(set_command);
		if (!params_string)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}
	else
	{
		/*
		 * Second command or more recorded.
		 * Commands are saved with format 'SET param1 TO value1;...;SET paramN TO valueN'
		 */
		params_string = (char *) repalloc(params_string,
										  strlen(params_string) + strlen(set_command) + 2);
		if (!params_string)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));

		sprintf(params_string, "%s;%s", params_string, set_command);
	}

	/* Launch the new command to all the connections already hold by the agent */
	if (agent->dn_connections)
	{
		for (i = 0; i < agent->num_dn_connections; i++)
		{
			if (agent->dn_connections[i])
				res = PGXCNodeSendSetQuery(agent->dn_connections[i]->conn, set_command);
		}
	}

	if (agent->coord_connections)
	{
		for (i = 0; i < agent->num_coord_connections; i++)
		{
			if (agent->coord_connections[i])
				res |= PGXCNodeSendSetQuery(agent->coord_connections[i]->conn, set_command);
		}
	}

	/* Save the latest string */
	if (command_type == POOL_CMD_LOCAL_SET)
		agent->local_params = params_string;
	else if (command_type == POOL_CMD_GLOBAL_SET)
		agent->session_params = params_string;

	return res;
}

/*
 * acquire connection
 */
static int *
agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	int			i;
	int		   *result;
	ListCell   *nodelist_item;

	Assert(agent);

	/* Check if pooler can accept those requests */
	if (list_length(datanodelist) > NumDataNodes ||
		list_length(coordlist) > NumCoords)
		return NULL;

	/*
	 * Allocate memory
	 * File descriptors of Datanodes and Coordinators are saved in the same array,
	 * This array will be sent back to the postmaster.
	 * It has a length equal to the length of the datanode list
	 * plus the length of the coordinator list.
	 * Datanode fds are saved first, then Coordinator fds are saved.
	 */
	result = (int *) palloc((list_length(datanodelist) + list_length(coordlist)) * sizeof(int));
	if (result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/*
	 * Initialize connection if it is not initialized yet
	 * First for the Datanodes
	 */
	if (!agent->dn_connections)
	{
		agent->dn_connections = (PGXCNodePoolSlot **)
			palloc(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));
		if (!agent->dn_connections)
		{
			pfree(result);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			return NULL;
		}

		for (i = 0; i < agent->num_dn_connections; i++)
			agent->dn_connections[i] = NULL;
	}

	/* Then for the Coordinators */
	if (!agent->coord_connections)
	{
		agent->coord_connections = (PGXCNodePoolSlot **)
			palloc(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
		if (!agent->coord_connections)
		{
			pfree(result);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			return NULL;
		}

		for (i = 0; i < agent->num_coord_connections; i++)
			agent->coord_connections[i] = NULL;
	}


	/* Initialize result */
	i = 0;
	/* Save in array fds of Datanodes first */
	foreach(nodelist_item, datanodelist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->dn_connections[node] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool, node, REMOTE_CONN_DATANODE);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				return NULL;
			}

			/* Store in the descriptor */
			agent->dn_connections[node] = slot;

			/* Update newly-acquired slot with session parameters */
			if (agent->session_params)
				PGXCNodeSendSetQuery(slot->conn, agent->session_params);
			if (agent->local_params)
				PGXCNodeSendSetQuery(slot->conn, agent->local_params);
		}

		result[i++] = PQsocket((PGconn *) agent->dn_connections[node]->conn);
	}

	/* Save then in the array fds for Coordinators */
	foreach(nodelist_item, coordlist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->coord_connections[node] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool, node, REMOTE_CONN_COORD);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				return NULL;
			}

			/* Store in the descriptor */
			agent->coord_connections[node] = slot;

			/* Update newly-acquired slot with session parameters */
			if (agent->session_params)
				PGXCNodeSendSetQuery(slot->conn, agent->session_params);
			if (agent->local_params)
				PGXCNodeSendSetQuery(slot->conn, agent->local_params);
		}

		result[i++] = PQsocket((PGconn *) agent->coord_connections[node]->conn);
	}

	return result;
}

/*
 * Cancel query
 */
static int 
cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	ListCell	*nodelist_item;
	char		errbuf[256];
	int		nCount;
	bool		bRet;

	nCount = 0;

	if (agent == NULL)
		return nCount;

	/* Send cancel on Data nodes first */
	foreach(nodelist_item, datanodelist)
	{
		int	node = lfirst_int(nodelist_item);

		if(node < 0 || node >= NumDataNodes)
			continue;

		if (agent->dn_connections == NULL)
			break;

		bRet = PQcancel((PGcancel *) agent->dn_connections[node]->xc_cancelConn, errbuf, sizeof(errbuf));
		if (bRet != false)
		{
			nCount++;
		}
	}

	/* Send cancel to Coordinators too, e.g. if DDL was in progress */
	foreach(nodelist_item, coordlist)
	{
		int	node = lfirst_int(nodelist_item);

		if(node < 0 || node >= NumDataNodes)
			continue;

		if (agent->coord_connections == NULL)
			break;

		bRet = PQcancel((PGcancel *) agent->coord_connections[node]->xc_cancelConn, errbuf, sizeof(errbuf));
		if (bRet != false)
		{
			nCount++;
		}
	}

	return nCount;
}

/*
 * Return connections back to the pool
 */
void
PoolManagerReleaseConnections(void)
{
	Assert(poolHandle);
	pool_putmessage(&poolHandle->port, 'r', NULL, 0);
	pool_flush(&poolHandle->port);
}

/*
 * Cancel Query
 */
void
PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list)
{
	uint32		n32;
	/*
	 * Buffer contains the list of both Coordinator and Datanodes, as well
	 * as the number of connections
	 */
	uint32 		buf[2 + dn_count + co_count];
	int 		i;

	if (poolHandle == NULL || dn_list == NULL || co_list == NULL)
		return;

	if (dn_count == 0 && co_count == 0)
		return;

	/* Insert the list of Datanodes in buffer */
	n32 = htonl((uint32) dn_count);
	buf[0] = n32;

	for (i = 0; i < dn_count;)
	{
		n32 = htonl((uint32) dn_list[i++]);
		buf[i] = n32;
	}

	/* Insert the list of Coordinators in buffer */
	n32 = htonl((uint32) co_count);
	buf[dn_count + 1] = n32;

	/* Not necessary to send to pooler a request if there is no Coordinator */
	if (co_count != 0)
	{
		for (i = dn_count + 1; i < (dn_count + co_count + 1);)
		{
			n32 = htonl((uint32) co_list[i - (dn_count + 1)]);
			buf[++i] = n32;
		}
	}
	pool_putmessage(&poolHandle->port, 'h', (char *) buf, (2 + dn_count + co_count) * sizeof(uint32));
	pool_flush(&poolHandle->port);
}

/*
 * Release connections for Datanodes and Coordinators
 */
static void
agent_release_connections(PoolAgent *agent, bool force_destroy)
{
	int			i;

	if (!agent->dn_connections && !agent->coord_connections)
		return;

	/*
	 * If there are some session parameters or temporary objects,
	 * do not put back connections to pool.
	 * Disconnection will be made when session is cut for this user.
	 * Local parameters are reset when transaction block is finished,
	 * so don't do anything for them, but just reset their list.
	 */
	if (agent->local_params)
	{
		pfree(agent->local_params);
		agent->local_params = NULL;
	}
	if (agent->session_params ||
		(agent->is_temp && !force_destroy))
		return;

	/*
	 * Remaining connections are assumed to be clean.
	 * First clean up for Datanodes
	 */
	for (i = 0; i < agent->num_dn_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->dn_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
			release_connection(agent->pool, slot, i, force_destroy, REMOTE_CONN_DATANODE);
		agent->dn_connections[i] = NULL;
	}
	/* Then clean up for Coordinator connections */
	for (i = 0; i < agent->num_coord_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->coord_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
			release_connection(agent->pool, slot, i, force_destroy, REMOTE_CONN_COORD);
		agent->coord_connections[i] = NULL;
	}
}

/*
 * Reset session parameters for given connections in the agent.
 * This is done before putting back to pool connections that have been
 * modified by session parameters.
 */
static void
agent_reset_session(PoolAgent *agent)
{
	if (!agent->dn_connections && !agent->coord_connections)
		return;

	if (!agent->session_params && !agent->local_params)
		return;

	/* Reset connection params */
	if (agent->session_params || agent->local_params)
	{
		int			i;

		/* Check agent slot for each Datanode */
		if (agent->dn_connections)
		{
			for (i = 0; i < agent->num_dn_connections; i++)
			{
				PGXCNodePoolSlot *slot = agent->dn_connections[i];

				/* Reset given slot with parameters */
				if (slot)
					PGXCNodeSendSetQuery(slot->conn, "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;");
			}
		}

		if (agent->coord_connections)
		{
			/* Check agent slot for each Coordinator */
			for (i = 0; i < agent->num_coord_connections; i++)
			{
				PGXCNodePoolSlot *slot = agent->coord_connections[i];

				/* Reset given slot with parameters */
				if (slot)
					PGXCNodeSendSetQuery(slot->conn, "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;");
			}
		}
	}

	/* Parameters are reset, so free commands */
	if (agent->session_params)
	{
		pfree(agent->session_params);
		agent->session_params = NULL;
	}
	if (agent->local_params)
	{
		pfree(agent->local_params);
		agent->local_params = NULL;
	}
}


/*
 * Create new empty pool for a database.
 * By default Database Pools have a size null so as to avoid interactions
 * between PGXC nodes in the cluster (Co/Co, Dn/Dn and Co/Dn).
 * Pool is increased at the first GET_CONNECTION message received.
 * Returns POOL_OK if operation succeed POOL_FAIL in case of OutOfMemory
 * error and POOL_WEXIST if poll for this database already exist.
 */
static DatabasePool *
create_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool;
	int			i;

	/* check if exist */
	databasePool = find_database_pool(database, user_name);
	if (databasePool)
	{
		/* already exist */
		return databasePool;
	}

	/* Allocate memory */
	databasePool = (DatabasePool *) palloc(sizeof(DatabasePool));
	if (!databasePool)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return NULL;
	}

	 /* Copy the database name */
	databasePool->database = pstrdup(database);
	 /* Copy the user name */
	databasePool->user_name = pstrdup(user_name);
	if (!databasePool->database)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		pfree(databasePool);
		return NULL;
	}

	/* Init next reference */
	databasePool->next = NULL;

	/* Init Datanode pools */
	if (NumDataNodes != 0)
	{
		databasePool->dataNodePools =
			(PGXCNodePool **) palloc(NumDataNodes * sizeof(PGXCNodePool **));
		if (!databasePool->dataNodePools)
		{
			/* out of memory */
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			pfree(databasePool->database);
			pfree(databasePool->user_name);
			pfree(databasePool);
			return NULL;
		}

		for (i = 0; i < NumDataNodes; i++)
			databasePool->dataNodePools[i] = NULL;
	}
	else
		databasePool->dataNodePools = NULL;


	/* Init Coordinator pools */
	if (NumCoords != 0)
	{
		databasePool->coordNodePools = (PGXCNodePool **) palloc(NumCoords * sizeof(PGXCNodePool **));
		if (!databasePool->coordNodePools)
		{
			/* out of memory */
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			pfree(databasePool->database);
			pfree(databasePool->user_name);
			pfree(databasePool);
			return NULL;
		}


		for (i = 0; i < NumCoords; i++)
			databasePool->coordNodePools[i] = NULL;
	}
	else
		databasePool->coordNodePools = NULL;

	/* Save number of node pool */
	databasePool->num_dn_pools = NumDataNodes;
	databasePool->num_co_pools = NumCoords;

	/* Insert into the list */
	insert_database_pool(databasePool);

	return databasePool;
}


/*
 * Destroy the pool and free memory
 */
static int
destroy_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool;
	int			i;

	/* Delete from the list */
	databasePool = remove_database_pool(database, user_name);
	if (databasePool)
	{
		if (databasePool->dataNodePools)
		{
			for (i = 0; i < NumDataNodes; i++)
				if (databasePool->dataNodePools[i])
					destroy_node_pool(databasePool->dataNodePools[i]);
			pfree(databasePool->dataNodePools);
		}
		if (databasePool->coordNodePools)
		{
			for (i = 0; i < NumCoords; i++)
				if (databasePool->coordNodePools[i])
					destroy_node_pool(databasePool->coordNodePools[i]);
			pfree(databasePool->coordNodePools);
		}
		/* free allocated memory */
		pfree(databasePool->database);
		pfree(databasePool->user_name);
		pfree(databasePool);
		return 1;
	}
	return 0;
}


/*
 * Insert new database pool to the list
 */
static void
insert_database_pool(DatabasePool *databasePool)
{
	Assert(databasePool);

	/* Reference existing list or null the tail */
	if (databasePools)
		databasePool->next = databasePools;
	else
		databasePool->next = NULL;

	/* Update head pointer */
	databasePools = databasePool;
}

/*
 * Rebuild information of database pools
 */
static void
reload_database_pools(void)
{
	DatabasePool *databasePool;

	/* Scan the list and reload each pool */
	databasePool = databasePools;
	while (databasePool)
	{
		/* Update each database pool slot with new connection information */
		int i;
		bool co_slot_used[databasePool->num_co_pools];
		bool dn_slot_used[databasePool->num_dn_pools];
		int new_co_num = NumCoords;
		int new_dn_num = NumDataNodes;
		PGXCNodePool **new_co_pools = NULL;
		PGXCNodePool **new_dn_pools = NULL;

		new_co_pools = (PGXCNodePool **) palloc(new_co_num * sizeof(PGXCNodePool **));
		new_dn_pools = (PGXCNodePool **) palloc(new_dn_num * sizeof(PGXCNodePool **));

		/* Check on the database pools that have been reused */
		for (i = 0; i < databasePool->num_co_pools; i++)
			co_slot_used[i] = false;
		for (i = 0; i < databasePool->num_dn_pools; i++)
			dn_slot_used[i] = false;

		for (i = 0; i < new_co_num; i++)
		{
			int j;
			Oid nodeoid_check = coord_connInfos[i].nodeoid;
			bool is_found = false;
			int index_id = 0;
			char *connstr_chk = PGXCNodeConnStr(coord_connInfos[i].host,
												coord_connInfos[i].port,
												databasePool->database,
												databasePool->user_name,
												"coordinator");

			/* Scan for pool existence */
			for (j = 0; j < databasePool->num_co_pools; j++)
			{
				PGXCNodePool *pool = databasePool->coordNodePools[j];
				if (!pool)
					continue;

				/*
				 * Node Oid and connection string has to be the same
				 * to ensure consistency.
				 */
				if (pool->nodeoid == nodeoid_check &&
					strcmp(pool->connstr, connstr_chk) == 0)
				{
					co_slot_used[j] = true;
					is_found = true;
					index_id = j;
					break;
				}
			}

			if (co_slot_used[index_id] &&
				databasePool->coordNodePools &&
				is_found)
			{
				new_co_pools[i] = databasePool->coordNodePools[index_id];
			}
			else
				new_co_pools[i] = NULL;
			pfree(connstr_chk);
		}
		for (i = 0; i < new_dn_num; i++)
		{
			int j;
			Oid nodeoid_check = datanode_connInfos[i].nodeoid;
			int index_id = 0;
			bool is_found = false;
			char *connstr_chk = PGXCNodeConnStr(datanode_connInfos[i].host,
												datanode_connInfos[i].port,
												databasePool->database,
												databasePool->user_name,
												"coordinator");

			/* Scan for pool existence */
			for (j = 0; j < databasePool->num_dn_pools; j++)
			{
				PGXCNodePool *pool = databasePool->dataNodePools[j];

				if (!pool)
					continue;

				if (pool->nodeoid == nodeoid_check &&
					strcmp(pool->connstr, connstr_chk) == 0)
				{
					dn_slot_used[j] = true;
					is_found = true;
					index_id = j;
					break;
				}
			}

			if (dn_slot_used[index_id] &&
				databasePool->dataNodePools &&
				is_found)
			{
				new_dn_pools[i] = databasePool->dataNodePools[index_id];
			}
			else
				new_dn_pools[i] = NULL;
			pfree(connstr_chk);
		}

		/* Clean up node pools that are not necessary anymore */
		for (i = 0; i < databasePool->num_co_pools; i++)
		{
			if (!co_slot_used[i])
				destroy_node_pool(databasePool->coordNodePools[i]);
		}
		for (i = 0; i < databasePool->num_dn_pools; i++)
		{
			if (!dn_slot_used[i])
				destroy_node_pool(databasePool->dataNodePools[i]);
		}
		if (databasePool->coordNodePools)
			pfree(databasePool->coordNodePools);
		if (databasePool->dataNodePools)
			pfree(databasePool->dataNodePools);

		/* Update new database pool */
		databasePool->num_co_pools = new_co_num;
		databasePool->num_dn_pools = new_dn_num;
		databasePool->coordNodePools = new_co_pools;
		databasePool->dataNodePools = new_dn_pools;
		databasePool = databasePool->next;
	}
}


/*
 * Find pool for specified database and username in the list
 */
static DatabasePool *
find_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool;

	/* Scan the list */
	databasePool = databasePools;
	while (databasePool)
	{
		if (strcmp(database, databasePool->database) == 0 &&
			strcmp(user_name, databasePool->user_name) == 0)
			break;

		databasePool = databasePool->next;
	}
	return databasePool;
}

/*
 * Find pool to be cleaned for specified database in the list
 */
static DatabasePool *
find_database_pool_to_clean(const char *database,
							const char *user_name,
							List *dn_list,
							List *co_list)
{
	DatabasePool *databasePool;

	/* Scan the list */
	databasePool = databasePools;
	while (databasePool)
	{
		ListCell   *nodelist_item;

		/* If database name does not correspond, move to next one */
		if (database && strcmp(database, databasePool->database) != 0)
		{
			databasePool = databasePool->next;		
			continue;
		}

		/* If user name does not correspond, move to next one */
		if (user_name && strcmp(user_name, databasePool->user_name) != 0)
		{
			databasePool = databasePool->next;		
			continue;
		}

		/* Check if this database pool is clean for given coordinator list */
		foreach (nodelist_item, co_list)
		{
			int nodenum = lfirst_int(nodelist_item);

			if (databasePool->coordNodePools &&
				databasePool->coordNodePools[nodenum] &&
				databasePool->coordNodePools[nodenum]->freeSize != 0)
				return databasePool;
		}

		/* Check if this database pool is clean for given datanode list */
		foreach (nodelist_item, dn_list)
		{
			int nodenum = lfirst_int(nodelist_item);

			if (databasePool->dataNodePools &&
				databasePool->dataNodePools[nodenum] &&
				databasePool->dataNodePools[nodenum]->freeSize != 0)
				return databasePool;
		}

		databasePool = databasePool->next;
	}
	return databasePool;
}

/*
 * Remove pool for specified database from the list
 */
static DatabasePool *
remove_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool,
			   *prev;

	/* Scan the list */
	databasePool = databasePools;
	prev = NULL;
	while (databasePool)
	{

		/* if match break the loop and return */
		if (strcmp(database, databasePool->database) == 0 &&
			strcmp(user_name, databasePool->user_name) == 0)
			break;
		prev = databasePool;
		databasePool = databasePool->next;
	}

	/* if found */
	if (databasePool)
	{

		/* Remove entry from chain or update head */
		if (prev)
			prev->next = databasePool->next;
		else
			databasePools = databasePool->next;


		databasePool->next = NULL;
	}
	return databasePool;
}

/*
 * Acquire connection
 */
static PGXCNodePoolSlot *
acquire_connection(DatabasePool *dbPool, int node, char client_conn_type)
{
	PGXCNodePool *nodePool;
	PGXCNodePoolSlot *slot;

	Assert(dbPool);

	/* Manage the case where pool is not ready */
	if (node >= NumCoords && client_conn_type == REMOTE_CONN_COORD)
	{
		elog(WARNING, "can not connect to coordinator %d", node);
		return NULL;
	}
	if (node >= NumDataNodes && client_conn_type == REMOTE_CONN_DATANODE)
	{
		elog(WARNING, "can not connect to datanode %d", node);
		return NULL;
	}

	if (client_conn_type == REMOTE_CONN_DATANODE)
		Assert(0 <= node && node < NumDataNodes);
	else if (client_conn_type == REMOTE_CONN_COORD)
		Assert(0 <= node && node < NumCoords);

	slot = NULL;
	/* Find referenced node pool depending on type of client connection */
	if (client_conn_type == REMOTE_CONN_DATANODE)
		nodePool = dbPool->dataNodePools[node];
	else if (client_conn_type == REMOTE_CONN_COORD)
		nodePool = dbPool->coordNodePools[node];

	/*
	 * When a Coordinator pool is initialized by a Coordinator Postmaster,
	 * it has a NULL size and is below minimum size that is 1
	 * This is to avoid problems of connections between Coordinators
	 * when creating or dropping Databases.
	 */
	if (nodePool == NULL || nodePool->freeSize == 0)
	{
		grow_pool(dbPool, node, client_conn_type);

		/* Get back the correct slot that has been grown up*/
		if (client_conn_type == REMOTE_CONN_DATANODE)
			nodePool = dbPool->dataNodePools[node];
		else if (client_conn_type == REMOTE_CONN_COORD)
			nodePool = dbPool->coordNodePools[node];
	}

	/* Check available connections */
	while (nodePool && nodePool->freeSize > 0)
	{
		int			poll_result;

		slot = nodePool->slot[--(nodePool->freeSize)];

	retry:
		/* Make sure connection is ok */
		poll_result = pqReadReady((PGconn *)slot->conn);

		if (poll_result == 0)
			break; 		/* ok, no data */
		else if (poll_result < 0)
		{
			if (errno == EAGAIN || errno == EINTR)
				goto retry;

			elog(WARNING, "Error in checking connection, errno = %d", errno);
		}
		else
			elog(WARNING, "Unexpected data on connection, cleaning.");

		destroy_slot(slot);
		slot = NULL;

		/* Decrement current max pool size */
		(nodePool->size)--;
		/* Ensure we are not below minimum size */
		grow_pool(dbPool, node, client_conn_type);
	}

	if (slot == NULL)
	{
		if (client_conn_type == REMOTE_CONN_DATANODE)
			elog(WARNING, "can not connect to datanode %d", node);
		else if (client_conn_type == REMOTE_CONN_COORD)
			elog(WARNING, "can not connect to coordinator %d", node);
	}
	return slot;
}


/*
 * release connection from specified pool and slot
 */
static void
release_connection(DatabasePool * dbPool, PGXCNodePoolSlot * slot,
				   int index, bool force_destroy, char client_conn_type)
{
	PGXCNodePool *nodePool;

	Assert(dbPool);
	Assert(slot);

	if (client_conn_type == REMOTE_CONN_DATANODE)
		Assert(0 <= index && index < NumDataNodes);
	else if (client_conn_type == REMOTE_CONN_COORD)
		Assert(0 <= index && index < NumCoords);

	/* Find referenced node pool depending on client connection type */
	if (client_conn_type == REMOTE_CONN_DATANODE)
		nodePool = dbPool->dataNodePools[index];
	else if (client_conn_type == REMOTE_CONN_COORD)
		nodePool = dbPool->coordNodePools[index];

	if (nodePool == NULL)
	{
		/* report problem */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("database does not use node %d", (index))));
		return;
	}

	/* return or discard */
	if (!force_destroy)
	{
		/* Insert the slot into the array and increase pool size */
		nodePool->slot[(nodePool->freeSize)++] = slot;
	}
	else
	{
		elog(DEBUG1, "Cleaning up connection from pool %s, closing", nodePool->connstr);
		destroy_slot(slot);
		/* Decrement pool size */
		(nodePool->size)--;
		/* Ensure we are not below minimum size */
		grow_pool(dbPool, index, client_conn_type);
	}
}


/*
 * Increase database pool size depending on connection type:
 * REMOTE_CONN_COORD or REMOTE_CONN_DATANODE
 */
static void
grow_pool(DatabasePool * dbPool, int index, char client_conn_type)
{
	PGXCNodePool *nodePool;

	Assert(dbPool);
	if (client_conn_type == REMOTE_CONN_DATANODE)
		Assert(0 <= index && index < NumDataNodes);
	else if (client_conn_type == REMOTE_CONN_COORD)
		Assert(0 <= index && index < NumCoords);

	/* Find referenced node pool */
	if (client_conn_type == REMOTE_CONN_DATANODE)
		nodePool = dbPool->dataNodePools[index];
	else if (client_conn_type == REMOTE_CONN_COORD)
		nodePool = dbPool->coordNodePools[index];

	if (!nodePool)
	{
		char *remote_type;

		/* Allocate new DBNode Pool */
		nodePool = (PGXCNodePool *) palloc(sizeof(PGXCNodePool));
		if (!nodePool)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));

		/*
		 * Don't forget to define the type of remote connection
		 * Now PGXC just support Co->Co and Co->Dn connections
		 * but Dn->Dn Connections could be used for other purposes.
		 */
		if (IS_PGXC_COORDINATOR)
			remote_type = pstrdup("coordinator");
		else if (IS_PGXC_DATANODE)
			remote_type = pstrdup("datanode");

		if (client_conn_type == REMOTE_CONN_DATANODE)
			/* initialize it */
			nodePool->connstr = PGXCNodeConnStr(datanode_connInfos[index].host,
												datanode_connInfos[index].port,
												dbPool->database,
												dbPool->user_name,
												remote_type);
		else if (client_conn_type == REMOTE_CONN_COORD)
			nodePool->connstr = PGXCNodeConnStr(coord_connInfos[index].host,
												coord_connInfos[index].port,
												dbPool->database,
												dbPool->user_name,
												remote_type);

		if (!nodePool->connstr)
		{
			pfree(nodePool);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		nodePool->slot = (PGXCNodePoolSlot **) palloc(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
		if (!nodePool->slot)
		{
			pfree(nodePool);
			pfree(nodePool->connstr);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}
		memset(nodePool->slot, 0, MaxPoolSize * sizeof(PGXCNodePoolSlot *));
		nodePool->freeSize = 0;
		nodePool->size = 0;

		/* and insert into the array */
		if (client_conn_type == REMOTE_CONN_DATANODE)
		{
			nodePool->nodeoid = datanode_connInfos[index].nodeoid;
			dbPool->dataNodePools[index] = nodePool;
		}
		else if (client_conn_type == REMOTE_CONN_COORD)
		{
			nodePool->nodeoid = coord_connInfos[index].nodeoid;
			dbPool->coordNodePools[index] = nodePool;
		}
	}

	while (nodePool->size < MinPoolSize || (nodePool->freeSize == 0 && nodePool->size < MaxPoolSize))
	{
		PGXCNodePoolSlot *slot;

		/* Allocate new slot */
		slot = (PGXCNodePoolSlot *) palloc(sizeof(PGXCNodePoolSlot));
		if (slot == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		/* If connection fails, be sure that slot is destroyed cleanly */
		slot->xc_cancelConn = NULL;

		/* Establish connection */
		slot->conn = PGXCNodeConnect(nodePool->connstr);
		if (!PGXCNodeConnected(slot->conn))
		{
			destroy_slot(slot);
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("failed to connect to data node")));
			break;
		}

		slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);

		/* Insert at the end of the pool */
		nodePool->slot[(nodePool->freeSize)++] = slot;

		/* Increase count of pool size */
		(nodePool->size)++;
		elog(DEBUG1, "Pooler: increased pool size to %d for pool %s",
			 nodePool->size,
			 nodePool->connstr);
	}
}


/*
 * Destroy pool slot
 */
static void
destroy_slot(PGXCNodePoolSlot *slot)
{
	if (!slot)
		return;

	PQfreeCancel((PGcancel *)slot->xc_cancelConn);
	PGXCNodeClose(slot->conn);
	pfree(slot);
}


/*
 * Destroy node pool
 */
static void
destroy_node_pool(PGXCNodePool *node_pool)
{
	int			i;

	if (!node_pool)
		return;

	/*
	 * At this point all agents using connections from this pool should be already closed
	 * If this not the connections to the data nodes assigned to them remain open, this will
	 * consume data node resources.
	 * I believe this is not the case because pool is only destroyed on coordinator shutdown.
	 * However we should be careful when changing things
	 */
	elog(DEBUG1, "About to destroy node pool %s, current size is %d, %d connections are in use",
		 node_pool->connstr, node_pool->freeSize, node_pool->size - node_pool->freeSize);
	if (node_pool->connstr)
		pfree(node_pool->connstr);

	if (node_pool->slot)
	{
		for (i = 0; i < node_pool->freeSize; i++)
			destroy_slot(node_pool->slot[i]);
		pfree(node_pool->slot);
	}
}


/*
 * Main handling loop
 */
static void
PoolerLoop(void)
{
	StringInfoData input_message;

	server_fd = pool_listen(PoolerPort, UnixSocketDir);
	if (server_fd == -1)
	{
		/* log error */
		return;
	}
	initStringInfo(&input_message);
	for (;;)
	{
		int			nfds;
		fd_set		rfds;
		int			retval;
		int			i;

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive(true))
			exit(1);

		/* watch for incoming connections */
		FD_ZERO(&rfds);
		FD_SET(server_fd, &rfds);

		nfds = server_fd;

		/* watch for incoming messages */
		for (i = 0; i < agentCount; i++)
		{
			PoolAgent  *agent = poolAgents[i];
			int			sockfd = Socket(agent->port);
			FD_SET		(sockfd, &rfds);

			nfds = Max(nfds, sockfd);
		}

		/* wait for event */
		retval = select(nfds + 1, &rfds, NULL, NULL, NULL);
		if (shutdown_requested)
		{
			for (i = agentCount - 1; i >= 0; i--)
			{
				PoolAgent  *agent = poolAgents[i];

				agent_destroy(agent);
			}
			while (databasePools)
				if (destroy_database_pool(databasePools->database,
										  databasePools->user_name) == 0)
					break;
			close(server_fd);
			exit(0);
		}
		if (retval > 0)
		{
			/*
			 * Agent may be removed from the array while processing
			 * and trailing items are shifted, so scroll downward
			 * to avoid problem
			 */
			for (i = agentCount - 1; i >= 0; i--)
			{
				PoolAgent  *agent = poolAgents[i];
				int			sockfd = Socket(agent->port);

				if (FD_ISSET(sockfd, &rfds))
					agent_handle_input(agent, &input_message);
			}
			if (FD_ISSET(server_fd, &rfds))
				agent_create();
		}
	}
}

/*
 * Clean Connection in all Database Pools for given Datanode and Coordinator list
 */
#define TIMEOUT_CLEAN_LOOP 10

int
clean_connection(List *dn_discard, List *co_discard, const char *database, const char *user_name)
{
	DatabasePool *databasePool;
	int			dn_len = list_length(dn_discard);
	int			co_len = list_length(co_discard);
	int			dn_list[list_length(dn_discard)];
	int			co_list[list_length(co_discard)];
	int			count, i;
	int			res = CLEAN_CONNECTION_COMPLETED;
	ListCell   *nodelist_item;
	PGXCNodePool *nodePool;

	/* Save in array the lists of node number */
	count = 0;
	foreach(nodelist_item,dn_discard)
		dn_list[count++] = lfirst_int(nodelist_item);

	count = 0;
	foreach(nodelist_item, co_discard)
		co_list[count++] = lfirst_int(nodelist_item);

	/* Find correct Database pool to clean */
	databasePool = find_database_pool_to_clean(database, user_name, dn_discard, co_discard);

	while (databasePool)
	{
		databasePool = find_database_pool_to_clean(database, user_name, dn_discard, co_discard);

		/* Database pool has not been found, cleaning is over */
		if (!databasePool)
			break;

		/*
		 * Clean each Pool Correctly
		 * First for Datanode Pool
		 */
		for (count = 0; count < dn_len; count++)
		{
			int node_num = dn_list[count];
			nodePool = databasePool->dataNodePools[node_num];

			if (nodePool)
			{
				/* Check if connections are in use */
				if (nodePool->freeSize != nodePool->size)
				{
					elog(WARNING, "Pool of Database %s is using Datanode %d connections",
								databasePool->database, node_num);
					res = CLEAN_CONNECTION_NOT_COMPLETED;
				}

				/* Destroy connections currently in Node Pool */
				if (nodePool->slot)
				{
					for (i = 0; i < nodePool->freeSize; i++)
						destroy_slot(nodePool->slot[i]);

					/* Move slots in use at the beginning of Node Pool array */
					for (i = nodePool->freeSize; i < nodePool->size; i++ )
						nodePool->slot[i - nodePool->freeSize] = nodePool->slot[i];
				}
				nodePool->size -= nodePool->freeSize;
				nodePool->freeSize = 0;
			}
		}

		/* Then for Coordinators */
		for (count = 0; count < co_len; count++)
		{
			int node_num = co_list[count];
			nodePool = databasePool->coordNodePools[node_num];

			if (nodePool)
			{
				/* Check if connections are in use */
				if (nodePool->freeSize != nodePool->size)
				{
					elog(WARNING, "Pool of Database %s is using Coordinator %d connections",
								databasePool->database, node_num);
					res = CLEAN_CONNECTION_NOT_COMPLETED;
				}

				/* Destroy connections currently in Node Pool */
				if (nodePool->slot)
				{
					for (i = 0; i < nodePool->freeSize; i++)
						destroy_slot(nodePool->slot[i]);

					/* Move slots in use at the beginning of Node Pool array */
					for (i = nodePool->freeSize; i < nodePool->size; i++ )
						nodePool->slot[i - nodePool->freeSize] = nodePool->slot[i];
				}
				nodePool->size -= nodePool->freeSize;
				nodePool->freeSize = 0;
			}
		}
	}

	/* Release lock on Pooler, to allow transactions to connect again. */
	is_pool_locked = false;
	return res;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
int *
abort_pids(int *len, int pid, const char *database, const char *user_name)
{
	int *pids = NULL;
	int i = 0;
	int count;

	Assert(!is_pool_locked);
	Assert(agentCount > 0);

	is_pool_locked = true;

	pids = (int *) palloc((agentCount - 1) * sizeof(int));

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = 0; count < agentCount; count++)
	{
		if (poolAgents[count]->pid == pid)
			continue;

		if (database && strcmp(poolAgents[count]->pool->database, database) != 0)
			continue;

		if (user_name && strcmp(poolAgents[count]->pool->user_name, user_name) != 0)
			continue;

		if (kill(poolAgents[count]->pid, SIGTERM) < 0)
			elog(ERROR, "kill(%ld,%d) failed: %m",
						(long) poolAgents[count]->pid, SIGTERM);

		pids[i++] = poolAgents[count]->pid;
	}

	*len = i;

	return pids;
}

/*
 *
 */
static void
pooler_die(SIGNAL_ARGS)
{
	shutdown_requested = true;
}


/*
 *
 */
static void
pooler_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);
	exit(2);
}

bool
IsPoolHandle(void)
{
	if (poolHandle == NULL)
		return false;
	return true;
}
