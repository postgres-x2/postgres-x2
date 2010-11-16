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
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
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
#include "pgxc/poolmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolutils.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "postmaster/postmaster.h"		/* For UnixSocketDir */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>

/* Configuration options */
int			NumDataNodes = 2;
int			NumCoords = 1;
int			MinPoolSize = 1;
int			MaxPoolSize = 100;
int			PoolerPort = 6667;

bool		PersistentConnections = false;

/* The memory context */
static MemoryContext PoolerMemoryContext = NULL;

/* Connection info of Datanodes */
char	   *DataNodeHosts = NULL;
char	   *DataNodePorts = NULL;
char	   *DataNodeUsers = NULL;
char	   *DataNodePwds = NULL;

/* Connection info of Coordinators */
char	   *CoordinatorHosts = NULL;
char	   *CoordinatorPorts = NULL;
char	   *CoordinatorUsers = NULL;
char	   *CoordinatorPwds = NULL;

/* PGXC Nodes info list */
static PGXCNodeConnectionInfo *datanode_connInfos;
static PGXCNodeConnectionInfo *coord_connInfos;

/* Pool to all the databases (linked list) */
static DatabasePool *databasePools = NULL;

/* PoolAgents */
static int	agentCount = 0;
static PoolAgent **poolAgents;

static PoolHandle *Handle = NULL;

static int	is_pool_cleaning = false;
static int	server_fd = -1;

static void agent_init(PoolAgent *agent, const char *database);
static void agent_destroy(PoolAgent *agent);
static void agent_create(void);
static void agent_handle_input(PoolAgent *agent, StringInfo s);
static DatabasePool *create_database_pool(const char *database);
static void insert_database_pool(DatabasePool *pool);
static int	destroy_database_pool(const char *database);
static DatabasePool *find_database_pool(const char *database);
static DatabasePool *remove_database_pool(const char *database);
static int *agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist);
static PGXCNodePoolSlot *acquire_connection(DatabasePool *dbPool, int node, char client_conn_type);
static void agent_release_connections(PoolAgent *agent, List *dn_discard, List *co_discard);
static void release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot, int index, bool clean,
							   char client_conn_type);
static void destroy_slot(PGXCNodePoolSlot *slot);
static void grow_pool(DatabasePool *dbPool, int index, char client_conn_type);
static void destroy_node_pool(PGXCNodePool *node_pool);
static void PoolerLoop(void);
static int clean_connection(List *dn_discard, List *co_discard, const char *database);
static int *abort_pids(int *count, int pid, const char *database);

/* Signal handlers */
static void pooler_die(SIGNAL_ARGS);
static void pooler_quickdie(SIGNAL_ARGS);

/* Check status of connection */
extern int	pqReadReady(PGconn *conn);

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t shutdown_requested = false;


/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;
	int			i, count;
	MemoryContext old_context;

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
	old_context = MemoryContextSwitchTo(PoolerMemoryContext);

	poolAgents = (PoolAgent **) palloc(MaxConnections * sizeof(PoolAgent *));
	if (poolAgents == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	datanode_connInfos = (PGXCNodeConnectionInfo *)
						 palloc(NumDataNodes * sizeof(PGXCNodeConnectionInfo));
	coord_connInfos = (PGXCNodeConnectionInfo *)
					  palloc(NumCoords * sizeof(PGXCNodeConnectionInfo));
	if (coord_connInfos == NULL
		|| datanode_connInfos == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* Parse Host/Port/Password/User data for Coordinators and Datanodes */
	for (count = 0; count < 2; count++)
	{
		PGXCNodeConnectionInfo *connectionInfos;
		int num_nodes;
		if (count == 0)
		{
			/* Need a modifiable copy */
			rawstring = pstrdup(DataNodeHosts);
			connectionInfos = datanode_connInfos;
			num_nodes = NumDataNodes;
		}
		else
		{
			/* Need a modifiable copy */
			rawstring = pstrdup(CoordinatorHosts);
			connectionInfos = coord_connInfos;
			num_nodes = NumCoords;
		}

		/* Do that for Coordinator and Datanode strings */
		/* Parse string into list of identifiers */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax for \"data_node_hosts\"")));
		}

		i = 0;
		foreach(l, elemlist)
		{
			char	   *curhost = (char *) lfirst(l);

			connectionInfos[i].host = pstrdup(curhost);
			if (connectionInfos[i].host == NULL)
			{
				ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			}
			/* Ignore extra entries, if any */
			if (++i == num_nodes)
				break;
		}
		list_free(elemlist);
		pfree(rawstring);

		/* Validate */
		if (i == 0)
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax for \"data_node_hosts\"")));
		}
		else if (i == 1)
		{
			/* Copy all values from first */
			for (; i < num_nodes; i++)
			{
				connectionInfos[i].host = pstrdup(connectionInfos[0].host);
				if (connectionInfos[i].host == NULL)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of memory")));
				}
			}
		}
		else if (i < num_nodes)
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax for \"data_node_hosts\"")));
		}

		/* Parse port data for Coordinators and Datanodes */
		/* Need a modifiable copy */
		if (count == 0)
			rawstring = pstrdup(DataNodePorts);
		if (count == 1)
			rawstring = pstrdup(CoordinatorPorts);

		/* Parse string into list of identifiers */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax for \"data_node_ports\"")));
		}

		i = 0;
		foreach(l, elemlist)
		{
			char	   *curport = (char *) lfirst(l);

			connectionInfos[i].port = pstrdup(curport);
			if (connectionInfos[i].port == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}
			/* Ignore extra entries, if any */
			if (++i == num_nodes)
				break;
		}
		list_free(elemlist);
		pfree(rawstring);

		/* Validate */
		if (i == 0)
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax for \"data_node_ports\"")));
		}
		else if (i == 1)
		{
			/* Copy all values from first */
			for (; i < num_nodes; i++)
			{
				connectionInfos[i].port = pstrdup(connectionInfos[0].port);
				if (connectionInfos[i].port == NULL)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of memory")));
				}
			}
		}
		else if (i < num_nodes)
		{
			if (count == 0)
			/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"data_node_ports\"")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"coordinator_ports\"")));			
		}

		if (count == 0)
			rawstring = pstrdup(DataNodeUsers);
		if (count == 1)
			rawstring = pstrdup(CoordinatorUsers);

		/* Parse string into list of identifiers */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			if (count == 0)
			/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"data_node_users\"")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"coordinator_users\"")));			
		}

		i = 0;
		foreach(l, elemlist)
		{
			char	   *curuser = (char *) lfirst(l);

			connectionInfos[i].uname = pstrdup(curuser);
			if (connectionInfos[i].uname == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}
			/* Ignore extra entries, if any */
			if (++i == num_nodes)
				break;
		}
		list_free(elemlist);
		pfree(rawstring);

		/* Validate */
		if (i == 0)
		{
			if (count == 0)
				/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"data_node_users\"")));
			else
				/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"coordinator_users\"")));
		}
		else if (i == 1)
		{
			/* Copy all values from first */
			for (; i < num_nodes; i++)
			{
				connectionInfos[i].uname = pstrdup(connectionInfos[0].uname);
				if (connectionInfos[i].uname == NULL)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of memory")));
				}
			}
		}
		else if (i < num_nodes)
		{
			if (count == 0)
				/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"data_node_users\"")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"coordinator_users\"")));
		}

		if (count == 0)
			rawstring = pstrdup(DataNodePwds);
		if (count == 1)
			rawstring = pstrdup(CoordinatorPwds);

		/* Parse string into list of identifiers */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			if (count == 0)
				/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"data_node_passwords\"")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"coordinator_passwords\"")));
		}

		i = 0;
		foreach(l, elemlist)
		{
			char	   *curpassword = (char *) lfirst(l);

			connectionInfos[i].password = pstrdup(curpassword);
			if (connectionInfos[i].password == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}
			/* Ignore extra entries, if any */
			if (++i == num_nodes)
				break;
		}
		list_free(elemlist);
		pfree(rawstring);

		/* Validate */
		if (i == 0)
		{
			if (count == 0)
				/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"data_node_passwords\"")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"coordinator_passwords\"")));
		}
		else if (i == 1)
		{
			/* Copy all values from first */
			for (; i < num_nodes; i++)
			{
				connectionInfos[i].password = pstrdup(connectionInfos[0].password);
				if (connectionInfos[i].password == NULL)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of memory")));
				}
			}
		}
		else if (i < num_nodes)
		{
			if (count == 0)
				/* syntax error in list */
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"data_node_passwords\"")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid list syntax for \"coordinator_passwords\"")));
		}
	}
	/* End of Parsing for Datanode and Coordinator Data */

	PoolerLoop();
	return 0;
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
	agent->dn_connections = NULL;
	agent->coord_connections = NULL;
	agent->pid = 0;

	/* Append new agent to the list */
	poolAgents[agentCount++] = agent;
}


/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
void
PoolManagerConnect(PoolHandle *handle, const char *database)
{
	int n32;
	char msgtype = 'c';

	Assert(handle);
	Assert(database);

	/* Save the handle */
	Handle = handle;

	/* Message type */
	pool_putbytes(&handle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(strlen(database) + 13);
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
}


/*
 * Init PoolAgent
*/
static void
agent_init(PoolAgent *agent, const char *database)
{
	Assert(agent);
	Assert(database);

	/* disconnect if we are still connected */
	if (agent->pool)
		agent_release_connections(agent, NULL, NULL);

	/* find database */
	agent->pool = find_database_pool(database);

	/* create if not found */
	if (agent->pool == NULL)
		agent->pool = create_database_pool(database);
}


/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{
	int			i;

	Assert(agent);

	close(Socket(agent->port));

	/* Discard connections if any remaining */
	if (agent->pool)
		agent_release_connections(agent, NULL, NULL);

	/* find agent in the list */
	for (i = 0; i < agentCount; i++)
	{
		if (poolAgents[i] == agent)
		{
			/* free memory */
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
PoolManagerDisconnect(PoolHandle *handle)
{
	Assert(handle);

	pool_putmessage(&handle->port, 'd', NULL, 0);
	pool_flush(&Handle->port);

	close(Socket(handle->port));

	pfree(handle);
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

	Assert(Handle);

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

	pool_putmessage(&Handle->port, 'g', (char *) nodes, sizeof(int) * (totlen + 2));
	pool_flush(&Handle->port);

	/* Receive response */
	fds = (int *) palloc(sizeof(int) * totlen);
	if (fds == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}
	if (pool_recvfds(&Handle->port, fds, totlen))
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
PoolManagerAbortTransactions(char *dbname, int **proc_pids)
{
	int num_proc_ids = 0;

	Assert(Handle);

	pool_putmessage(&Handle->port, 'a', dbname, strlen(dbname) + 1);

	pool_flush(&Handle->port);

	/* Then Get back Pids from Pooler */
	num_proc_ids = pool_recvpids(&Handle->port, proc_pids);

	return num_proc_ids;
}


/*
 * Clean up Pooled connections
 */
void
PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname)
{
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];
	ListCell   *nodelist_item;
	int			i, n32;
	char		msgtype = 'f';

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
	pool_putbytes(&Handle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(sizeof(int) * (totlen + 2) + strlen(dbname) + 9);
	pool_putbytes(&Handle->port, (char *) &n32, 4);

	/* Send list of nodes */
	pool_putbytes(&Handle->port, (char *) nodes, sizeof(int) * (totlen + 2));

	/* Length of Database string */
	n32 = htonl(strlen(dbname) + 1);
	pool_putbytes(&Handle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator */
	pool_putbytes(&Handle->port, dbname, strlen(dbname) + 1);
	pool_flush(&Handle->port);

	/* Receive result message */
	if (pool_recvres(&Handle->port) != CLEAN_CONNECTION_COMPLETED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Clean connections not completed")));
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
		const char *database;
		int			datanodecount;
		int			coordcount;
		List	   *datanodelist = NIL;
		List	   *coordlist = NIL;
		int		   *fds;
		int		   *pids;
		int			i, len, res;

		/*
		 * During a pool cleaning, Abort, Connect and Get Connections messages
		 * are not allowed on pooler side.
		 * It avoids to have new backends taking connections
		 * while remaining transactions are aborted during FORCE and then
		 * Pools are being shrinked.
		 */
		if (is_pool_cleaning && (qtype == 'a' ||
								 qtype == 'c' ||
								 qtype == 'g'))
			elog(WARNING,"Pool operation cannot run during Pool cleaning");

		switch (qtype)
		{
			case 'a':			/* ABORT */
				pool_getmessage(&agent->port, s, 0);
				database = pq_getmsgstring(s);
				pq_getmsgend(s);
				pids = abort_pids(&len, agent->pid, database);

				pool_sendpids(&agent->port, pids, len);
				if (pids)
					pfree(pids);
				break;
			case 'c':			/* CONNECT */
				pool_getmessage(&agent->port, s, 0);
				agent->pid = pq_getmsgint(s, 4);
				len = pq_getmsgint(s, 4);
				database = pq_getmsgbytes(s, len);
				/*
				 * Coordinator pool is not initialized.
				 * With that it would be impossible to create a Database by default.
				 */
				agent_init(agent, database);
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
				database = pq_getmsgbytes(s, len);
				pq_getmsgend(s);

				/* Clean up connections here */
				res = clean_connection(datanodelist, coordlist, database);

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
			case 'r':			/* RELEASE CONNECTIONS */
				pool_getmessage(&agent->port, s, 4 * NumDataNodes + 4 * NumCoords + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				if (coordcount != 0)
					for (i = 0; i < coordcount; i++)
						coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);
				agent_release_connections(agent, datanodelist, coordlist);
				list_free(datanodelist);
				list_free(coordlist);
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
 * acquire connection
 */
static int *
agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	int			i;
	int		   *result;
	ListCell   *nodelist_item;

	Assert(agent);

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
								palloc(NumDataNodes * sizeof(PGXCNodePoolSlot *));
		if (!agent->dn_connections)
		{
			pfree(result);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			return NULL;
		}

		for (i = 0; i < NumDataNodes; i++)
			agent->dn_connections[i] = NULL;
	}

	/* Then for the Coordinators */
	if (!agent->coord_connections)
	{
		agent->coord_connections = (PGXCNodePoolSlot **)
								palloc(NumCoords * sizeof(PGXCNodePoolSlot *));
		if (!agent->coord_connections)
		{
			pfree(result);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			return NULL;
		}

		for (i = 0; i < NumCoords; i++)
			agent->coord_connections[i] = NULL;
	}	


	/* Initialize result */
	i = 0;
	/* Save in array fds of Datanodes first */
	foreach(nodelist_item, datanodelist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->dn_connections[node - 1] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool, node, REMOTE_CONN_DATANODE);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				return NULL;
			}

			/* Store in the descriptor */
			agent->dn_connections[node - 1] = slot;
		}

		result[i++] = PQsocket((PGconn *) agent->dn_connections[node - 1]->conn);
	}

	/* Save then in the array fds for Coordinators */
	foreach(nodelist_item, coordlist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->coord_connections[node - 1] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool, node, REMOTE_CONN_COORD);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				return NULL;
			}

			/* Store in the descriptor */
			agent->coord_connections[node - 1] = slot;
		}

		result[i++] = PQsocket((PGconn *) agent->coord_connections[node - 1]->conn);
	}

	return result;
}


/*
 * Return connections back to the pool
 */
void
PoolManagerReleaseConnections(int dn_ndisc, int* dn_discard, int co_ndisc, int* co_discard)
{
	uint32		n32;
	/*
	 * Buffer contains the list of both Coordinator and Datanodes, as well
	 * as the number of connections
	 */
	uint32 		buf[2 + dn_ndisc + co_ndisc];
	int 		i;

	Assert(Handle);

	/* Insert the list of Datanodes in buffer */
	n32 = htonl((uint32) dn_ndisc);
	buf[0] = n32;

	for (i = 0; i < dn_ndisc;)
	{
		n32 = htonl((uint32) dn_discard[i++]);
		buf[i] = n32;
	}

	/* Insert the list of Coordinators in buffer */
	n32 = htonl((uint32) co_ndisc);
	buf[dn_ndisc + 1] = n32;

	/* Not necessary to send to pooler a request if there is no Coordinator */
	if (co_ndisc != 0)
	{
		for (i = dn_ndisc + 1; i < (dn_ndisc + co_ndisc + 1);)
		{
			n32 = htonl((uint32) co_discard[i - (dn_ndisc + 1)]);
			buf[++i] = n32;
		}
	}
	pool_putmessage(&Handle->port, 'r', (char *) buf,
					(2 + dn_ndisc + co_ndisc) * sizeof(uint32));
	pool_flush(&Handle->port);
}


/*
 * Release connections for Datanodes and Coordinators
 */
static void
agent_release_connections(PoolAgent *agent, List *dn_discard, List *co_discard)
{
	int			i;
	PGXCNodePoolSlot *slot;

	if (!agent->dn_connections && !agent->coord_connections)
		return;

	/* Discard first for Datanodes */
	if (dn_discard)
	{
		ListCell   *lc;

		foreach(lc, dn_discard)
		{
			int node = lfirst_int(lc);
			Assert(node > 0 && node <= NumDataNodes);
			slot = agent->dn_connections[node - 1];

			/* Discard connection */
			if (slot)
				release_connection(agent->pool, slot, node - 1, false, REMOTE_CONN_DATANODE);
			agent->dn_connections[node - 1] = NULL;
		}
	}

	/* Then discard for Coordinators */
	if (co_discard)
	{
		ListCell   *lc;

		foreach(lc, co_discard)
		{
			int node = lfirst_int(lc);
			Assert(node > 0 && node <= NumCoords);
			slot = agent->coord_connections[node - 1];

			/* Discard connection */
			if (slot)
				release_connection(agent->pool, slot, node - 1, false, REMOTE_CONN_COORD);
			agent->coord_connections[node - 1] = NULL;
		}
	}

	/*
	 * Remaining connections are assumed to be clean.
	 * First clean up for Datanodes
	 */
	for (i = 0; i < NumDataNodes; i++)
	{
		slot = agent->dn_connections[i];

		/* Release connection */
		if (slot)
			release_connection(agent->pool, slot, i, true, REMOTE_CONN_DATANODE);
		agent->dn_connections[i] = NULL;
	}
	/* Then clean up for Coordinator connections */
	for (i = 0; i < NumCoords; i++)
	{
		slot = agent->coord_connections[i];

		/* Release connection */
		if (slot)
			release_connection(agent->pool, slot, i, true, REMOTE_CONN_COORD);
		agent->coord_connections[i] = NULL;
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
create_database_pool(const char *database)
{
	DatabasePool *databasePool;
	int			i;

	/* check if exist */
	databasePool = find_database_pool(database);
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

	 /* Copy the database name */ ;
	databasePool->database = pstrdup(database);
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
	databasePool->dataNodePools = (PGXCNodePool **)
								  palloc(NumDataNodes * sizeof(PGXCNodePool **));
	if (!databasePool->dataNodePools)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		pfree(databasePool->database);
		pfree(databasePool);
		return NULL;
	}

	for (i = 0; i < NumDataNodes; i++)
		databasePool->dataNodePools[i] = NULL;

	/* Init Coordinator pools */
	databasePool->coordNodePools = (PGXCNodePool **)
								  palloc(NumCoords * sizeof(PGXCNodePool **));
	if (!databasePool->coordNodePools)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		pfree(databasePool->database);
		pfree(databasePool);
		return NULL;
	}

	for (i = 0; i < NumCoords; i++)
		databasePool->coordNodePools[i] = NULL;

	/* Insert into the list */
	insert_database_pool(databasePool);

	return databasePool;
}


/*
 * Destroy the pool and free memory
 */
static int
destroy_database_pool(const char *database)
{
	DatabasePool *databasePool;
	int			i;

	/* Delete from the list */
	databasePool = remove_database_pool(database);
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
 * Find pool for specified database in the list
 */
static DatabasePool *
find_database_pool(const char *database)
{
	DatabasePool *databasePool;

	/* Scan the list */
	databasePool = databasePools;
	while (databasePool)
	{
		/* if match break the loop and return */
		if (strcmp(database, databasePool->database) == 0)
			break;

		databasePool = databasePool->next;
	}
	return databasePool;
}


/*
 * Remove pool for specified database from the list
 */
static DatabasePool *
remove_database_pool(const char *database)
{
	DatabasePool *databasePool,
			   *prev;

	/* Scan the list */
	databasePool = databasePools;
	prev = NULL;
	while (databasePool)
	{

		/* if match break the loop and return */
		if (strcmp(database, databasePool->database) == 0)
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

	if (client_conn_type == REMOTE_CONN_DATANODE)
		Assert(0 < node && node <= NumDataNodes);
	else if (client_conn_type == REMOTE_CONN_COORD)
		Assert(0 < node && node <= NumCoords);

	slot = NULL;
	/* Find referenced node pool depending on type of client connection */
	if (client_conn_type == REMOTE_CONN_DATANODE)
		nodePool = dbPool->dataNodePools[node - 1];
	else if (client_conn_type == REMOTE_CONN_COORD)
		nodePool = dbPool->coordNodePools[node - 1];

	/*
	 * When a Coordinator pool is initialized by a Coordinator Postmaster,
	 * it has a NULL size and is below minimum size that is 1
	 * This is to avoid problems of connections between Coordinators
	 * when creating or dropping Databases.
	 */
	if (nodePool == NULL || nodePool->freeSize == 0)
	{
		grow_pool(dbPool, node - 1, client_conn_type);

		/* Get back the correct slot that has been grown up*/
		if (client_conn_type == REMOTE_CONN_DATANODE)
			nodePool = dbPool->dataNodePools[node - 1];
		else if (client_conn_type == REMOTE_CONN_COORD)
			nodePool = dbPool->coordNodePools[node - 1];
	}

	/* Check available connections */
	while (nodePool && nodePool->freeSize > 0)
	{
		int			poll_result;

		slot = nodePool->slot[--(nodePool->freeSize)];

	retry:
		/* Make sure connection is ok */
		poll_result = pqReadReady(slot->conn);

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
		grow_pool(dbPool, node - 1, client_conn_type);
	}

	if (slot == NULL)
	{
		if (client_conn_type == REMOTE_CONN_DATANODE)
			elog(WARNING, "can not connect to data node %d", node);
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
				   int index, bool clean, char client_conn_type)
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
				 errmsg("database does not use node %d", (index + 1))));
		return;
	}

	/* return or discard */
	if (clean)
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
		{
			remote_type = pstrdup("coordinator");

		}
		else if (IS_PGXC_DATANODE)
		{
			remote_type = pstrdup("datanode");
		}

		if (client_conn_type == REMOTE_CONN_DATANODE)
			/* initialize it */
			nodePool->connstr = PGXCNodeConnStr(datanode_connInfos[index].host,
												datanode_connInfos[index].port,
												dbPool->database,
												datanode_connInfos[index].uname,
												datanode_connInfos[index].password,
												remote_type);
		else if (client_conn_type == REMOTE_CONN_COORD)
			nodePool->connstr = PGXCNodeConnStr(coord_connInfos[index].host,
												coord_connInfos[index].port,
												dbPool->database,
												coord_connInfos[index].uname,
												coord_connInfos[index].password,
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
			dbPool->dataNodePools[index] = nodePool;
		else if (client_conn_type == REMOTE_CONN_COORD)
			dbPool->coordNodePools[index] = nodePool;
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

	/*
	 * At this point all agents using connections from this pool should be already closed
	 * If this not the connections to the data nodes assigned to them remain open, this will
	 * consume data node resources.
	 * I believe this is not the case because pool is only destroyed on coordinator shutdown.
	 * However we should be careful when changing thinds
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
				if (destroy_database_pool(databasePools->database) == 0)
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
clean_connection(List *dn_discard, List *co_discard, const char *database)
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
	databasePool = find_database_pool(database);

	/* Database pool has not been found */
	if (!databasePool)
		return CLEAN_CONNECTION_NOT_COMPLETED;

	/*
	 * Clean each Pool Correctly
	 * First for Datanode Pool
	 */
	for (count = 0; count < dn_len; count++)
	{
		int node_num = dn_list[count];
		nodePool = databasePool->dataNodePools[node_num - 1];

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
		nodePool = databasePool->coordNodePools[node_num - 1];

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

	/* Release lock on Pooler, to allow transactions to connect again. */
	is_pool_cleaning = false;
	return res;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
int *
abort_pids(int *len, int pid, const char *database)
{
	int *pids = NULL;
	int i = 0;
	int count;

	Assert(!is_pool_cleaning);
	Assert(agentCount > 0);

	is_pool_cleaning = true;

	pids = (int *) palloc((agentCount - 1) * sizeof(int));

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = 0; count < agentCount; count++)
	{
		if (poolAgents[count]->pid != pid &&
			strcmp(poolAgents[count]->pool->database, database) == 0)
		{
			if (kill(poolAgents[count]->pid, SIGTERM) < 0)
				elog(ERROR, "kill(%ld,%d) failed: %m",
							(long) poolAgents[count]->pid, SIGTERM);

			pids[i++] = poolAgents[count]->pid;
		}
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
