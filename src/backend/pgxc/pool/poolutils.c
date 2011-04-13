/*-------------------------------------------------------------------------
 *
 * poolutils.c
 *
 * Utilities for Postgres-XC pooler
 * 
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *    $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"

#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/locator.h"
#include "pgxc/poolutils.h"
#include "access/gtm.h"
#include "commands/dbcommands.h"
#include "utils/lsyscache.h"
#include "utils/acl.h"

#include "nodes/parsenodes.h"

/*
 * CleanConnection()
 *
 * Utility to clean up Postgres-XC Pooler connections.
 * This utility is launched to all the Coordinators of the cluster
 *
 * Use of CLEAN CONNECTION is limited to a super user.
 * It is advised to clean connections before shutting down a Node or drop a Database.
 *
 * SQL query synopsis is as follows:
 * CLEAN CONNECTION TO
 *		(COORDINATOR num | DATANODE num | ALL {FORCE})
 *		[ FOR DATABASE dbname ]
 *		[ TO USER username ]
 *
 * Connection cleaning can be made on a chosen database called dbname
 * or/and a chosen user.
 * Cleaning is done for all the users of a given database
 * if no user name is specified.
 * Cleaning is done for all the databases for one user
 * if no database name is specified.
 *
 * It is also possible to clean connections of several Coordinators or Datanodes
 * Ex:	CLEAN CONNECTION TO DATANODE 1,5,7 FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR 2,4,6 FOR DATABASE template1
 *		CLEAN CONNECTION TO DATANODE 3,5 TO USER postgres
 *		CLEAN CONNECTION TO COORDINATOR 6,1 FOR DATABASE template1 TO USER postgres
 *
 * Or even to all Coordinators/Datanodes at the same time
 * Ex:	CLEAN CONNECTION TO DATANODE * FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR * TO USER postgres
 *		CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1 TO USER postgres
 *
 * When FORCE is used, all the transactions using pooler connections are aborted,
 * and pooler connections are cleaned up.
 * Ex:	CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1;
 *		CLEAN CONNECTION TO ALL FORCE TO USER postgres;
 *		CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1 TO USER postgres;
 *
 * FORCE can only be used with TO ALL, as it takes a lock on pooler to stop requests
 * asking for connections, aborts all the connections in the cluster, and cleans up
 * pool connections associated to the given user and/or database.
 */
void
CleanConnection(CleanConnStmt *stmt)
{
	ListCell   *nodelist_item;
	List	   *co_list = NIL;
	List	   *dn_list = NIL;
	List	   *stmt_nodes = NIL;
	char	   *dbname = stmt->dbname;
	char	   *username = stmt->username;
	bool		is_coord = stmt->is_coord;
	bool		is_force = stmt->is_force;
	int			max_node_number = 0;

	/* Only a DB administrator can clean pooler connections */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to clean pool connections")));

	/* Database name or user name is mandatory */
	if (!dbname && !username)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("must define Database name or user name")));

	/* Check if the Database exists by getting its Oid */
	if (dbname &&
		!OidIsValid(get_database_oid(dbname)))
	{
		ereport(WARNING,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", dbname)));
		return;
	}

	/* Check if role exists */
	if (username &&
		!OidIsValid(get_roleid(username)))
	{
		ereport(WARNING,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", username)));
		return;
	}

	/*
	 * FORCE is activated,
	 * Send a SIGTERM signal to all the processes and take a lock on Pooler
	 * to avoid backends to take new connections when cleaning.
	 * Only Disconnect is allowed.
	 */
	if (is_force)
	{
		int loop = 0;
		int *proc_pids = NULL;
		int num_proc_pids, count;

		num_proc_pids = PoolManagerAbortTransactions(dbname, username, &proc_pids);

		/*
		 * Watch the processes that received a SIGTERM.
		 * At the end of the timestamp loop, processes are considered as not finished
		 * and force the connection cleaning has failed
		 */

		while (num_proc_pids > 0 && loop < TIMEOUT_CLEAN_LOOP)
		{
			for (count = num_proc_pids - 1; count >= 0; count--)
			{
				switch(kill(proc_pids[count],0))
				{
					case 0: /* Termination not done yet */
						break;

					default:
						/* Move tail pid in free space */
						proc_pids[count] = proc_pids[num_proc_pids - 1];
						num_proc_pids--;
						break;
				}
			}
			pg_usleep(1000000);
			loop++;
		}

		if (proc_pids)
			pfree(proc_pids);

		if (loop >= TIMEOUT_CLEAN_LOOP)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("All Transactions have not been aborted")));
	}

	/* Check node list */
	if (stmt->nodes && is_coord)
		max_node_number = NumCoords;
	else
		max_node_number = NumDataNodes;

	foreach(nodelist_item, stmt->nodes)
	{
		int node_num = intVal(lfirst(nodelist_item));
		stmt_nodes = lappend_int(stmt_nodes, node_num);

		if (node_num > max_node_number ||
			node_num < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Node Number %d is incorrect", node_num)));
	}

	/* Build lists to be sent to Pooler Manager */
	if (stmt->nodes && is_coord)
		co_list = stmt_nodes;
	else if (stmt->nodes && !is_coord)
		dn_list = stmt_nodes;
	else
	{
		co_list = GetAllCoordNodes();
		dn_list = GetAllDataNodes();
	}

	/*
	 * If force is launched, send a signal to all the processes
	 * that are in transaction and take a lock.
	 * Get back their process number and watch them locally here.
	 * Process are checked as alive or not with pg_usleep and when all processes are down
	 * go out of the control loop.
	 * If at the end of the loop processes are not down send an error to client.
	 * Then Make a clean with normal pool cleaner.
	 * Always release the lock when calling CLEAN CONNECTION.
	 */

	/* Finish by contacting Pooler Manager */
	PoolManagerCleanConnection(dn_list, co_list, dbname, username);

	/* Clean up memory */
	if (co_list)
		list_free(co_list);
	if (dn_list)
		list_free(dn_list);
}

/*
 * DropDBCleanConnection
 *
 * Clean Connection for given database before dropping it
 * FORCE is not used here
 */
void
DropDBCleanConnection(char *dbname)
{
	List	*co_list = GetAllCoordNodes();
	List	*dn_list = GetAllDataNodes();
	char	query[256];

	/* Check permissions for this database */
	if (!pg_database_ownercheck(get_database_oid(dbname), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   dbname);

	PoolManagerCleanConnection(dn_list, co_list, dbname, NULL);

	/* Clean up memory */
	if (co_list)
		list_free(co_list);
	if (dn_list)
		list_free(dn_list);
}
