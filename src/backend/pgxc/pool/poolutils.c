/*-------------------------------------------------------------------------
 *
 * poolutils.c
 *
 * Utilities for Postgres-XC pooler
 * 
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
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
 *		FOR DATABASE dbname
 *
 * Connection cleaning has to be made on a chosen database called dbname.
 *
 * It is also possible to clean connections of several Coordinators or Datanodes
 * Ex:	CLEAN CONNECTION TO DATANODE 1,5,7 FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR 2,4,6 FOR DATABASE template1
 *
 * Or even to all Coordinators/Datanodes at the same time
 * Ex:	CLEAN CONNECTION TO DATANODE * FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1
 *
 * When FORCE is used, all the transactions using pooler connections are aborted,
 * and pooler connections are cleaned up.
 * Ex:	CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1;
 *
 * FORCE can only be used with TO ALL, as it takes a lock on pooler to stop requests
 * asking for connections, aborts all the connections in the cluster, and cleans up
 * pool connections.
 */
void
CleanConnection(CleanConnStmt *stmt)
{
	ListCell   *nodelist_item;
	List	   *co_list = NIL;
	List	   *dn_list = NIL;
	List	   *stmt_nodes = NIL;
	char	   *dbname = stmt->dbname;
	bool		is_coord = stmt->is_coord;
	bool		is_force = stmt->is_force;
	int			max_node_number = 0;
	Oid			oid;

	/* Only a DB administrator can clean pooler connections */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to clean pool connections")));

	/* Check if the Database exists by getting its Oid */
	oid = get_database_oid(dbname);
	if (!OidIsValid(oid))
	{
		ereport(WARNING,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", dbname)));
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

		num_proc_pids = PoolManagerAbortTransactions(dbname, &proc_pids);

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
	PoolManagerCleanConnection(dn_list, co_list, dbname);

	/* Clean up memory */
	if (co_list)
		list_free(co_list);
	if (dn_list)
		list_free(dn_list);
}
