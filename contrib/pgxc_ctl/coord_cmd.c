/*-------------------------------------------------------------------------
 *
 * coord_cmd.c
 *
 *    Coordinator command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <setjmp.h>
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "pgxc_ctl.h"
#include "do_command.h"
#include "variables.h"
#include "varnames.h"
#include "pgxc_ctl_log.h"
#include "config.h"
#include "do_shell.h"
#include "utils.h"
#include "coord_cmd.h"
#include "gtm_util.h"

static int failover_oneCoordinator(int coordIdx);

/*
 *======================================================================
 *
 * Coordinator staff
 *
 *=====================================================================
 */
/*
 * Initialize coordinator masters -----------------------------------------------------------
 */
int init_coordinator_master_all(void)
{
	elog(NOTICE, "Initialize all the coordinator masters.\n");
	return(init_coordinator_master(aval(VAR_coordNames)));
}

cmd_t *prepare_initCoordinatorMaster(char *nodeName)
{
	cmd_t *cmd, *cmdInitdb, *cmdPgConf, *cmdWalArchDir, *cmdWalArch, *cmdPgHba;
	int jj, kk, gtmPxyIdx;
	char **confFiles = NULL;
	FILE *f;
	char localStdin[MAXPATH+1];
	char *gtmHost, *gtmPort;
	char timestamp[MAXTOKEN+1];

	/* Reset coordinator master directory and run initdb */
	if ((jj = coordIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: Node %s is not a coordinator.\n", nodeName);
		return(NULL);
	}
	if(pingNode(aval(VAR_coordMasterServers)[jj], aval(VAR_coordPorts)[jj]) == 0)
	{
		elog(ERROR, "ERROR: target coordinator master %s is running now.   Skip initilialization.\n",
			 nodeName);
		return(NULL);
	}
	cmd = cmdInitdb = initCmd(aval(VAR_coordMasterServers)[jj]);
	snprintf(newCommand(cmdInitdb), MAXLINE, 
			 "rm -rf %s;"
			 "mkdir -p %s;"
			 "initdb --nodename %s -D %s",
			 aval(VAR_coordMasterDirs)[jj],
			 aval(VAR_coordMasterDirs)[jj],
			 nodeName,
			 aval(VAR_coordMasterDirs)[jj]);

	/* Update postgresql.conf */

	/* coordSpecificExtraConfig */
	gtmPxyIdx = getEffectiveGtmProxyIdxFromServerName(aval(VAR_coordMasterServers)[jj]);
	gtmHost = (gtmPxyIdx > 0) ? aval(VAR_gtmProxyServers)[gtmPxyIdx] : sval(VAR_gtmMasterServer);
	gtmPort = (gtmPxyIdx > 0) ? aval(VAR_gtmProxyPorts)[gtmPxyIdx] : sval(VAR_gtmMasterPort);
	appendCmdEl(cmdInitdb, (cmdPgConf = initCmd(aval(VAR_coordMasterServers)[jj])));
	snprintf(newCommand(cmdPgConf), MAXLINE,
			 "cat >> %s/postgresql.conf", aval(VAR_coordMasterDirs)[jj]);
	if (!is_none(sval(VAR_coordExtraConfig)))
		AddMember(confFiles, sval(VAR_coordExtraConfig));
	if (!is_none(aval(VAR_coordSpecificExtraConfig)[jj]))
		AddMember(confFiles, aval(VAR_coordSpecificExtraConfig)[jj]);
	if ((f = prepareLocalStdin((cmdPgConf->localStdin = Malloc(MAXPATH+1)), MAXPATH, confFiles)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	/* From configuration variables */
	fprintf(f,
			"#===========================================\n"
			"# Added at initialization. %s\n"
			"port = %d\n"
			"pooler_port = %s\n"
			"gtm_host = '%s'\n"
			"gtm_port = %s\n"
			"# End of Additon\n",
			timeStampString(timestamp, MAXTOKEN),
			atoi(aval(VAR_coordPorts)[jj]),
			aval(VAR_poolerPorts)[jj],
			gtmHost, gtmPort);
	fclose(f);
	CleanArray(confFiles);

	/* Log Shipping */

	if (isVarYes(VAR_coordSlave) && !is_none(aval(VAR_coordSlaveServers)[jj]))
	{
		/* Build WAL archive target directory */
		appendCmdEl(cmdInitdb, (cmdWalArchDir = initCmd(aval(VAR_coordSlaveServers)[jj])));
		snprintf(newCommand(cmdWalArchDir), MAXLINE,
				 "rm -rf %s;mkdir -p %s; chmod 0700 %s",
				 aval(VAR_coordArchLogDirs)[jj], aval(VAR_coordArchLogDirs)[jj], 
				 aval(VAR_coordArchLogDirs)[jj]);
		/* Build master's postgresql.conf */
		appendCmdEl(cmdInitdb, (cmdWalArch = initCmd(aval(VAR_coordMasterServers)[jj])));
		if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
		{
			cleanCmd(cmd);
			return(NULL);
		}
		fprintf(f, 
				"#========================================\n"
				"# Addition for log shipping, %s\n"
				"wal_level = hot_standby\n"
				"archive_mode = on\n"
				"archive_command = 'rsync %%p %s@%s:%s/%%f'\n"
				"max_wal_senders = %s\n"
				"# End of Addition\n",
				timeStampString(timestamp, MAXPATH),
				sval(VAR_pgxcUser), aval(VAR_coordSlaveServers)[jj], aval(VAR_coordArchLogDirs)[jj],
				aval(VAR_coordMaxWALSenders)[jj]);
		fclose(f);
		cmdWalArch->localStdin = Strdup(localStdin);
		snprintf(newCommand(cmdWalArch), MAXLINE,
				 "cat >> %s/postgresql.conf",
				 aval(VAR_coordMasterDirs)[jj]);
	}

	/* pg_hba.conf */

	appendCmdEl(cmdInitdb, (cmdPgHba = initCmd(aval(VAR_coordMasterServers)[jj])));
	if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f, 
			"#=================================================\n"
			"# Addition at initialization, %s\n",
			timeStampString(timestamp, MAXTOKEN));
	if (!is_none(sval(VAR_coordExtraPgHba)))
		AddMember(confFiles, sval(VAR_coordExtraPgHba));
	if (!is_none(aval(VAR_coordSpecificExtraPgHba)[jj]))
		AddMember(confFiles, aval(VAR_coordSpecificExtraPgHba)[jj]);
	appendFiles(f, confFiles);
	CleanArray(confFiles);
	for (kk = 0; aval(VAR_coordPgHbaEntries)[kk]; kk++)
	{
		fprintf(f,"host all %s %s trust\n",	sval(VAR_pgxcOwner), aval(VAR_coordPgHbaEntries)[kk]);
		if (isVarYes(VAR_coordSlave))
			if (!is_none(aval(VAR_coordSlaveServers)[jj]))
				fprintf(f, "host replication %s %s trust\n",
						sval(VAR_pgxcOwner), aval(VAR_coordPgHbaEntries)[kk]);
	}
	fprintf(f, "# End of addition\n");
	fclose(f);
	cmdPgHba->localStdin = Strdup(localStdin);
	snprintf(newCommand(cmdPgHba), MAXLINE,
			 "cat >> %s/pg_hba.conf", aval(VAR_coordMasterDirs)[jj]);

	/*
	 * Now prepare statements to create/alter nodes.
	 */
	return(cmd);
}

int init_coordinator_master(char **nodeList)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	/*
	 * Build directory and run initdb
	 */
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(NOTICE, "Initialize coordinator master %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_initCoordinatorMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(NOTICE, "Done.\n");
	return(rc);
}

/*
 * Initialize coordinator slaves ---------------------------------------------------------------
 */
int init_coordinator_slave_all(void)
{
	elog(NOTICE, "Initialize all the coordinator slaves.\n");
	return(init_coordinator_slave(aval(VAR_coordNames)));
}

cmd_t *prepare_initCoordinatorSlave(char *nodeName)
{
	cmd_t *cmd, *cmdBuildDir, *cmdStartMaster, *cmdBaseBkup, *cmdRecoveryConf, *cmdPgConf;
	int idx;
	FILE *f;
	char localStdin[MAXPATH+1];
	char timestamp[MAXTOKEN+1];

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: %s is not a coordinator.\n", nodeName);
		return(NULL);
	}
	if (is_none(aval(VAR_coordSlaveServers)[idx]))
	{
		elog(ERROR, "ERROR: Slave of the coordinator %s is not configured.\n", nodeName);
		return(NULL);
	}

	/* Build work directory */
	cmd = cmdBuildDir = initCmd(aval(VAR_coordSlaveServers)[idx]);
	snprintf(newCommand(cmdBuildDir), MAXLINE,
			 "rm -rf %s;mkdir -p %s;chmod 0700 %s",
			 aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx]);
	/* 
	 * Check if the master is running --> May not need change if we have watchdog.   This case, we need
	 * a master which can handle the request.   So GTM should be running. We can test all of them by
	 * single 'select 1' command.
	 */
	if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) != 0)
	{
		/* Master is not running. Must start it first */
		appendCmdEl(cmdBuildDir, (cmdStartMaster = initCmd(aval(VAR_coordMasterServers)[idx])));
		snprintf(newCommand(cmdStartMaster), MAXLINE,
				 "pg_ctl start -Z coordinator -D %s -o -i",
				 aval(VAR_coordMasterDirs)[idx]);
	}
	/*
	 * Obtain base backup of the master
	 */
	appendCmdEl(cmdBuildDir, (cmdBaseBkup = initCmd(aval(VAR_coordSlaveServers)[idx])));
	snprintf(newCommand(cmdBaseBkup), MAXLINE,
			 "pg_basebackup -p %s -h %s -D %s -x",
			 aval(VAR_coordPorts)[idx], aval(VAR_coordMasterServers)[idx], aval(VAR_coordSlaveDirs)[idx]);

	/* Configure recovery.conf file at the slave */
	appendCmdEl(cmdBuildDir, (cmdRecoveryConf = initCmd(aval(VAR_coordSlaveServers)[idx])));
	if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#==========================================\n"
			"# Added to initialize the slave, %s\n"
			"standby_mode = on\n"
			"primary_conninfo = 'host = %s port = %s "
			"user = %s application_name = %s'\n"
			"restore_command = 'cp %s/%%f %%p'\n"
			"archive_cleanup_command = 'pg_archivecleanup %s %%r'\n"
			"# End of addition\n",
			timeStampString(timestamp, MAXTOKEN), aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx],
			sval(VAR_pgxcOwner), aval(VAR_coordNames)[idx], 
			aval(VAR_coordArchLogDirs)[idx], aval(VAR_coordArchLogDirs)[idx]);
	fclose(f);
	cmdRecoveryConf->localStdin = Strdup(localStdin);
	snprintf(newCommand(cmdRecoveryConf), MAXLINE,
			 "cat >> %s/recovery.conf\n", aval(VAR_coordSlaveDirs)[idx]);

	/* Configure postgresql.conf at the slave */
	appendCmdEl(cmdBuildDir, (cmdPgConf = initCmd(aval(VAR_coordSlaveServers)[idx])));
	if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#==========================================\n"
			"# Added to initialize the slave, %s\n"
			"hot_standby = on\n"
			"port = %s\n",
			timeStampString(timestamp, MAXTOKEN), aval(VAR_coordPorts)[idx]);
	fclose(f);
	cmdPgConf->localStdin = Strdup(localStdin);
	snprintf(newCommand(cmdPgConf), MAXLINE,
			 "cat >> %s/postgresql.conf", aval(VAR_coordSlaveDirs)[idx]);
	return(cmd);
}

		
int init_coordinator_slave(char **nodeList)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	int rc;
	cmd_t *cmd;

	if (!isVarYes(VAR_coordSlave))
	{
		elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
		return(1);
	}
	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	/*
	 * First step: initialize work directory and run the master if necessary
	 */
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Initializa the coordinator slave %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_initCoordinatorSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done.\n");
	return(rc);
}

/*
 * Configure nodes in each coordinator -------------------------------------------
 *
 * Issues CREATE NODE/ALTER NODE through psql.
 *
 * Please note that CREATE/ALTER/DROP NODE are handled only locally.  You have to
 * visit all the coordinators.
 */
int configure_nodes_all(void)
{
	return configure_nodes(aval(VAR_coordNames));
}

int configure_nodes(char **nodeList)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		if ((cmd = prepare_configureNode(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done.\n");
	return(rc);
}

cmd_t *prepare_configureNode(char *nodeName)
{
	cmd_t *cmd;
	int ii;
	int idx;
	FILE *f;

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(ERROR, "ERROR: %s is not a coordinator.\n", nodeName);
		return NULL;
	}
	if (is_none(aval(VAR_coordMasterServers)[idx]))
		return NULL;
	cmd = initCmd(NULL);
	snprintf(newCommand(cmd), MAXLINE,
			 "psql -p %d -h %s -a postgres %s",
			 atoi(aval(VAR_coordPorts)[idx]),
			 aval(VAR_coordMasterServers)[idx],
			 sval(VAR_pgxcOwner));
	if ((f = prepareLocalStdin(newFilename(cmd->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return NULL;
	}
	/* Setup coordinators */
	for (ii = 0; aval(VAR_coordNames)[ii]; ii++)
	{
		int targetIdx;
		if (is_none(aval(VAR_coordNames)[ii]))
			continue;
		if ((targetIdx = coordIdx(aval(VAR_coordNames)[ii])) < 0)
		{
			elog(ERROR, "ERROR: internal error.  Could not get coordinator idex for %s\n", aval(VAR_coordNames)[ii]);
			continue;
		}
		if (!is_none(aval(VAR_coordMasterServers)[ii]))
		{
			if (idx != targetIdx)
				/* Register outside coordinator */
				fprintf(f, "CREATE NODE %s WITH (TYPE='coordinator', HOST='%s', PORT=%d);\n",
						aval(VAR_coordNames)[ii], 
						aval(VAR_coordMasterServers)[ii],
						atoi(aval(VAR_coordPorts)[ii]));
			else
				/* Update myself */
				fprintf(f, "ALTER NODE %s WITH (HOST='%s', PORT=%d);\n",
						aval(VAR_coordNames)[ii],
						aval(VAR_coordMasterServers)[ii],
						atoi(aval(VAR_coordPorts)[ii]));
		}
	}
	/* Setup datanodes */
	for (ii = 0; aval(VAR_datanodeNames)[ii]; ii++)
	{
		int dnIdx;

		if ((dnIdx = datanodeIdx(aval(VAR_datanodeNames)[ii])) < 0)
		{
			elog(ERROR, "ERROR: inernal error. Could not get datanode index for %s.\n", aval(VAR_datanodeNames)[ii]);
			fclose(f);
			cleanCmd(cmd);
			return NULL;
		}
		if (is_none(aval(VAR_datanodeNames)[ii]) || is_none(aval(VAR_datanodeMasterServers)[dnIdx]))
			continue;
		if (sval(VAR_primaryDatanode) && (strcmp(sval(VAR_primaryDatanode), aval(VAR_datanodeNames)[dnIdx]) == 0))
		{
			/* Primary Node */
			if (strcmp(aval(VAR_coordMasterServers)[idx], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
				/* Primay and preferred node */
				fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY, PREFERRED);\n",
						aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
						atoi(aval(VAR_datanodePorts)[dnIdx]));
			else
				/* Primary but not prefereed node */
				fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY);\n",
						aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
						atoi(aval(VAR_datanodePorts)[dnIdx]));
		}
		else
		{
			/* Non-primary node */
			if (strcmp(aval(VAR_coordMasterServers)[idx], aval(VAR_datanodeMasterServers)[dnIdx]) == 0)
				/* Preferred node */
				fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PREFERRED);\n",
						aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
						atoi(aval(VAR_datanodePorts)[dnIdx]));
			else
				/* non-Preferred node */
				fprintf(f, "CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d);\n",
						aval(VAR_datanodeNames)[dnIdx], aval(VAR_datanodeMasterServers)[dnIdx],
						atoi(aval(VAR_datanodePorts)[dnIdx]));
		}
	}
	fclose(f);
	return(cmd);
}	


/*
 * Kill coordinator masters -------------------------------------------------------------
 *
 * It is not recommended to kill them in such a manner.   This is just for emergence.
 * You should try to stop component by "stop" command.
 */

int kill_coordinator_master_all(void)
{
	elog(INFO, "Killing all the coordinator masters.\n");
	return(kill_coordinator_master(aval(VAR_coordNames)));
}

cmd_t * prepare_killCoordinatorMaster(char *nodeName)
{
	int idx;
	pid_t pmPid;
	cmd_t *cmdKill = NULL, *cmd = NULL;

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: node %s is not a coordinator.\n", nodeName);
		return(NULL);
	}
	cmd = cmdKill = initCmd(aval(VAR_coordMasterServers)[idx]);
	if ((pmPid = get_postmaster_pid(aval(VAR_coordMasterServers)[idx], aval(VAR_coordMasterDirs)[idx])) > 0)
	{
		char *pidList = getChPidList(aval(VAR_coordMasterServers)[idx], pmPid);

		snprintf(newCommand(cmdKill), MAXLINE,
				 "kill -9 %d %s; rm -f /tmp/.s.'*'%d'*'",
				 pmPid, pidList, atoi(aval(VAR_coordPorts)[idx]));
		freeAndReset(pidList);
	}
	else
	{
		snprintf(newCommand(cmdKill), MAXLINE,
				 "killall -u %s -9 postgres; rm -f /tmp/.s.'*'%d'*'",
				 sval(VAR_pgxcUser), atoi(aval(VAR_coordPorts)[idx]));
	}
	return cmd;
}

int kill_coordinator_master(char **nodeList)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Killing coordinator master %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_killCoordinatorMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	return(rc);
}

/*
 * Kill coordinator masters -------------------------------------------------------------
 *
 * It is not recommended to kill them in such a manner.   This is just for emergence.
 * You should try to stop component by "stop" command.
 */
int kill_coordinator_slave_all(void)
{
	elog(INFO, "Killing all the cooridinator slaves.\n");
	return(kill_coordinator_slave(aval(VAR_coordNames)));
}

cmd_t *prepare_killCoordinatorSlave(char *nodeName)
{
	int idx;
	pid_t pmPid;
	cmd_t *cmd = NULL, *cmdKill = NULL, *cmdRmSock;

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: %s is not a coordinator.\n", nodeName);
		return(NULL);
	}
	if ((pmPid = get_postmaster_pid(aval(VAR_coordSlaveServers)[idx], aval(VAR_coordSlaveDirs)[idx])) > 0)
	{
		char *pidList = getChPidList(aval(VAR_coordSlaveServers)[idx], pmPid);

		cmd = cmdKill = initCmd(aval(VAR_coordSlaveServers)[idx]);
		snprintf(newCommand(cmdKill), MAXLINE,
				 "kill -9 %d %s", 
				 pmPid, pidList);
		freeAndReset(pidList);
	}
	if (cmd == NULL)
		cmd = cmdRmSock = initCmd(aval(VAR_coordSlaveServers)[idx]);
	else
		appendCmdEl(cmd, (cmdRmSock = initCmd(aval(VAR_coordSlaveServers)[idx])));
	snprintf(newCommand(cmdRmSock), MAXLINE,
			 "rm -f /tmp/.s.'*'%d'*'", atoi(aval(VAR_coordPorts)[idx]));
	return(cmd);
}

int kill_coordinator_slave(char **nodeList)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	if (!isVarYes(VAR_coordSlave))
	{
		elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
		return(1);
	}
	cmdList = initCmdList();
	actualNodeList = makeActualNodeList(nodeList);
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Killing coordinatlr slave %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_killCoordinatorSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done.\n");
	return (rc);
}

cmd_t *prepare_cleanCoordinatorMaster(char *nodeName)
{
	cmd_t *cmd;
	int idx;

	if ((idx = coordIdx(nodeName)) < 0)
		return NULL;
	if (is_none(aval(VAR_coordMasterServers)[idx]))
		return NULL;
	cmd = initCmd(aval(VAR_coordMasterServers)[idx]);
	snprintf(newCommand(cmd), MAXLINE,
			 "rm -rf %s;mkdir -p %s;chmod 0700 %s; rm -f /tmp/.s.*%d*; rm -f /tmp/.s.*%d*",
			 aval(VAR_coordMasterDirs)[idx], aval(VAR_coordMasterDirs)[idx], aval(VAR_coordMasterDirs)[idx],
			 atoi(aval(VAR_coordPorts)[idx]), atoi(aval(VAR_poolerPorts)[idx]));
	return cmd;
}

/*
 * Cleanup coordinator master resources -- directory and socket.
 */
int clean_coordinator_master(char **nodeList)
{
	char **actualNodeList;
	cmdList_t *cmdList;
	int ii;
	cmd_t *cmd;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Clean coordinator master %s resources.\n", actualNodeList[ii]);
		if ((cmd = prepare_cleanCoordinatorMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "WARNING: coordinator master %s not found.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done.\n");
	return (rc);
}

int clean_coordinator_master_all(void)
{
	elog(INFO, "Cleaning all the coordinator masters resources.\n");
	return(clean_coordinator_master(aval(VAR_coordNames)));
}

/*
 * Cleanup coordinator slave resources -- directory and the socket.
 */
cmd_t *prepare_cleanCoordinatorSlave(char *nodeName)
{
	cmd_t *cmd;
	int idx;

	if ((idx = coordIdx(nodeName)) < 0)
		return NULL;
	if (is_none(aval(VAR_coordSlaveServers)[idx]))
		return NULL;
	cmd = initCmd(aval(VAR_coordMasterServers)[idx]);
	snprintf(newCommand(cmd), MAXLINE,
			 "rm -rf %s;mkdir -p %s;chmod 0700 %s; rm -f /tmp/.s.*%d*; rm -f /tmp/.s.*%d*",
			 aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordSlaveDirs)[idx],
			 atoi(aval(VAR_coordPorts)[idx]), atoi(aval(VAR_poolerPorts)[idx]));
	return cmd;
}

int clean_coordinator_slave(char **nodeList)
{
	char **actualNodeList;
	cmdList_t *cmdList;
	int ii;
	cmd_t *cmd;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Clean coordinator slave %s resources.\n", actualNodeList[ii]);
		if ((cmd = prepare_cleanCoordinatorSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
		else
			elog(WARNING, "WARNING: coordinator slave %s not found.\n", actualNodeList[ii]);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done.\n");
	return (rc);
}

int clean_coordinator_slave_all(void)
{
	elog(INFO, "Cleaning all the cooridnator slaves resources.\n");
	return(clean_coordinator_slave(aval(VAR_coordNames)));
}

/*
 * Start coordinator master ---------------------------------------------
 */
int start_coordinator_master_all(void)
{
	elog(INFO, "Starting coordinator master.\n");
	return(start_coordinator_master(aval(VAR_coordNames)));
}

cmd_t *prepare_startCoordinatorMaster(char *nodeName)
{
	cmd_t *cmd = NULL, *cmdPgCtl;
	int idx;

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: %s is not a coordinator, skipping.\n", nodeName);
		return(NULL);
	}
	/*
	 * Check if the coordinator is running
	 */
	if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) == 0)
	{
		elog(ERROR, "ERROR: target coordinator master %s is already running now.   Skip initilialization.\n",
			 nodeName);
		return(NULL);
	}
	cmd = cmdPgCtl = initCmd(aval(VAR_coordMasterServers)[idx]);
	snprintf(newCommand(cmdPgCtl), MAXLINE,
			 "pg_ctl start -Z coordinator -D %s -o -i",
			 aval(VAR_coordMasterDirs)[idx]);
	return(cmd);
}

int start_coordinator_master(char **nodeList)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Starting coordinator master %s\n", actualNodeList[ii]);
		if ((cmd = prepare_startCoordinatorMaster(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done.\n");
	return(rc);
}

/*
 * Start coordinator slaves ----------------------------------------
 */
int start_coordinator_slave_all(void)
{
	elog(INFO, "Starting all the coordinator slaves.\n");
	return(start_coordinator_slave(aval(VAR_coordNames)));
}

cmd_t *prepare_startCoordinatorSlave(char *nodeName)
{
	int idx;
	FILE *f;
	char timestamp[MAXTOKEN+1];
	cmd_t *cmd = NULL, *cmdPgCtlStart, *cmdPgConfMaster, *cmdMasterReload;

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: %s is not a coordinator, skipping.\n", nodeName);
		return(NULL);
	}
	/*
	 * Check if the coordinator is running
	 */
	if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) != 0)
	{
		elog(ERROR, "ERROR: Coordinator Master %s is not runnig now. Cannot start the slave.\n",
			 aval(VAR_coordNames)[idx]);
		return(NULL);
	}
	cmd = cmdPgCtlStart = initCmd(aval(VAR_coordSlaveServers)[idx]);
	snprintf(newCommand(cmdPgCtlStart), MAXLINE,
			 "pg_ctl start -Z coordinator -D %s -o -i",
			 aval(VAR_coordSlaveDirs)[idx]);

	/* Postgresql.conf at the Master */

	appendCmdEl(cmdPgCtlStart, (cmdPgConfMaster = initCmd(aval(VAR_coordMasterServers)[idx])));
	snprintf(newCommand(cmdPgConfMaster), MAXLINE,
			 "cat >> %s/postgresql.conf", aval(VAR_coordMasterDirs)[idx]);
	if ((f = prepareLocalStdin(newFilename(cmdPgConfMaster->localStdin), MAXPATH, NULL)) == NULL)
	{
		cleanCmd(cmd);
		return(NULL);
	}
	fprintf(f,
			"#==========================================================\n"
			"# Added to start the slave in sync. mode, %s\n"
			"synchronous_commit = on\n"
			"synchronous_standby_names = '%s'\n"
			"# End of the addition\n",
			timeStampString(timestamp, MAXTOKEN),
			aval(VAR_coordNames)[idx]);
	fclose(f);
		
	/* Reloae postgresql.conf change */
	appendCmdEl(cmdPgCtlStart, (cmdMasterReload = initCmd(aval(VAR_coordMasterServers)[idx])));
	snprintf(newCommand(cmdMasterReload), MAXLINE,
			 "pg_ctl reload -Z coordinator -D %s",
			 aval(VAR_coordMasterDirs)[idx]);
	return(cmd);
}

int start_coordinator_slave(char **nodeList)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	if (!isVarYes(VAR_coordSlave))
	{
		elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
		return(1);
	}
	cmdList = initCmdList();
	actualNodeList = makeActualNodeList(nodeList);
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Starting coordinator slave %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_startCoordinatorSlave(actualNodeList[ii])))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done\n");
	return(rc);
}

/*
 * Stop coordinator masters ---------------------------------------------------
 */
/* Does not check if immediate is valid here */
int stop_coordinator_master_all(char *immediate)
{
	elog(INFO, "Stopping all the coordinator masters.\n");
	return(stop_coordinator_master(aval(VAR_coordNames), immediate));
}

cmd_t *prepare_stopCoordinatorMaster(char *nodeName, char *immediate)
{
	int idx;
	cmd_t *cmd;

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: %s is not a coordinator.\n", nodeName);
		return(NULL);
	}
	cmd = initCmd(aval(VAR_coordMasterServers)[idx]);
	if (immediate)
		snprintf(newCommand(cmd), MAXLINE,
				 "pg_ctl stop -Z coordinator -D %s -m %s",
				 aval(VAR_coordMasterDirs)[idx], immediate);
	else
		snprintf(newCommand(cmd), MAXLINE,
				 "pg_ctl stop -Z coordinator -D %s",
				 aval(VAR_coordMasterDirs)[idx]);
	return(cmd);
}
			 

/* Does not check if immediate is valid here. */
int stop_coordinator_master(char **nodeList, char *immediate)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	int rc;

	if (immediate == NULL)
		immediate = FAST;
	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		cmd_t *cmd;
		elog(INFO, "Stopping coordinator master %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_stopCoordinatorMaster(actualNodeList[ii], immediate)))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	elog(INFO, "Done.\n");
	return(rc);
}


/*
 * Stop coordinator slaves ----------------------------------------------------
 */
int stop_coordinator_slave_all(char *immediate)
{
	elog(INFO, "Stopping all the coordinator slaves.\n");
	return(stop_coordinator_slave(aval(VAR_coordNames), immediate));
}

cmd_t *prepare_stopCoordinatorSlave(char *nodeName, char *immediate)
{
	int idx;
	cmd_t *cmd = NULL, *cmdMasterReload, *cmdPgCtlStop;
	FILE *f;
	char localStdin[MAXPATH+1];
	char timestamp[MAXTOKEN+1];

	if ((idx = coordIdx(nodeName)) < 0)
	{
		elog(WARNING, "WARNING: %s is not a coordinator.\n", nodeName);
		return(NULL);
	}
	if (pingNode(aval(VAR_coordMasterServers)[idx], aval(VAR_coordPorts)[idx]) == 0)
	{
		/* Master is running.  Need to switch log shipping to asynchronous mode. */
		cmd = cmdMasterReload = initCmd(aval(VAR_coordMasterServers)[idx]);
		if ((f = prepareLocalStdin(localStdin, MAXPATH, NULL)) == NULL)
		{
			cleanCmd(cmd);
			return(NULL);
		}
		fprintf(f,
				"#=======================================\n"
				"# Updated to trun off the slave %s\n"
				"synchronous_standby_names = ''\n"
				"# End of the update\n",
				timeStampString(timestamp, MAXTOKEN));
		fclose(f);
		snprintf(newCommand(cmdMasterReload), MAXLINE,
				 "cat >> %s/postgresql.conf",
				 aval(VAR_coordMasterDirs)[idx]);
		cmdMasterReload->localStdin = Strdup(localStdin);
	}
	if (cmd)
		appendCmdEl(cmdMasterReload, (cmdPgCtlStop = initCmd(aval(VAR_coordSlaveServers)[idx])));
	else
		cmd = cmdPgCtlStop = initCmd(aval(VAR_coordSlaveServers)[idx]);
	if (immediate)
		snprintf(newCommand(cmdPgCtlStop), MAXLINE,
				 "pg_ctl stop -Z coordinator -D %s -m %s",
				 aval(VAR_coordSlaveDirs)[idx], immediate);
	else
		snprintf(newCommand(cmdPgCtlStop), MAXLINE,
				 "pg_ctl stop -Z coordinator -D %s",
				 aval(VAR_coordSlaveDirs)[idx]);
	return(cmd);
}


int stop_coordinator_slave(char **nodeList, char *immediate)
{
	char **actualNodeList;
	int ii;
	cmdList_t *cmdList;
	cmd_t *cmd;
	int rc;

	if (!isVarYes(VAR_coordSlave))
	{
		elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
		return(1);
	}
	if (immediate == NULL)
		immediate = "fast";
	actualNodeList = makeActualNodeList(nodeList);
	cmdList = initCmdList();
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		elog(INFO, "Stopping the coordinator slave %s.\n", actualNodeList[ii]);
		if ((cmd = prepare_stopCoordinatorSlave(actualNodeList[ii], immediate)))
			addCmd(cmdList, cmd);
	}
	rc = doCmdList(cmdList);
	cleanCmdList(cmdList);
	CleanArray(actualNodeList);
	return(rc);
}

/*
 * Failover coordinator ---------------------------------------------------------
 */
int failover_coordinator(char **nodeList)
{
	char **actualNodeList;
	int ii;
	int rc = 0;

	elog(INFO, "Failover coordiantors.\n");
	if (!isVarYes(VAR_coordSlave))
	{
		elog(ERROR, "ERROR: Coordinator slaves are not configured.\n");
		return(2);
	}
	actualNodeList = makeActualNodeList(nodeList);
	for (ii = 0; actualNodeList[ii]; ii++)
	{
		int idx;
		int rc_local;

		elog(INFO, "Failover the coordinator %s.\n", actualNodeList[ii]);
		if ((idx = coordIdx(actualNodeList[ii])) < 0)
		{
			elog(ERROR, "ERROR: %s is not a coordinator. Skipping.\n", actualNodeList[ii]);
			continue;
		}
		if (is_none(aval(VAR_coordSlaveServers)[idx]))
		{
			elog(ERROR, "ERROR: slave of the coordinator %s is not configured. Skipping\n",
				 actualNodeList[ii]);
			continue;
		}
		rc_local = failover_oneCoordinator(idx);
		if (rc_local < 0)
			return(rc_local);
		else
			if (rc_local > rc)
				rc = rc_local;
	}
	elog(INFO, "Done.\n");
	return(rc);
}

static int failover_oneCoordinator(int coordIdx)
{
	int	rc = 0;
	int rc_local;
	int jj;
	int	gtmPxyIdx;
	char *gtmHost;
	char *gtmPort;
	FILE *f;
	char timestamp[MAXTOKEN+1];
	
#define checkRc() do{if(WEXITSTATUS(rc_local) > rc) rc = WEXITSTATUS(rc_local);}while(0)

	/*
	 * Determine the target gtm
	 */
	gtmPxyIdx= getEffectiveGtmProxyIdxFromServerName(aval(VAR_coordSlaveServers)[coordIdx]);
	gtmHost = (gtmPxyIdx < 0) ? sval(VAR_gtmMasterServer) : aval(VAR_gtmProxyServers)[coordIdx];
	gtmPort = (gtmPxyIdx < 0) ? sval(VAR_gtmMasterPort) : aval(VAR_gtmProxyPorts)[coordIdx];
	if (gtmPxyIdx >= 0)
		elog(NOTICE, "Failover coordinator %s using gtm %s\n",
			 aval(VAR_coordNames)[coordIdx], aval(VAR_gtmProxyNames)[gtmPxyIdx]);
	else
		elog(NOTICE, "Filover coordinator %s using GTM itself\n",
			 aval(VAR_coordNames)[coordIdx]);
		
	/* Unregister the coordinator from GTM */
	unregister_coordinator(aval(VAR_coordNames)[coordIdx]);

	/* Promote the slave */
	rc_local = doImmediate(aval(VAR_coordSlaveServers)[coordIdx], NULL,
						   "pg_ctl promote -Z coordinator -D %s",
						   aval(VAR_coordSlaveDirs)[coordIdx]);
	checkRc();

	/* Reconfigure new coordinator master with new gtm_proxy or gtm */

	if ((f =  pgxc_popen_w(aval(VAR_coordSlaveServers)[coordIdx],
						   "cat >> %s/postgresql.conf",
						   aval(VAR_coordSlaveDirs)[coordIdx])) == NULL)
	{
		elog(ERROR, "ERROR: Could not prepare to update postgresql.conf, %s", strerror(errno));
		return(-1);
	}
	fprintf(f,
			"#=================================================\n"
			"# Added to promote, %s\n"
			"gtm_host = '%s'\n"
			"gtm_port = %s\n"
			"# End of addition\n",
			timeStampString(timestamp, MAXTOKEN),
			gtmHost, gtmPort);
	fclose(f);

	/* Restart coord Slave Server */
	rc_local = doImmediate(aval(VAR_coordSlaveServers)[coordIdx], NULL,
						   "pg_ctl restart -Z coordinator -D %s -w -o -i; sleep 1",
						   aval(VAR_coordSlaveDirs)[coordIdx]);
	checkRc();
	
	/* Update the configuration variable */
	var_assign(&(aval(VAR_coordMasterServers)[coordIdx]), Strdup(aval(VAR_coordSlaveServers)[coordIdx]));
	var_assign(&(aval(VAR_coordSlaveServers)[coordIdx]), Strdup("none"));
	var_assign(&(aval(VAR_coordMasterDirs)[coordIdx]), Strdup(aval(VAR_coordSlaveDirs)[coordIdx]));
	var_assign(&(aval(VAR_coordSlaveDirs)[coordIdx]), Strdup("none"));

	if ((f = fopen(pgxc_ctl_config_path, "a")) == NULL)
	{
		elog(ERROR, "ERROR: Failed to open configuration file %s, %s\n", pgxc_ctl_config_path, strerror(errno));
		return(-1);
	}
	fprintf(f,
			"#=====================================================\n"
			"# Updated due to the coordinator failover, %s, %s\n"
			"coordMasterServers=( %s )\n"
			"coordMasterDirs=( %s )\n"
			"coordSlaveServers=( %s )\n"
			"coordSlaveDirs=( %s )\n"
			"# End of the update\n",
			aval(VAR_coordNames)[coordIdx], timeStampString(timestamp, MAXTOKEN),
			listValue(VAR_coordMasterServers),
			listValue(VAR_coordMasterDirs),
			listValue(VAR_coordSlaveServers),
			listValue(VAR_coordSlaveDirs));
	fclose(f);

	/* Backup the configuration file */
	if (isVarYes(VAR_configBackup))
	{
		rc_local = doConfigBackup();
		checkRc();
	}

	/*
	 * Reconfigure coordinators with new coordinator
	 */
	for (jj = 0; aval(VAR_coordNames)[jj]; jj++)
	{
		if (is_none(aval(VAR_coordMasterServers)[jj]))
			continue;
			
		if (pingNode(aval(VAR_coordMasterServers)[jj], aval(VAR_coordPorts)[jj]) != 0)
		{
			elog(ERROR, "Coordinator %s is not running.  Skip reconfiguration for this coordinator.\n",
				 aval(VAR_coordNames)[jj]);
			continue;
		}
		if ((f = pgxc_popen_wRaw("psql -p %s -h %s postgres %s",
								 aval(VAR_coordPorts)[jj],
								 aval(VAR_coordMasterServers)[jj],
								 sval(VAR_pgxcOwner)))
			== NULL)
		{
			elog(ERROR, "ERROR: failed to start psql for coordinator %s, %s\n", aval(VAR_coordNames)[jj], strerror(errno));
			continue;
		}
		fprintf(f,
				"ALTER NODE %s WITH (HOST='%s', PORT=%s);\n"
				"select pgxc_pool_reload();\n"
				"\\q\n",
				aval(VAR_coordNames)[coordIdx], aval(VAR_coordMasterServers)[coordIdx], aval(VAR_coordPorts)[coordIdx]);
		fclose(f);
	}
	return(rc);

#	undef checkRc
}

/*
 * Show coordinator configuration
 */
int show_config_coordMasterSlaveMulti(char **nodeList)
{
	int ii;
	int idx;

	lockLogFile();
	for (ii = 0; nodeList[ii]; ii++)
	{
		if ((idx = coordIdx(nodeList[ii])) < 0)
			continue;
		else
		{
			show_config_coordMaster(TRUE, idx, aval(VAR_coordMasterServers)[idx]);
			if (isVarYes(VAR_coordSlave))
				show_config_coordSlave(TRUE, idx, aval(VAR_coordSlaveServers)[idx]);
		}
	}
	unlockLogFile();
	return 0;
}

int show_config_coordMasterMulti(char **nodeList)
{
	int ii;
	int idx;

	lockLogFile();
	for (ii = 0; nodeList[ii]; ii++)
	{
		if ((idx = coordIdx(nodeList[ii])) < 0)
			continue;
		else
			show_config_coordMaster(TRUE, idx, aval(VAR_coordMasterServers)[idx]);
	}
	unlockLogFile();
	return 0;
}

int show_config_coordSlaveMulti(char **nodeList)
{
	int ii;
	int idx;

	if (!isVarYes(VAR_coordSlave))
		return(1);
	lockLogFile();
	for (ii = 0; nodeList[ii]; ii++)
	{
		if ((idx = coordIdx(nodeList[ii])) < 0)
			continue;
		else
			show_config_coordSlave(TRUE, idx, aval(VAR_coordSlaveServers)[idx]);
	}
	unlockLogFile();
	return 0;
}

int show_config_coordMaster(int flag, int idx, char *hostname)
{
	int ii;
	char outBuf[MAXLINE+1];
	char editBuf[MAXPATH+1];

	outBuf[0] = 0;
	if (flag)
		strncat(outBuf, "Coordinator Master: ", MAXLINE);
	if (hostname)
	{
		snprintf(editBuf, MAXPATH, "host: %s", hostname);
		strncat(outBuf, editBuf, MAXLINE);
	}
	if (flag || hostname)
		strncat(outBuf, "\n", MAXLINE);
	lockLogFile();
	if (outBuf[0])
		elog(NOTICE, "%s", outBuf);
	elog(NOTICE, "    Nodename: '%s', port: %s, pooler port: %s\n", 
		 aval(VAR_coordNames)[idx], aval(VAR_coordPorts)[idx], aval(VAR_poolerPorts)[idx]);
	elog(NOTICE, "    MaxWalSenders: %s, Dir: '%s'\n", 
		 aval(VAR_coordMaxWALSenders)[idx], aval(VAR_coordMasterDirs)[idx]);
	elog(NOTICE, "    ExtraConfig: '%s', Specific Extra Config: '%s'\n",
		 sval(VAR_coordExtraConfig), aval(VAR_coordSpecificExtraConfig)[idx]);
	strncpy(outBuf, "    pg_hba entries ( ", MAXLINE);
	for (ii = 0; aval(VAR_coordPgHbaEntries)[ii]; ii++)
	{
		snprintf(editBuf, MAXPATH, "'%s' ", aval(VAR_coordPgHbaEntries)[ii]);
		strncat(outBuf, editBuf, MAXLINE);
	}
	elog(NOTICE, "%s)\n", outBuf);
	elog(NOTICE, "    Extra pg_hba: '%s', Specific Extra pg_hba: '%s'\n",
				sval(VAR_coordExtraPgHba), aval(VAR_coordSpecificExtraPgHba)[idx]);
	unlockLogFile();
	return 0;
}

int show_config_coordSlave(int flag, int idx, char *hostname)
{
	char outBuf[MAXLINE+1];
	char editBuf[MAXPATH+1];

	outBuf[0] = 0;
	if (flag)
		strncat(outBuf, "Coordinator Slave: ", MAXLINE);
	if (hostname)
	{
		snprintf(editBuf, MAXPATH,  "host: %s", hostname);
		strncat(outBuf, editBuf, MAXLINE);
	}
	if (flag || hostname)
		strncat(outBuf, "\n", MAXLINE);
	lockLogFile();
	if (outBuf[0])
		elog(NOTICE, "%s", outBuf);
	elog(NOTICE,"    Nodename: '%s', port: %s, pooler port: %s\n",
		 aval(VAR_coordNames)[idx], aval(VAR_coordPorts)[idx], aval(VAR_poolerPorts)[idx]);
	elog(NOTICE, "    Dir: '%s', Archive Log Dir: '%s'\n",
		 aval(VAR_coordSlaveDirs)[idx], aval(VAR_coordArchLogDirs)[idx]);
	unlockLogFile();
	return 0;
}



