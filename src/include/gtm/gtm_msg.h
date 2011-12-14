/*-------------------------------------------------------------------------
 *
 * gtm_msg.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_MSG_H
#define GTM_MSG_H

typedef enum GTM_MessageType
{
	MSG_TYPE_INVALID,
	MSG_NODE_REGISTER,		/* Register a PGXC Node with GTM */
	MSG_NODE_UNREGISTER,	/* Unregister a PGXC Node with GTM */
	MSG_NODE_LIST,
	MSG_NODE_BEGIN_REPLICATION_INIT,
	MSG_NODE_END_REPLICATION_INIT,
	MSG_BEGIN_BACKUP,		/* Start backup by Standby */
	MSG_END_BACKUP,			/* End backup preparation by Standby */
	MSG_TXN_BEGIN,			/* Start a new transaction */
	MSG_TXN_BEGIN_GETGXID,	/* Start a new transaction and get GXID */
	MSG_TXN_BEGIN_GETGXID_MULTI,	/* Start multiple new transactions and get GXIDs */
	MSG_TXN_START_PREPARED,		/* Begins to prepare a transation for commit */
	MSG_TXN_COMMIT,			/* Commit a running or prepared transaction */
	MSG_TXN_COMMIT_MULTI,	/* Commit multiple running or prepared transactions */
	MSG_TXN_COMMIT_PREPARED,	/* Commit a prepared transaction */
	MSG_TXN_PREPARE,		/* Finish preparing a transaction */
	MSG_TXN_ROLLBACK,		/* Rollback a transaction */
	MSG_TXN_ROLLBACK_MULTI,	/* Rollback multiple transactions */
	MSG_TXN_GET_GID_DATA,	/* Get info associated with a GID, and get a GXID */
	MSG_TXN_GET_GXID,		/* Get a GXID for a transaction */
	MSG_TXN_GET_NEXT_GXID,		/* Get next GXID */
	MSG_TXN_GXID_LIST,
	MSG_SNAPSHOT_GET,		/* Get a global snapshot */
	MSG_SNAPSHOT_GET_MULTI,	/* Get multiple global snapshots */
	MSG_SNAPSHOT_GXID_GET,	/* Get GXID and snapshot together */
	MSG_SEQUENCE_INIT,		/* Initialize a new global sequence */
	MSG_SEQUENCE_GET_CURRENT,/* Get the current value of sequence */
	MSG_SEQUENCE_GET_NEXT,	/* Get the next sequence value of sequence */
	MSG_SEQUENCE_GET_LAST,	/* Get the last sequence value of sequence */
	MSG_SEQUENCE_SET_VAL,	/* Set values for sequence */
	MSG_SEQUENCE_RESET,		/* Reset the sequence */
	MSG_SEQUENCE_CLOSE,		/* Close a previously inited sequence */
	MSG_SEQUENCE_RENAME,	/* Rename a sequence */
	MSG_SEQUENCE_ALTER,		/* Alter a sequence */
	MSG_SEQUENCE_LIST,		/* Get a list of sequences */
	MSG_TXN_GET_STATUS,		/* Get status of a given transaction */
	MSG_TXN_GET_ALL_PREPARED,	/* Get information about all outstanding
								 * prepared transactions */
	MSG_TXN_BEGIN_GETGXID_AUTOVACUUM,	/* Start a new transaction and get GXID for autovacuum */
	MSG_DATA_FLUSH,					/* flush pending data */
	MSG_BACKEND_DISCONNECT,			/* tell GTM that the backend diconnected from the proxy */

	/*
	 * Must be at the end
	 */
	MSG_TYPE_COUNT			/* A dummmy entry just to count the message types */
} GTM_MessageType;

typedef enum GTM_ResultType
{
	NODE_REGISTER_RESULT,
	NODE_UNREGISTER_RESULT,
	NODE_LIST_RESULT,
	NODE_BEGIN_REPLICATION_INIT_RESULT,
	NODE_END_REPLICATION_INIT_RESULT,
	BEGIN_BACKUP_RESULT,
	END_BACKUP_RESULT,
	TXN_BEGIN_RESULT,
	TXN_BEGIN_GETGXID_RESULT,
	TXN_BEGIN_GETGXID_MULTI_RESULT,
	TXN_PREPARE_RESULT,
	TXN_START_PREPARED_RESULT,
	TXN_COMMIT_PREPARED_RESULT,
	TXN_COMMIT_RESULT,
	TXN_COMMIT_MULTI_RESULT,
	TXN_ROLLBACK_RESULT,
	TXN_ROLLBACK_MULTI_RESULT,
	TXN_GET_GID_DATA_RESULT,
	TXN_GET_GXID_RESULT,
	TXN_GET_NEXT_GXID_RESULT,
	TXN_GXID_LIST_RESULT,
	SNAPSHOT_GET_RESULT,
	SNAPSHOT_GET_MULTI_RESULT,
	SNAPSHOT_GXID_GET_RESULT,
	SEQUENCE_INIT_RESULT,
	SEQUENCE_GET_CURRENT_RESULT,
	SEQUENCE_GET_NEXT_RESULT,
	SEQUENCE_GET_LAST_RESULT,
	SEQUENCE_SET_VAL_RESULT,
	SEQUENCE_RESET_RESULT,
	SEQUENCE_CLOSE_RESULT,
	SEQUENCE_RENAME_RESULT,
	SEQUENCE_ALTER_RESULT,
	SEQUENCE_LIST_RESULT,
	TXN_GET_STATUS_RESULT,
	TXN_GET_ALL_PREPARED_RESULT,
	TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT,
} GTM_ResultType;

/*
 * Special message header for the messgaes exchanged between the GTM server and
 * the proxy.
 *
 * ph_conid: connection identifier which is used to route 
 * the messages to the right backend.
 */
typedef struct GTM_ProxyMsgHeader
{
	GTMProxy_ConnID	ph_conid;
} GTM_ProxyMsgHeader;

#endif
