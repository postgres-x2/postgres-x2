/*-------------------------------------------------------------------------
 *
 * gtm_client.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_CLIENT_H
#define GTM_CLIENT_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_msg.h"
#include "gtm/libpq-fe.h"

typedef union GTM_ResultData
{
	GTM_TransactionHandle	grd_txnhandle;	/* TXN_BEGIN */

	struct
	{
		GlobalTransactionId		gxid;
		GTM_Timestamp			timestamp;
	} grd_gxid_tp;							/* TXN_BEGIN_GETGXID */

	GlobalTransactionId		grd_gxid;		/* TXN_PREPARE
											 * TXN_COMMIT
											 * TXN_ROLLBACK
											 */
	
	struct
	{
		GTM_TransactionHandle	txnhandle;
		GlobalTransactionId		gxid;
	} grd_txn;								/* TXN_GET_GXID */

	GTM_SequenceKeyData		grd_seqkey;		/* SEQUENCE_INIT
											 * SEQUENCE_RESET
											 * SEQUENCE_CLOSE */
	struct
	{
		GTM_SequenceKeyData	seqkey;
		GTM_Sequence		seqval;
	} grd_seq;								/* SEQUENCE_GET_CURRENT
											   SEQUENCE_GET_NEXT */

	struct
	{
		int						txn_count; /* TXN_BEGIN_GETGXID_MULTI */
		GlobalTransactionId		start_gxid;
		GTM_Timestamp			timestamp;
	} grd_txn_get_multi;

	struct
	{
		int			txn_count;								/* TXN_COMMIT_MULTI */
		int			status[GTM_MAX_GLOBAL_TRANSACTIONS];
	} grd_txn_rc_multi;

	struct
	{
		GTM_TransactionHandle	txnhandle;				/* SNAPSHOT_GXID_GET */
		GlobalTransactionId		gxid;					/* SNAPSHOT_GET */
		int						txn_count;				/* SNAPSHOT_GET_MULTI */
		int						status[GTM_MAX_GLOBAL_TRANSACTIONS];
	} grd_txn_snap_multi;

	/*
	 * TODO
	 * 	TXN_GET_STATUS
	 * 	TXN_GET_ALL_PREPARED
	 */
} GTM_ResultData;

typedef struct GTM_Result
{
	GTM_ResultType		gr_type;
	int					gr_msglen;
	int					gr_status;
	GTM_ProxyMsgHeader	gr_proxyhdr;
	GTM_ResultData		gr_resdata;
	/*
	 * We keep these two items outside the union to avoid repeated malloc/free
	 * of the xip array. If these items are pushed inside the union, they may
	 * get overwritten by other members in the union
	 */
	int					gr_xip_size;
	GTM_SnapshotData	gr_snapshot;

	/*
	 * Similarly, keep the buffer for proxying data outside the union
	 */
	char				*gr_proxy_data;
	int					gr_proxy_datalen;
} GTM_Result;

/*
 * Connection Management API
 */
GTM_Conn *connect_gtm(const char *connect_string);
void disconnect_gtm(GTM_Conn *conn);

/*
 * Transaction Management API
 */
GlobalTransactionId begin_transaction(GTM_Conn *conn, GTM_IsolationLevel isolevel, GTM_Timestamp *timestamp);
GlobalTransactionId begin_transaction_autovacuum(GTM_Conn *conn, GTM_IsolationLevel isolevel);
int commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid,
						int nodecnt, PGXC_NodeId nodes[]);

/*
 * Snapshot Management API
 */
GTM_SnapshotData *get_snapshot(GTM_Conn *conn, GlobalTransactionId gxid,
		bool canbe_grouped);

/*
 * Sequence Management API
 */
int open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
				  GTM_Sequence minval, GTM_Sequence maxval,
				  GTM_Sequence startval, bool cycle);
int alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
				   GTM_Sequence minval, GTM_Sequence maxval,
				   GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart);
int close_sequence(GTM_Conn *conn, GTM_SequenceKey key);
int rename_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey);
GTM_Sequence get_current(GTM_Conn *conn, GTM_SequenceKey key);
GTM_Sequence get_next(GTM_Conn *conn, GTM_SequenceKey key);
int set_val(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence nextval, bool is_called);
int reset_sequence(GTM_Conn *conn, GTM_SequenceKey key);


#endif
