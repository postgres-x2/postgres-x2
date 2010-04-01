/*-------------------------------------------------------------------------
 *
 * gtm.c
 * 
 *	  Module interfacing with GTM 
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>

#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"
#include "access/gtm.h"
#include "access/transam.h"
#include "utils/elog.h"

/* Configuration variables */
char *GtmHost = "localhost";
int GtmPort = 6666;
int GtmCoordinatorId = 1;

extern bool FirstSnapshotSet;

static GTM_Conn *conn;

#define CheckConnection() \
	if (GTMPQstatus(conn) != CONNECTION_OK) InitGTM()


bool IsGTMConnected()
{
	return conn != NULL;
}

void
InitGTM()
{
	/* 256 bytes should be enough */
	char conn_str[256];

	sprintf(conn_str, "host=%s port=%d coordinator_id=%d", GtmHost, GtmPort, GtmCoordinatorId);

	conn = PQconnectGTM(conn_str);
	if (GTMPQstatus(conn) != CONNECTION_OK)
	{
		int save_errno = errno;

		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("can not connect to GTM: %m")));		

		errno = save_errno;		

		CloseGTM();
	}
}

void
CloseGTM()
{
	GTMPQfinish(conn);
	conn = NULL;
}

GlobalTransactionId
BeginTranGTM()
{
	GlobalTransactionId  xid = InvalidGlobalTransactionId;

	CheckConnection();
	// TODO Isolation level
	if (conn)
		xid =  begin_transaction(conn, GTM_ISOLATION_RC);

	/* If something went wrong (timeout), try and reset GTM connection 
	 * and retry. This is safe at the beginning of a transaction.
	 */
	if (!TransactionIdIsValid(xid))
	{
		CloseGTM();
		InitGTM();
		if (conn)
			xid =  begin_transaction(conn, GTM_ISOLATION_RC);
	}
	return xid;
}

GlobalTransactionId
BeginTranAutovacuumGTM()
{
	GlobalTransactionId  xid = InvalidGlobalTransactionId;

	CheckConnection();
	// TODO Isolation level
	if (conn)
		xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);

	/* If something went wrong (timeout), try and reset GTM connection and retry.
	 * This is safe at the beginning of a transaction.
	 */
	if (!TransactionIdIsValid(xid))
	{
		CloseGTM();
		InitGTM();
		if (conn)
			xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);
	}
	return xid;
}

int
CommitTranGTM(GlobalTransactionId gxid)
{
	int ret;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();
	ret = commit_transaction(conn, gxid);

	/* If something went wrong (timeout), try and reset GTM connection. 
	 * We will close the transaction locally anyway, and closing GTM will force
	 * it to be closed on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
	}
	return ret;
}

int
RollbackTranGTM(GlobalTransactionId gxid)
{
	int ret;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();
	ret = abort_transaction(conn, gxid);

	/* If something went wrong (timeout), try and reset GTM connection. 
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
	}
	return ret;
}

GTM_Snapshot
GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped)
{
	GTM_Snapshot ret_snapshot = NULL;
	CheckConnection();
	if (conn)
		ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
	if (ret_snapshot == NULL)
	{
		CloseGTM();
		InitGTM();
	}
	return ret_snapshot;
}


/**
 * Create a sequence on the GTM.
 *
 * 
 */
int CreateSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval,
		GTM_Sequence maxval, GTM_Sequence startval, bool cycle)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname);
	seqkey.gsk_key = seqname;

	return conn ? open_sequence(conn, &seqkey, increment, minval, maxval, startval, cycle) : 0;
}

/**
 * Get the next sequence value
 */
GTM_Sequence
GetNextValGTM(char *seqname)
{
	GTM_Sequence ret = -1;
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname);
	seqkey.gsk_key = seqname;

	if (conn)
		ret =  get_next(conn, &seqkey);
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
	}
	return ret;
}

/**
 * Drop the sequence
 */
int
DropSequenceGTM(char *seqname)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname);
	seqkey.gsk_key = seqname;

	return conn ? close_sequence(conn, &seqkey) : -1;
}


