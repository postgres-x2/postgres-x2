/*-------------------------------------------------------------------------
 *
 * gtm_txn.c
 *	Transaction handling
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"
#include "gtm/elog.h"
#include "gtm/palloc.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/assert.h"
#include "gtm/stringinfo.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"
#include <unistd.h>

/* Local functions */
static XidStatus GlobalTransactionIdGetStatus(GlobalTransactionId transactionId);
static bool GTM_SetDoVacuum(GTM_TransactionHandle handle);

GTM_Transactions GTMTransactions;

void
GTM_InitTxnManager(void)
{
	int ii;

	memset(&GTMTransactions, 0, sizeof (GTM_Transactions));

	for (ii = 0; ii < GTM_MAX_GLOBAL_TRANSACTIONS; ii++)
	{
		GTM_TransactionInfo *gtm_txninfo = &GTMTransactions.gt_transactions_array[ii];
		gtm_txninfo->gti_in_use = false;
		GTM_RWLockInit(&gtm_txninfo->gti_lock);
	}

	/*
	 * XXX When GTM is stopped and restarted, it must start assinging GXIDs
	 * greater than the previously assgined values. If it was a clean shutdown,
	 * the GTM can store the last assigned value at a known location on
	 * permanent storage and read it back when it's restarted. It will get
	 * trickier for GTM failures.
	 *
	 * TODO We skip thia part for the prototype.
	 */ 
	GTMTransactions.gt_nextXid = FirstNormalGlobalTransactionId;

	/*
	 * XXX The gt_oldestXid is the cluster level oldest Xid
	 */
	GTMTransactions.gt_oldestXid = FirstNormalGlobalTransactionId;
	
	/*
	 * XXX Compute various xid limits to avoid wrap-around related database
	 * corruptions. Again, this is not implemeneted for the prototype
	 */
	GTMTransactions.gt_xidVacLimit = InvalidGlobalTransactionId;
	GTMTransactions.gt_xidWarnLimit = InvalidGlobalTransactionId;
	GTMTransactions.gt_xidStopLimit = InvalidGlobalTransactionId;
	GTMTransactions.gt_xidWrapLimit = InvalidGlobalTransactionId;

	/*
	 * XXX Newest XID that is committed or aborted
	 */
	GTMTransactions.gt_latestCompletedXid = FirstNormalGlobalTransactionId;
	
	/*
	 * Initialize the locks to protect various XID fields as well as the linked
	 * list of transactions
	 */
	GTM_RWLockInit(&GTMTransactions.gt_XidGenLock);
	GTM_RWLockInit(&GTMTransactions.gt_TransArrayLock);

	/*
	 * Initialize the list
	 */
	GTMTransactions.gt_open_transactions = NIL;
	GTMTransactions.gt_lastslot = -1;

	GTMTransactions.gt_gtm_state = GTM_STARTING;

	return;
}

/*
 * Get the status of current or past transaction.
 */
static XidStatus
GlobalTransactionIdGetStatus(GlobalTransactionId transactionId)
{
	XidStatus	xidstatus;

	/*
	 * Also, check to see if the transaction ID is a permanent one.
	 */
	if (!GlobalTransactionIdIsNormal(transactionId))
	{
		if (GlobalTransactionIdEquals(transactionId, BootstrapGlobalTransactionId))
			return TRANSACTION_STATUS_COMMITTED;
		if (GlobalTransactionIdEquals(transactionId, FrozenGlobalTransactionId))
			return TRANSACTION_STATUS_COMMITTED;
		return TRANSACTION_STATUS_ABORTED;
	}

	/*
	 * TODO To be implemeneted
	 */
	return xidstatus;
}

/*
 * Given the GXID, find the corresponding transaction handle.
 */
GTM_TransactionHandle
GTM_GXIDToHandle(GlobalTransactionId gxid)
{
	ListCell *elem = NULL;
   	GTM_TransactionInfo *gtm_txninfo = NULL;

	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_READ);

	foreach(elem, GTMTransactions.gt_open_transactions)
	{
		gtm_txninfo = (GTM_TransactionInfo *)lfirst(elem);
		if (GlobalTransactionIdEquals(gtm_txninfo->gti_gxid, gxid))
			break;
		gtm_txninfo = NULL;
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

	if (gtm_txninfo != NULL)
		return gtm_txninfo->gti_handle;
	else
		return InvalidTransactionHandle;
}

/*
 * Given the transaction handle, find the corresponding transaction info
 * structure
 *
 * Note: Since a transaction handle is just an index into the global array,
 * this function should be very quick. We should turn into an inline future for
 * fast path.
 */
GTM_TransactionInfo *
GTM_HandleToTransactionInfo(GTM_TransactionHandle handle)
{
   	GTM_TransactionInfo *gtm_txninfo = NULL;

	if ((handle < 0) || (handle > GTM_MAX_GLOBAL_TRANSACTIONS))
	{
		ereport(WARNING,
				(ERANGE, errmsg("Invalid transaction handle: %d", handle)));
		return NULL;
	}

	gtm_txninfo = &GTMTransactions.gt_transactions_array[handle];

	if (!gtm_txninfo->gti_in_use)
	{
		ereport(WARNING,
				(ERANGE, errmsg("Invalid transaction handle, txn_info not in use")));
		return NULL;
	}

	return gtm_txninfo;
}

/*
 * Remove the given transaction info structures from the global array. If the
 * calling thread does not have enough cached structures, we in fact keep the
 * structure in the global array and also add it to the list of cached
 * structures for this thread. This ensures that the next transaction starting
 * in this thread can quickly get a free slot in the array of transactions and
 * also avoid repeated malloc/free of the structures.
 *
 * Also compute the latestCompletedXid.
 */
static void
GTM_RemoveTransInfoMulti(GTM_TransactionInfo *gtm_txninfo[], int txn_count)
{
	int ii;

	/*
	 * Remove the transaction structure from the global list of open
	 * transactions
	 */
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

	for (ii = 0; ii < txn_count; ii++)
	{
		if (gtm_txninfo[ii] == NULL)
			continue;

		GTMTransactions.gt_open_transactions = list_delete(GTMTransactions.gt_open_transactions, gtm_txninfo[ii]);

		if (GlobalTransactionIdIsNormal(gtm_txninfo[ii]->gti_gxid) &&
			GlobalTransactionIdFollowsOrEquals(gtm_txninfo[ii]->gti_gxid,
											   GTMTransactions.gt_latestCompletedXid))
			GTMTransactions.gt_latestCompletedXid = gtm_txninfo[ii]->gti_gxid;


		elog(DEBUG1, "GTM_RemoveTransInfoMulti: removing transaction id %u, %lu",
				gtm_txninfo[ii]->gti_gxid, gtm_txninfo[ii]->gti_thread_id);
		/*
		 * Now mark the transaction as aborted and mark the structure as not-in-use
		 */
		gtm_txninfo[ii]->gti_state = GTM_TXN_ABORTED;
		gtm_txninfo[ii]->gti_nodecount = 0;
		gtm_txninfo[ii]->gti_in_use = false;
		gtm_txninfo[ii]->gti_snapshot_set = false;
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	return;
}

/*
 * Remove all transaction infos associated with the caller thread and the given
 * backend
 *
 * Also compute the latestCompletedXid.
 */
void
GTM_RemoveAllTransInfos(int backend_id)
{
	ListCell *cell, *prev;
	GTM_ThreadID thread_id;

	thread_id = pthread_self();
	
	/*
	 * Scan the global list of open transactions
	 */
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);
	prev = NULL;
	cell = list_head(GTMTransactions.gt_open_transactions);
	while (cell != NULL)
	{
		GTM_TransactionInfo *gtm_txninfo = lfirst(cell);
		/* check if current entry is associated with the thread */
		if ((gtm_txninfo->gti_in_use) &&
			(gtm_txninfo->gti_thread_id == thread_id) &&
			((gtm_txninfo->gti_backend_id == backend_id) || (backend_id == -1)))
		{
			/* remove the entry */
			GTMTransactions.gt_open_transactions = list_delete_cell(GTMTransactions.gt_open_transactions, cell, prev);

			/* update the latestComletedXid */
			if (GlobalTransactionIdIsNormal(gtm_txninfo->gti_gxid) &&
				GlobalTransactionIdFollowsOrEquals(gtm_txninfo->gti_gxid,
												   GTMTransactions.gt_latestCompletedXid))
				GTMTransactions.gt_latestCompletedXid = gtm_txninfo->gti_gxid;

			elog(DEBUG1, "GTM_RemoveAllTransInfos: removing transaction id %u, %lu:%lu",
					gtm_txninfo->gti_gxid, gtm_txninfo->gti_thread_id, thread_id);
			/*
			 * Now mark the transaction as aborted and mark the structure as not-in-use
			 */
			gtm_txninfo->gti_state = GTM_TXN_ABORTED;
			gtm_txninfo->gti_nodecount = 0;
			gtm_txninfo->gti_in_use = false;
			gtm_txninfo->gti_snapshot_set = false;
			
			/* move to next cell in the list */
			if (prev)
				cell = lnext(prev);
			else
				cell = list_head(GTMTransactions.gt_open_transactions);
		}
		else
		{
			prev = cell;
			cell = lnext(cell);
		}
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	return;
}
/*
 * GlobalTransactionIdDidCommit
 *		True iff transaction associated with the identifier did commit.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool							/* true if given transaction committed */
GlobalTransactionIdDidCommit(GlobalTransactionId transactionId)
{
	XidStatus	xidstatus;

	xidstatus = GlobalTransactionIdGetStatus(transactionId);

	/*
	 * If it's marked committed, it's committed.
	 */
	if (xidstatus == TRANSACTION_STATUS_COMMITTED)
		return true;

	/*
	 * It's not committed.
	 */
	return false;
}

/*
 * GlobalTransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool							/* true if given transaction aborted */
GlobalTransactionIdDidAbort(GlobalTransactionId transactionId)
{
	XidStatus	xidstatus;

	xidstatus = GlobalTransactionIdGetStatus(transactionId);

	/*
	 * If it's marked aborted, it's aborted.
	 */
	if (xidstatus == TRANSACTION_STATUS_ABORTED)
		return true;

	/*
	 * It's not aborted.
	 */
	return false;
}

/*
 * GlobalTransactionIdPrecedes --- is id1 logically < id2?
 */
bool
GlobalTransactionIdPrecedes(GlobalTransactionId id1, GlobalTransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.	If both are normal, do a modulo-2^31 comparison.
	 */
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32) (id1 - id2);
	return (diff < 0);
}

/*
 * GlobalTransactionIdPrecedesOrEquals --- is id1 logically <= id2?
 */
bool
GlobalTransactionIdPrecedesOrEquals(GlobalTransactionId id1, GlobalTransactionId id2)
{
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 <= id2);

	diff = (int32) (id1 - id2);
	return (diff <= 0);
}

/*
 * GlobalTransactionIdFollows --- is id1 logically > id2?
 */
bool
GlobalTransactionIdFollows(GlobalTransactionId id1, GlobalTransactionId id2)
{
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 > id2);

	diff = (int32) (id1 - id2);
	return (diff > 0);
}

/*
 * GlobalTransactionIdFollowsOrEquals --- is id1 logically >= id2?
 */
bool
GlobalTransactionIdFollowsOrEquals(GlobalTransactionId id1, GlobalTransactionId id2)
{
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 >= id2);

	diff = (int32) (id1 - id2);
	return (diff >= 0);
}


/*
 * Set that the transaction is doing vacuum
 *
 */
static bool
GTM_SetDoVacuum(GTM_TransactionHandle handle)
{
	GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(handle);

	if (gtm_txninfo == NULL)
		ereport(ERROR, (EINVAL, errmsg("Invalid transaction handle")));

	gtm_txninfo->gti_vacuum = true;
	return true;
}

/*
 * Allocate the next XID for my new transaction
 *
 * The new XID is also stored into the transaction info structure of the given
 * transaction before returning.
 */
GlobalTransactionId
GTM_GetGlobalTransactionIdMulti(GTM_TransactionHandle handle[], int txn_count)
{
	GlobalTransactionId xid, start_xid = InvalidGlobalTransactionId;
	GTM_TransactionInfo *gtm_txninfo = NULL;
	int ii;

	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);

	if (GTMTransactions.gt_gtm_state == GTM_SHUTTING_DOWN)
	{
		GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
		ereport(ERROR, (EINVAL, errmsg("GTM shutting down -- can not issue new transaction ids")));
		return InvalidGlobalTransactionId;
	}


	/*
	 * If we are allocating the first XID of a new page of the commit log,
	 * zero out that commit-log page before returning. We must do this while
	 * holding XidGenLock, else another xact could acquire and commit a later
	 * XID before we zero the page.  Fortunately, a page of the commit log
	 * holds 32K or more transactions, so we don't have to do this very often.
	 *
	ExtendCLOG(xid);
	 */

	/*
	 * Now advance the nextXid counter.  This must not happen until after we
	 * have successfully completed ExtendCLOG() --- if that routine fails, we
	 * want the next incoming transaction to try it again.	We cannot assign
	 * more XIDs until there is CLOG space for them.
	 */
	for (ii = 0; ii < txn_count; ii++)
	{
		xid = GTMTransactions.gt_nextXid;

		if (!GlobalTransactionIdIsValid(start_xid))
			start_xid = xid;

		/*----------
		 * Check to see if it's safe to assign another XID.  This protects against
		 * catastrophic data loss due to XID wraparound.  The basic rules are:
		 *
		 * If we're past xidVacLimit, start trying to force autovacuum cycles.
		 * If we're past xidWarnLimit, start issuing warnings.
		 * If we're past xidStopLimit, refuse to execute transactions, unless
		 * we are running in a standalone backend (which gives an escape hatch
		 * to the DBA who somehow got past the earlier defenses).
		 *
		 * Test is coded to fall out as fast as possible during normal operation,
		 * ie, when the vac limit is set and we haven't violated it.
		 *----------
		 */
		if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidVacLimit) &&
			GlobalTransactionIdIsValid(GTMTransactions.gt_xidVacLimit))
		{
			if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidStopLimit))
			{
				GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
				ereport(ERROR,
						(ERANGE,
						 errmsg("database is not accepting commands to avoid wraparound data loss in database ")));
			}
			else if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidWarnLimit))
				ereport(WARNING,
				(errmsg("database must be vacuumed within %u transactions",
						GTMTransactions.gt_xidWrapLimit - xid)));
		}

		GlobalTransactionIdAdvance(GTMTransactions.gt_nextXid);
		gtm_txninfo = GTM_HandleToTransactionInfo(handle[ii]);
		Assert(gtm_txninfo);
		gtm_txninfo->gti_gxid = xid;
	}

	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

	return start_xid;
}

/*
 * Allocate the next XID for my new transaction
 *
 * The new XID is also stored into the transaction info structure of the given
 * transaction before returning.
 */
GlobalTransactionId
GTM_GetGlobalTransactionId(GTM_TransactionHandle handle)
{
	return GTM_GetGlobalTransactionIdMulti(&handle, 1);
}

/*
 * Read nextXid but don't allocate it.
 */
GlobalTransactionId
ReadNewGlobalTransactionId(void)
{
	GlobalTransactionId xid;

	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_READ);	
	xid = GTMTransactions.gt_nextXid;
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

	return xid;
}

/*
 * Set the nextXid.
 *
 * The GXID is usually read from a control file and set when the GTM is
 * started. When the GTM is finally shutdown, the next to-be-assigned GXID is
 * stroed in the control file.
 *
 * XXX We don't yet handle any crash recovery. So if the GTM is shutdown 
 */
void
SetNextGlobalTransactionId(GlobalTransactionId gxid)
{
	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);	
	GTMTransactions.gt_nextXid = gxid;
	GTMTransactions.gt_gtm_state = GTM_RUNNING;
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
	return;
}


/* Transaction Control */
int
GTM_BeginTransactionMulti(GTM_CoordinatorId coord_id,
					 GTM_IsolationLevel isolevel[],
					 bool readonly[],
					 GTMProxy_ConnID connid[],
					 int txn_count,
					 GTM_TransactionHandle txns[])
{
	GTM_TransactionInfo *gtm_txninfo[txn_count];
	MemoryContext oldContext;
	int kk;

	memset(gtm_txninfo, 0, sizeof (gtm_txninfo));

	/*
	 * XXX We should allocate the transaction info structure in the
	 * top-most memory context instead of a thread context. This is
	 * necessary because the transaction may outlive the thread which
	 * started the transaction. Also, since the structures are stored in
	 * the global array, it's dangerous to free the structures themselves
	 * without removing the corresponding references from the global array
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	
	for (kk = 0; kk < txn_count; kk++)
	{
		int ii, jj, startslot;

		/*
		 * We had no cached slots. Now find a free slot in the transation array
		 * and store the transaction info structure there
		 */
		GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

		startslot = GTMTransactions.gt_lastslot + 1;
		if (startslot >= GTM_MAX_GLOBAL_TRANSACTIONS)
			startslot = 0;

		for (ii = startslot, jj = 0;
			 jj < GTM_MAX_GLOBAL_TRANSACTIONS;
			 ii = (ii + 1) % GTM_MAX_GLOBAL_TRANSACTIONS, jj++)
		{
			if (GTMTransactions.gt_transactions_array[ii].gti_in_use == false)
			{
				gtm_txninfo[kk] = &GTMTransactions.gt_transactions_array[ii];
				break;
			}

			if (ii == GTMTransactions.gt_lastslot)
			{
				GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
				ereport(ERROR,
						(ERANGE, errmsg("Max transaction limit reached")));
			}
		}


		gtm_txninfo[kk]->gti_gxid = InvalidGlobalTransactionId;
		gtm_txninfo[kk]->gti_xmin = InvalidGlobalTransactionId;
		gtm_txninfo[kk]->gti_state = GTM_TXN_STARTING;
		gtm_txninfo[kk]->gti_coordid = coord_id;

		gtm_txninfo[kk]->gti_isolevel = isolevel[kk];
		gtm_txninfo[kk]->gti_readonly = readonly[kk];
		gtm_txninfo[kk]->gti_backend_id = connid[kk];
		gtm_txninfo[kk]->gti_in_use = true;

		gtm_txninfo[kk]->gti_handle = ii;
		gtm_txninfo[kk]->gti_vacuum = false;
		gtm_txninfo[kk]->gti_thread_id = pthread_self();
		GTMTransactions.gt_lastslot = ii;		

		txns[kk] = ii;

		/*
		 * Add the structure to the global list of open transactions. We should
		 * call add the element to the list in the context of TopMostMemoryContext
		 * because the list is global and any memory allocation must outlive the
		 * thread context
		 */
		GTMTransactions.gt_open_transactions = lappend(GTMTransactions.gt_open_transactions, gtm_txninfo[kk]);
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

	MemoryContextSwitchTo(oldContext);

	return txn_count;
}

/* Transaction Control */
GTM_TransactionHandle
GTM_BeginTransaction(GTM_CoordinatorId coord_id,
					 GTM_IsolationLevel isolevel,
					 bool readonly)
{
	GTM_TransactionHandle txn;
	GTMProxy_ConnID connid = -1;

	GTM_BeginTransactionMulti(coord_id, &isolevel, &readonly, &connid, 1, &txn);
	return txn;
}

/*
 * Same as GTM_RollbackTransaction, but takes GXID as input
 */
int
GTM_RollbackTransactionGXID(GlobalTransactionId gxid)
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_RollbackTransaction(txn);
}

/*
 * Rollback multiple transactions in one go
 */
int
GTM_RollbackTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[])
{
	GTM_TransactionInfo *gtm_txninfo[txn_count];
	int ii;
   
	for (ii = 0; ii < txn_count; ii++)
	{
		gtm_txninfo[ii] = GTM_HandleToTransactionInfo(txn[ii]);

		if (gtm_txninfo[ii] == NULL)
		{
			status[ii] = STATUS_ERROR;
			continue;
		}

		/*
		 * Mark the transaction as being aborted
		 */
		GTM_RWLockAcquire(&gtm_txninfo[ii]->gti_lock, GTM_LOCKMODE_WRITE);
		gtm_txninfo[ii]->gti_state = GTM_TXN_ABORT_IN_PROGRESS;
		GTM_RWLockRelease(&gtm_txninfo[ii]->gti_lock);
		status[ii] = STATUS_OK;
	}

	GTM_RemoveTransInfoMulti(gtm_txninfo, txn_count);

	return txn_count;
}

/*
 * Rollback a transaction
 */
int
GTM_RollbackTransaction(GTM_TransactionHandle txn)
{
	int status;
	GTM_RollbackTransactionMulti(&txn, 1, &status);
	return status;
}


/*
 * Same as GTM_CommitTransaction but takes GXID as input
 */
int
GTM_CommitTransactionGXID(GlobalTransactionId gxid)
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_CommitTransaction(txn);
}

/*
 * Commit multiple transactions in one go
 */
int
GTM_CommitTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[])
{
	GTM_TransactionInfo *gtm_txninfo[txn_count];
	int ii;
   
	for (ii = 0; ii < txn_count; ii++)
	{
		gtm_txninfo[ii] = GTM_HandleToTransactionInfo(txn[ii]);

		if (gtm_txninfo[ii] == NULL)
		{
			status[ii] = STATUS_ERROR;
			continue;
		}
		/*
		 * Mark the transaction as being aborted
		 */
		GTM_RWLockAcquire(&gtm_txninfo[ii]->gti_lock, GTM_LOCKMODE_WRITE);
		gtm_txninfo[ii]->gti_state = GTM_TXN_COMMIT_IN_PROGRESS;
		GTM_RWLockRelease(&gtm_txninfo[ii]->gti_lock);
		status[ii] = STATUS_OK;
	}

	GTM_RemoveTransInfoMulti(gtm_txninfo, txn_count);

	return txn_count;
}

/*
 * Commit a transaction
 */
int
GTM_CommitTransaction(GTM_TransactionHandle txn)
{
	int status;
	GTM_CommitTransactionMulti(&txn, 1, &status);
	return status;
}

/*
 * Prepare a transaction
 */
int
GTM_PrepareTransaction(GTM_TransactionHandle txn,
					   uint32 nodecnt,
					   PGXC_NodeId nodes[])
{
	GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(txn);

	if (gtm_txninfo == NULL)
		return STATUS_ERROR;

	/*
	 * Mark the transaction as being aborted
	 */
	GTM_RWLockAcquire(&gtm_txninfo->gti_lock, GTM_LOCKMODE_WRITE);
	
	gtm_txninfo->gti_state = GTM_TXN_PREPARE_IN_PROGRESS;
	gtm_txninfo->gti_nodecount = nodecnt;
	if (gtm_txninfo->gti_nodes == NULL)
		gtm_txninfo->gti_nodes = (PGXC_NodeId *)MemoryContextAlloc(TopMostMemoryContext, sizeof (PGXC_NodeId) * GTM_MAX_2PC_NODES);
	memcpy(gtm_txninfo->gti_nodes, nodes, sizeof (PGXC_NodeId) * nodecnt);

	GTM_RWLockRelease(&gtm_txninfo->gti_lock);

	return STATUS_OK;
}

/*
 * Same as GTM_PrepareTransaction but takes GXID as input
 */
int
GTM_PrepareTransactionGXID(GlobalTransactionId gxid,
					   uint32 nodecnt,
					   PGXC_NodeId nodes[])
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_PrepareTransaction(txn, nodecnt, nodes);
}

/*
 * Get status of the given transaction
 */
GTM_TransactionStates
GTM_GetStatus(GTM_TransactionHandle txn)
{
	GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(txn);
	return gtm_txninfo->gti_state;
}

/*
 * Same as GTM_GetStatus but takes GXID as input
 */
GTM_TransactionStates
GTM_GetStatusGXID(GlobalTransactionId gxid)
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_GetStatus(txn);
}

/*
 * Process MSG_TXN_BEGIN message
 */
void
ProcessBeginTransactionCommand(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	StringInfoData buf;
	GTM_TransactionHandle txn;
	MemoryContext oldContext;

	txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator Id - replace 0 with that
	 */
	txn = GTM_BeginTransaction(0, txn_isolation_level, txn_read_only);
	if (txn == InvalidTransactionHandle)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start a new transaction")));

	MemoryContextSwitchTo(oldContext);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn, sizeof(txn));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_BEGIN_GETGXID message
 */
void
ProcessBeginTransactionGetGXIDCommand(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	MemoryContext oldContext;

	txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator Id - replace 0 with that
	 */
	txn = GTM_BeginTransaction(0, txn_isolation_level, txn_read_only);
	if (txn == InvalidTransactionHandle)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start a new transaction")));

	gxid = GTM_GetGlobalTransactionId(txn);
	if (gxid == InvalidGlobalTransactionId)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get a new transaction id")));

	MemoryContextSwitchTo(oldContext);

	elog(LOG, "Sending transaction id %u", gxid);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_GETGXID_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_BEGIN_GETGXID_AUTOVACUUM message
 */
void
ProcessBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	MemoryContext oldContext;

	elog(DEBUG3, "Inside ProcessBeginTransactionGetGXIDAutovacuumCommand");

	txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator Id - replace 0 with that
	 */
	txn = GTM_BeginTransaction(0, txn_isolation_level, txn_read_only);
	if (txn == InvalidTransactionHandle)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start a new transaction")));

	gxid = GTM_GetGlobalTransactionId(txn);
	if (gxid == InvalidGlobalTransactionId)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get a new transaction id")));

	/* Indicate that it is for autovacuum */
	GTM_SetDoVacuum(txn);

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG3, "Sending transaction id %d", gxid);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_BEGIN_GETGXID_MULTI message
 */
void
ProcessBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
	bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
	int txn_count;
	StringInfoData buf;
	GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId gxid, end_gxid;
	GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];
	MemoryContext oldContext;
	int count;
	int ii;

	txn_count = pq_getmsgint(message, sizeof (int));

	if (txn_count <= 0)
		elog(PANIC, "Zero or less transaction count");

	for (ii = 0; ii < txn_count; ii++)
	{
		txn_isolation_level[ii] = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
		txn_read_only[ii] = pq_getmsgbyte(message);
		txn_connid[ii] = pq_getmsgint(message, sizeof (GTMProxy_ConnID));
	}

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator Id - replace 0 with that
	 */
	count = GTM_BeginTransactionMulti(0, txn_isolation_level, txn_read_only, txn_connid,
									  txn_count, txn);
	if (count != txn_count)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start %d new transactions", txn_count)));

	gxid = GTM_GetGlobalTransactionIdMulti(txn, txn_count);
	if (gxid == InvalidGlobalTransactionId)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get a new transaction id")));

	MemoryContextSwitchTo(oldContext);
	
	end_gxid = gxid + txn_count;
	if (end_gxid < gxid)
		end_gxid += FirstNormalGlobalTransactionId;

	elog(LOG, "Sending transaction ids from %u to %u", gxid, end_gxid);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_GETGXID_MULTI_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_COMMIT message
 */
void
ProcessCommitTransactionCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	int isgxid = 0;
	MemoryContext oldContext;
	int status = STATUS_OK;

	isgxid = pq_getmsgbyte(message);

	if (isgxid)
	{
		const char *data = pq_getmsgbytes(message, sizeof (gxid));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid GXID")));
		memcpy(&gxid, data, sizeof (gxid));
		txn = GTM_GXIDToHandle(gxid);
	}
	else
	{
		const char *data = pq_getmsgbytes(message, sizeof (txn));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid Transaction Handle")));
		memcpy(&txn, data, sizeof (txn));
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Commit the transaction
	 */
	status = GTM_CommitTransaction(txn);

	MemoryContextSwitchTo(oldContext);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_COMMIT_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_sendint(&buf, status, sizeof(status));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_ROLLBACK message
 */
void
ProcessRollbackTransactionCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	int isgxid = 0;
	MemoryContext oldContext;
	int status = STATUS_OK;

	isgxid = pq_getmsgbyte(message);

	if (isgxid)
	{
		const char *data = pq_getmsgbytes(message, sizeof (gxid));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid GXID")));
		memcpy(&gxid, data, sizeof (gxid));
		txn = GTM_GXIDToHandle(gxid);
	}
	else
	{
		const char *data = pq_getmsgbytes(message, sizeof (txn));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid Transaction Handle")));
		memcpy(&txn, data, sizeof (txn));
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Commit the transaction
	 */
	status = GTM_RollbackTransaction(txn);

	MemoryContextSwitchTo(oldContext);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_ROLLBACK_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_sendint(&buf, status, sizeof(status));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}


/*
 * Process MSG_TXN_COMMIT_MULTI message
 */
void
ProcessCommitTransactionCommandMulti(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	int isgxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	MemoryContext oldContext;
	int status[GTM_MAX_GLOBAL_TRANSACTIONS];
	int txn_count, count;
	int ii;

	txn_count = pq_getmsgint(message, sizeof (int));

	for (ii = 0; ii < txn_count; ii++)
	{
		isgxid[ii] = pq_getmsgbyte(message);
		if (isgxid[ii])
		{
			const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid GXID")));
			memcpy(&gxid[ii], data, sizeof (gxid[ii]));
			txn[ii] = GTM_GXIDToHandle(gxid[ii]);
			elog(DEBUG1, "ProcessCommitTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
		}
		else
		{
			const char *data = pq_getmsgbytes(message, sizeof (txn[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid Transaction Handle")));
			memcpy(&txn[ii], data, sizeof (txn[ii]));
			elog(DEBUG1, "ProcessCommitTransactionCommandMulti: handle(%u)", txn[ii]);
		}
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Commit the transaction
	 */
	count = GTM_CommitTransactionMulti(txn, txn_count, status);

	MemoryContextSwitchTo(oldContext);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_COMMIT_MULTI_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
	pq_sendbytes(&buf, (char *)status, sizeof(int) * txn_count);
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_ROLLBACK_MULTI message
 */
void
ProcessRollbackTransactionCommandMulti(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	int isgxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	MemoryContext oldContext;
	int status[GTM_MAX_GLOBAL_TRANSACTIONS];
	int txn_count, count;
	int ii;

	txn_count = pq_getmsgint(message, sizeof (int));

	for (ii = 0; ii < txn_count; ii++)
	{
		isgxid[ii] = pq_getmsgbyte(message);
		if (isgxid[ii])
		{
			const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid GXID")));
			memcpy(&gxid[ii], data, sizeof (gxid[ii]));
			txn[ii] = GTM_GXIDToHandle(gxid[ii]);
			elog(DEBUG1, "ProcessRollbackTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
		}
		else
		{
			const char *data = pq_getmsgbytes(message, sizeof (txn[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid Transaction Handle")));
			memcpy(&txn[ii], data, sizeof (txn[ii]));
			elog(DEBUG1, "ProcessRollbackTransactionCommandMulti: handle(%u)", txn[ii]);
		}
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Commit the transaction
	 */
	count = GTM_RollbackTransactionMulti(txn, txn_count, status);

	MemoryContextSwitchTo(oldContext);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_ROLLBACK_MULTI_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
	pq_sendbytes(&buf, (char *)status, sizeof(int) * txn_count);
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_PREPARE message
 */
void
ProcessPrepareTransactionCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	int isgxid = 0;
	int nodecnt;
	PGXC_NodeId *nodes;
	MemoryContext oldContext;

	isgxid = pq_getmsgbyte(message);

	if (isgxid)
	{
		const char *data = pq_getmsgbytes(message, sizeof (gxid));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid GXID")));
		memcpy(&gxid, data, sizeof (gxid));
		txn = GTM_GXIDToHandle(gxid);
	}
	else
	{
		const char *data = pq_getmsgbytes(message, sizeof (txn));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid Transaction Handle")));
		memcpy(&txn, data, sizeof (txn));
	}

	nodecnt = pq_getmsgint(message, sizeof (nodecnt));
	nodes = (PGXC_NodeId *) palloc(sizeof (PGXC_NodeId) * nodecnt);
	memcpy(nodes, pq_getmsgbytes(message, sizeof (PGXC_NodeId) * nodecnt),
			sizeof (PGXC_NodeId) * nodecnt);

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Prepare the transaction
	 */
	if (GTM_PrepareTransaction(txn, nodecnt, nodes) != STATUS_OK)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to commit the transaction")));

	MemoryContextSwitchTo(oldContext);

	pfree(nodes);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_PREPARE_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}

/*
 * Process MSG_TXN_GET_GXID message
 */
void
ProcessGetGXIDTransactionCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	const char *data;
	MemoryContext oldContext;

	elog(DEBUG3, "Inside ProcessGetGXIDTransactionCommand");

	data = pq_getmsgbytes(message, sizeof (txn));
	if (data == NULL)
		ereport(ERROR,
				(EPROTO,
				 errmsg("Message does not contain valid Transaction Handle")));
	memcpy(&txn, data, sizeof (txn));

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Get the transaction id for the given global transaction
	 */
	gxid = GTM_GetGlobalTransactionId(txn);
	if (GlobalTransactionIdIsValid(gxid))
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get the transaction id")));

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG3, "Sending transaction id %d", gxid);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_GET_GXID_RESULT, 4);
	if (myport->is_proxy)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn, sizeof(txn));
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_endmessage(myport, &buf);

	if (!myport->is_proxy)
		pq_flush(myport);
	return;
}


/*
 * Mark GTM as shutting down. This point onwards no new GXID are issued to
 * ensure that the last GXID recorded in the control file remains sane
 */
void
GTM_SetShuttingDown(void)
{
	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
	GTMTransactions.gt_gtm_state = GTM_SHUTTING_DOWN;
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
}

void
GTM_RestoreTxnInfo(int ctlfd, GlobalTransactionId next_gxid)
{
	GlobalTransactionId saved_gxid;

	if (ctlfd != -1)
	{
		if ((read(ctlfd, &saved_gxid, sizeof (saved_gxid)) != sizeof (saved_gxid)) &&
			(!GlobalTransactionIdIsValid(next_gxid)))
			return;
		if (!GlobalTransactionIdIsValid(next_gxid))
			next_gxid = saved_gxid;
	}

	elog(LOG, "Restoring last GXID to %u\n", next_gxid);

	if (GlobalTransactionIdIsValid(next_gxid))
		SetNextGlobalTransactionId(next_gxid);
	/* Set this otherwise a strange snapshot might be returned for the first one */
	GTMTransactions.gt_latestCompletedXid = next_gxid - 1;
	return;
}

void
GTM_SaveTxnInfo(int ctlfd)
{
	GlobalTransactionId next_gxid;

	next_gxid = ReadNewGlobalTransactionId();

	elog(LOG, "Saving transaction info - next_gxid: %u", next_gxid);

	write(ctlfd, &next_gxid, sizeof (next_gxid));
}
/*
 * TODO
 */
int GTM_GetAllTransactions(GTM_TransactionInfo txninfo[], uint32 txncnt);

/*
 * TODO
 */
uint32 GTM_GetAllPrepared(GlobalTransactionId gxids[], uint32 gxidcnt);

