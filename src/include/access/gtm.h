/*-------------------------------------------------------------------------
 *
 * gtm.h
 * 
 *	  Module interfacing with GTM definitions
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef ACCESS_GTM_H
#define ACCESS_GTM_H

#include "gtm/gtm_c.h"

/* Configuration variables */
extern char *GtmHost;
extern int GtmPort;
extern int GtmCoordinatorId;

extern bool IsGTMConnected(void);
extern void InitGTM(void);
extern void CloseGTM(void);
extern GlobalTransactionId BeginTranGTM(void);
extern GlobalTransactionId BeginTranAutovacuumGTM(void);
extern int CommitTranGTM(GlobalTransactionId gxid);
extern int RollbackTranGTM(GlobalTransactionId gxid);
extern GTM_Snapshot GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped);
extern GTM_Sequence GetNextValGTM(char *seqname);
extern int CreateSequenceGTM(char *seqname, GTM_Sequence increment, 
		GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
		bool cycle);
extern int DropSequenceGTM(char *seqname);
#endif /* ACCESS_GTM_H */
