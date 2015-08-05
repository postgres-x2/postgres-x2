/*-------------------------------------------------------------------------
 *
 * xc_gtm_sync_timeout.h
 *		Staff of checking commit report of the given global transaction
 *		to GTM.
 *
 *
 * Portions Copyright (c) 1996-2011 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2015 Postgres-XC Development Group
 *
 * src/include/pgxc/xc_maintenance_mode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XC_GTM_COMMIT_SYNC_H
#define XC_GTM_COMMIT_SYNC_H

#include <unistd.h>
#include "c.h"
#include "postgres.h"
#include "utils/guc.h"
#include "gtm/gtm_c.h"

extern int xc_gtm_sync_timeout;			/* GUC */
extern bool xc_gtm_commit_sync_test;	/* Hidden GUC for test */
extern bool shouldSyncGXID;

extern bool syncGXID_GTM(GlobalTransactionId gxid);
extern void setLatestGTMSnapshot(GlobalTransactionId xmin,
							  	 GlobalTransactionId xmax,
							  	 int xcnt,
							  	 GlobalTransactionId *xip);
#define setSyncGXID() do{shouldSyncGXID = true;}while(0)

#endif /* XC_GTM_COMMIT_SYNC_H */
