/*-------------------------------------------------------------------------
 *
 * gtm_utils.c
 *  Utililies of GTM
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 *
 * IDENTIFICATION
 *		src/gtm/common/gtm_utils.c
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_utils.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"

/*
 * gtm_report_failure() is an utility function to report fatal failure
 * which occureed inside GTM to XCM, especially communication errors.
 *
 * failed_conn is null-able when failed to establish a connection
 * with other node.
 */
#if 0
/*
 * PGXCTODO: This portion of code needs XCM support
 * to be able to report GTM failures to XC watcher and
 * enable a GTM reconnection kick.
 */
void
gtm_report_failure(GTM_Conn *failed_conn)
{
	elog(LOG, "Calling report_xcwatch_gtm_failure()...");
	return;
}
#endif
