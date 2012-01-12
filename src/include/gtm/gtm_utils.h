/*-------------------------------------------------------------------------
 *
 * gtm_utils.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
 *
 * src/include/gtm/gtm_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_UTILS_H
#define GTM_UTILS_H

#include "gtm/libpq-int.h"
#include "gtm/gtm_msg.h"

#if 0
/*
 * PGXCTODO: This portion of code needs XCM support
 * to be able to report GTM failures to XC watcher and
 * enable a GTM reconnection kick.
 */
void gtm_report_failure(GTM_Conn *);
#endif

void gtm_util_init_nametabs(void);
char *gtm_util_message_name(GTM_MessageType type);
char *gtm_util_result_name(GTM_ResultType type);

#endif /* GTM_UTIL_H */
