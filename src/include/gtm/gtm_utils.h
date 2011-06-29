/*-------------------------------------------------------------------------
 *
 * gtm_utils.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * src/include/gtm/gtm_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_UTILS_H
#define GTM_UTILS_H

#include "gtm/libpq-int.h"

#if 0
/*
 * PGXCTODO: This portion of code needs XCM support
 * to be able to report GTM failures to XC watcher and
 * enable a GTM reconnection kick.
 */
void gtm_report_failure(GTM_Conn *);
#endif

#endif /* GTM_UTIL_H */
