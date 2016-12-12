/*-------------------------------------------------------------------------
 *
 * gtm_miscinit.h
 *	  This file contains general gtm and gtm proxy initialization
 *
 * Portions Copyright (c) 2015, PostgreSQL_X2 Global Development Group
 *
 * src/include/gtm/gtm_miscinit.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_MISCINIT_H
#define GTM_MISCINIT_H

extern void CreateDataDirLockFile(const char *pidfile, const char *dataDir);

extern void CreateSocketLockFile(const char *socketfile, const char *socketDir);

#endif   /* GTM_MISCINIT_H */
