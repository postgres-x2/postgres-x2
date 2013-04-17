/*-------------------------------------------------------------------------
 *
 * gtm_backup.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2013 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_BACKUP_H
#define _GTM_BACKUP_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_seq.h"

extern GTM_RWLock	gtm_bkup_lock;

#define RestoreDuration	2000

void GTM_WriteRestorePoint(void);
void GTM_MakeBackup(char *path);

#endif /* GTM_BACKUP_H */
