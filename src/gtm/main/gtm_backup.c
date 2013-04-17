#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_backup.h"
#include "gtm/elog.h"

GTM_RWLock gtm_bkup_lock;

extern char GTMControlFile[];

void GTM_WriteRestorePoint(void)
{
	FILE *f = fopen(GTMControlFile, "w");

	if (f == NULL)
	{
		ereport(LOG, (errno,
					  errmsg("Cannot open control file"),
					  errhint("%s", strerror(errno))));
		return;
	}
	GTM_WriteRestorePointXid(f);
	GTM_WriteRestorePointSeq(f);
	fclose(f);
}

void GTM_MakeBackup(char *path)
{
	FILE *f = fopen(path, "w");

	if (f == NULL)
	{
		ereport(LOG, (errno,
					  errmsg("Cannot open backup file %s", path),
					  errhint("%s", strerror(errno))));
		return;
	}
	GTM_SaveTxnInfo(f);
	GTM_SaveSeqInfo(f);
	fclose(f);
}

