/*
 * Copyright (c) 2004-2007 EnterpriseDB Corporation. All Rights Reserved.
 */
#ifndef PGXC_CLASS_H
#define PGXC_CLASS_H

#include "nodes/parsenodes.h"

#define PgxcClassRelationId  9001

CATALOG(pgxc_class,9001) BKI_WITHOUT_OIDS
{
	Oid			pcrelid;
	char		pclocatortype;
	int2		pcattnum;
	int2 		pchashalgorithm;
	int2 		pchashbuckets;
} FormData_pgxc_class;

typedef FormData_pgxc_class *Form_pgxc_class;

#define Natts_pgxc_class					5

#define Anum_pgxc_class_pcrelid			1
#define Anum_pgxc_class_pclocatortype	2
#define Anum_pgxc_class_pcattnum			3
#define Anum_pgxc_class_pchashalgorithm	4
#define Anum_pgxc_class_pchashbuckets	5

extern void PgxcClassCreate(Oid pcrelid,
					char  pclocatortype,
					int pcattnum,
					int pchashalgorithm,
					int pchashbuckets);

extern void RemovePgxcClass(Oid pcrelid);

#endif   /* PGXC_CLASS_H */

