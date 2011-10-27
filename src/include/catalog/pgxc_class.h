/*
 * Copyright (c) 2004-2007 EnterpriseDB Corporation. All Rights Reserved.
 */
#ifndef PGXC_CLASS_H
#define PGXC_CLASS_H

#include "nodes/parsenodes.h"

#define PgxcClassRelationId  9001

CATALOG(pgxc_class,9001) BKI_WITHOUT_OIDS
{
	Oid		pcrelid;		/* Table Oid */
	char		pclocatortype;		/* Type of distribution */
	int2		pcattnum;		/* Column number of distribution */
	int2 		pchashalgorithm;	/* Hashing algorithm */
	int2 		pchashbuckets;		/* Number of buckets */

	/* VARIABLE LENGTH FIELDS: */
	oidvector	nodeoids;		/* List of nodes used by table */
} FormData_pgxc_class;

typedef FormData_pgxc_class *Form_pgxc_class;

#define Natts_pgxc_class			6

#define Anum_pgxc_class_pcrelid			1
#define Anum_pgxc_class_pclocatortype		2
#define Anum_pgxc_class_pcattnum		3
#define Anum_pgxc_class_pchashalgorithm		4
#define Anum_pgxc_class_pchashbuckets		5
#define Anum_pgxc_class_nodes			6

extern void PgxcClassCreate(Oid pcrelid,
							char  pclocatortype,
							int pcattnum,
							int pchashalgorithm,
							int pchashbuckets,
							int numnodes,
							Oid *nodes);
extern void RemovePgxcClass(Oid pcrelid);

#endif   /* PGXC_CLASS_H */

