/*-------------------------------------------------------------------------
 *
 * pgxc_class.c
 *	routines to support manipulation of the pgxc_class relation
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pgxc_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "pgxc/locator.h"

void
PgxcClassCreate(Oid pcrelid,
				char  pclocatortype,
				int pcattnum,
				int pchashalgorithm,
				int pchashbuckets)
{
	Relation pgxcclassrel;
	HeapTuple  htup;
	bool	   nulls[Natts_pgxc_class];
	Datum	  values[Natts_pgxc_class];
	int		i;

	/* Iterate through edb_linkauth attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_class; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}
	
	/* should not happen */
	if(pcrelid == InvalidOid)
	{
		elog(ERROR,"pgxc class relid invalid.");
		return;
	}

	values[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);	
	values[Anum_pgxc_class_pclocatortype - 1] = ObjectIdGetDatum(pclocatortype);

	if (pclocatortype == LOCATOR_TYPE_HASH)
	{
		values[Anum_pgxc_class_pcattnum - 1] = ObjectIdGetDatum(pcattnum);
		values[Anum_pgxc_class_pchashalgorithm - 1] = ObjectIdGetDatum(pchashalgorithm);
		values[Anum_pgxc_class_pchashbuckets - 1] = ObjectIdGetDatum(pchashbuckets);
	} 

	/* Open the edb_linkauth relation for insertion */
	pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

	(void) simple_heap_insert(pgxcclassrel, htup);
		
	CatalogUpdateIndexes(pgxcclassrel, htup);

	heap_close(pgxcclassrel, RowExclusiveLock);
}

#ifdef PGXC
/*
 * RemovePGXCClass():
 * 
 * Remove extended PGXC information
 *
 * arg1: Oid of the relation.
 *
 */
void
RemovePgxcClass(Oid pcrelid)
{
	Relation  relation;
	HeapTuple tup;

	/*
	 * Delete the pgxc_class tuple.
	 */
	relation = heap_open(PgxcClassRelationId, RowExclusiveLock);
	tup = SearchSysCache(PGXCCLASSRELID,
						 ObjectIdGetDatum(pcrelid),
						 0, 0, 0);

	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}
#endif  /* PGXC */


