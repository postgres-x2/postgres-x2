/*-------------------------------------------------------------------------
 *
 * pgxc_class.c
 *	routines to support manipulation of the pgxc_class relation
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "pgxc/locator.h"
#include "utils/array.h"

void
PgxcClassCreate(Oid pcrelid,
				char  pclocatortype,
				int pcattnum,
				int pchashalgorithm,
				int pchashbuckets,
				int numnodes,
				Oid *nodes)
{
	Relation	pgxcclassrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_class];
	Datum		values[Natts_pgxc_class];
	int		i;
	oidvector	*nodes_array;

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(nodes, numnodes);

	/* Iterate through edb_linkauth attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_class; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/* should not happen */
	if (pcrelid == InvalidOid)
	{
		elog(ERROR,"pgxc class relid invalid.");
		return;
	}

	values[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);
	values[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	if (pclocatortype == LOCATOR_TYPE_HASH || pclocatortype == LOCATOR_TYPE_MODULO)
	{
		values[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);
		values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
		values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
	}

	/* Node information */
	values[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);

	/* Open the relation for insertion */
	pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

	(void) simple_heap_insert(pgxcclassrel, htup);

	CatalogUpdateIndexes(pgxcclassrel, htup);

	heap_close(pgxcclassrel, RowExclusiveLock);
}

/*
 * RemovePGXCClass():
 *		Remove extended PGXC information
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


