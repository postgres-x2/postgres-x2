/*
 * dbsize.c
 *		Database object size functions, and related inquiries
 *
 * Copyright (c) 2002-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dbsize.c
 *
 */

#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#endif
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relmapper.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#ifdef PGXC
static Datum pgxc_database_size(Oid dbOid);
static int64 pgxc_exec_sizefunc(Oid relOid, char *funcname, char *extra_arg);

/*
 * Below macro is important when the object size functions are called
 * for system catalog tables. For pg_catalog tables and other coordinator-only
 * tables, we should return the data from coordinator. If we don't find
 * locator info, that means it is a coordinator-only table.
 */
#define COLLECT_FROM_DATANODES(relid) \
	(IS_PGXC_COORDINATOR && !IsConnFromCoord() && \
	(GetRelationLocInfo((relid)) != NULL))
#endif

/* Return physical size of directory contents, or 0 if dir doesn't exist */
static int64
db_dir_size(const char *path)
{
	int64		dirsize = 0;
	struct dirent *direntry;
	DIR		   *dirdesc;
	char		filename[MAXPGPATH];

	dirdesc = AllocateDir(path);

	if (!dirdesc)
		return 0;

	while ((direntry = ReadDir(dirdesc, path)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(filename, MAXPGPATH, "%s/%s", path, direntry->d_name);

		if (stat(filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", filename)));
		}
		dirsize += fst.st_size;
	}

	FreeDir(dirdesc);
	return dirsize;
}

/*
 * calculate size of database in all tablespaces
 */
static int64
calculate_database_size(Oid dbOid)
{
	int64		totalsize;
	DIR		   *dirdesc;
	struct dirent *direntry;
	char		dirpath[MAXPGPATH];
	char		pathname[MAXPGPATH];
	AclResult	aclresult;

	/* User must have connect privilege for target database */
	aclresult = pg_database_aclcheck(dbOid, GetUserId(), ACL_CONNECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_DATABASE,
					   get_database_name(dbOid));

	/* Shared storage in pg_global is not counted */

	/* Include pg_default storage */
	snprintf(pathname, MAXPGPATH, "base/%u", dbOid);
	totalsize = db_dir_size(pathname);

	/* Scan the non-default tablespaces */
	snprintf(dirpath, MAXPGPATH, "pg_tblspc");
	dirdesc = AllocateDir(dirpath);
	if (!dirdesc)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open tablespace directory \"%s\": %m",
						dirpath)));

	while ((direntry = ReadDir(dirdesc, dirpath)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

#ifdef PGXC
		/* Postgres-XC tablespaces include node name in path */
		snprintf(pathname, MAXPGPATH, "pg_tblspc/%s/%s_%s/%u",
				 direntry->d_name, TABLESPACE_VERSION_DIRECTORY, PGXCNodeName, dbOid);
#else
		snprintf(pathname, MAXPGPATH, "pg_tblspc/%s/%s/%u",
				 direntry->d_name, TABLESPACE_VERSION_DIRECTORY, dbOid);
#endif
		totalsize += db_dir_size(pathname);
	}

	FreeDir(dirdesc);

	/* Complain if we found no trace of the DB at all */
	if (!totalsize)
		ereport(ERROR,
				(ERRCODE_UNDEFINED_DATABASE,
				 errmsg("database with OID %u does not exist", dbOid)));

	return totalsize;
}

Datum
pg_database_size_oid(PG_FUNCTION_ARGS)
{
	Oid			dbOid = PG_GETARG_OID(0);

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_DATUM(pgxc_database_size(dbOid));
#endif

	PG_RETURN_INT64(calculate_database_size(dbOid));
}

Datum
pg_database_size_name(PG_FUNCTION_ARGS)
{
	Name		dbName = PG_GETARG_NAME(0);
	Oid			dbOid = get_database_oid(NameStr(*dbName), false);

#ifdef PGXC
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		PG_RETURN_DATUM(pgxc_database_size(dbOid));
#endif

	PG_RETURN_INT64(calculate_database_size(dbOid));
}


/*
 * calculate total size of tablespace
 */
static int64
calculate_tablespace_size(Oid tblspcOid)
{
	char		tblspcPath[MAXPGPATH];
	char		pathname[MAXPGPATH];
	int64		totalsize = 0;
	DIR		   *dirdesc;
	struct dirent *direntry;
	AclResult	aclresult;

	/*
	 * User must have CREATE privilege for target tablespace, either
	 * explicitly granted or implicitly because it is default for current
	 * database.
	 */
	if (tblspcOid != MyDatabaseTableSpace)
	{
		aclresult = pg_tablespace_aclcheck(tblspcOid, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tblspcOid));
	}

	if (tblspcOid == DEFAULTTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "base");
	else if (tblspcOid == GLOBALTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "global");
	else
#ifdef PGXC
		/* Postgres-XC tablespaces include node name in path */
		snprintf(tblspcPath, MAXPGPATH, "pg_tblspc/%u/%s_%s", tblspcOid,
				 TABLESPACE_VERSION_DIRECTORY, PGXCNodeName);
#else
		snprintf(tblspcPath, MAXPGPATH, "pg_tblspc/%u/%s", tblspcOid,
				 TABLESPACE_VERSION_DIRECTORY);
#endif

	dirdesc = AllocateDir(tblspcPath);

	if (!dirdesc)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open tablespace directory \"%s\": %m",
						tblspcPath)));

	while ((direntry = ReadDir(dirdesc, tblspcPath)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, MAXPGPATH, "%s/%s", tblspcPath, direntry->d_name);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", pathname)));
		}

		if (S_ISDIR(fst.st_mode))
			totalsize += db_dir_size(pathname);

		totalsize += fst.st_size;
	}

	FreeDir(dirdesc);

	return totalsize;
}

Datum
pg_tablespace_size_oid(PG_FUNCTION_ARGS)
{
	Oid			tblspcOid = PG_GETARG_OID(0);

	PG_RETURN_INT64(calculate_tablespace_size(tblspcOid));
}

Datum
pg_tablespace_size_name(PG_FUNCTION_ARGS)
{
	Name		tblspcName = PG_GETARG_NAME(0);
	Oid			tblspcOid = get_tablespace_oid(NameStr(*tblspcName), false);

	PG_RETURN_INT64(calculate_tablespace_size(tblspcOid));
}


/*
 * calculate size of (one fork of) a relation
 */
static int64
calculate_relation_size(RelFileNode *rfn, BackendId backend, ForkNumber forknum)
{
	int64		totalsize = 0;
	char	   *relationpath;
	char		pathname[MAXPGPATH];
	unsigned int segcount = 0;

	relationpath = relpathbackend(*rfn, backend, forknum);

	for (segcount = 0;; segcount++)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (segcount == 0)
			snprintf(pathname, MAXPGPATH, "%s",
					 relationpath);
		else
			snprintf(pathname, MAXPGPATH, "%s.%u",
					 relationpath, segcount);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				break;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", pathname)));
		}
		totalsize += fst.st_size;
	}

	return totalsize;
}

Datum
pg_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	text	   *forkName = PG_GETARG_TEXT_P(1);
	Relation	rel;
	int64		size;

#ifdef PGXC
	if (COLLECT_FROM_DATANODES(relOid))
	{
		size = pgxc_exec_sizefunc(relOid, "pg_relation_size", text_to_cstring(forkName));
		PG_RETURN_INT64(size);
	}
#endif /* PGXC */

	rel = relation_open(relOid, AccessShareLock);

	size = calculate_relation_size(&(rel->rd_node), rel->rd_backend,
							  forkname_to_number(text_to_cstring(forkName)));

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * Calculate total on-disk size of a TOAST relation, including its index.
 * Must not be applied to non-TOAST relations.
 */
static int64
calculate_toast_table_size(Oid toastrelid)
{
	int64		size = 0;
	Relation	toastRel;
	Relation	toastIdxRel;
	ForkNumber	forkNum;

	toastRel = relation_open(toastrelid, AccessShareLock);

	/* toast heap size, including FSM and VM size */
	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		size += calculate_relation_size(&(toastRel->rd_node),
										toastRel->rd_backend, forkNum);

	/* toast index size, including FSM and VM size */
	toastIdxRel = relation_open(toastRel->rd_rel->reltoastidxid, AccessShareLock);
	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		size += calculate_relation_size(&(toastIdxRel->rd_node),
										toastIdxRel->rd_backend, forkNum);

	relation_close(toastIdxRel, AccessShareLock);
	relation_close(toastRel, AccessShareLock);

	return size;
}

/*
 * Calculate total on-disk size of a given table,
 * including FSM and VM, plus TOAST table if any.
 * Indexes other than the TOAST table's index are not included.
 *
 * Note that this also behaves sanely if applied to an index or toast table;
 * those won't have attached toast tables, but they can have multiple forks.
 */
static int64
calculate_table_size(Oid relOid)
{
	int64		size = 0;
	Relation	rel;
	ForkNumber	forkNum;

	rel = relation_open(relOid, AccessShareLock);

	/*
	 * heap size, including FSM and VM
	 */
	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		size += calculate_relation_size(&(rel->rd_node), rel->rd_backend,
										forkNum);

	/*
	 * Size of toast relation
	 */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
		size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);

	relation_close(rel, AccessShareLock);

	return size;
}

/*
 * Calculate total on-disk size of all indexes attached to the given table.
 *
 * Can be applied safely to an index, but you'll just get zero.
 */
static int64
calculate_indexes_size(Oid relOid)
{
	int64		size = 0;
	Relation	rel;

	rel = relation_open(relOid, AccessShareLock);

	/*
	 * Aggregate all indexes on the given relation
	 */
	if (rel->rd_rel->relhasindex)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *cell;

		foreach(cell, index_oids)
		{
			Oid			idxOid = lfirst_oid(cell);
			Relation	idxRel;
			ForkNumber	forkNum;

			idxRel = relation_open(idxOid, AccessShareLock);

			for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
				size += calculate_relation_size(&(idxRel->rd_node),
												idxRel->rd_backend,
												forkNum);

			relation_close(idxRel, AccessShareLock);
		}

		list_free(index_oids);
	}

	relation_close(rel, AccessShareLock);

	return size;
}

Datum
pg_table_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);

#ifdef PGXC
	if (COLLECT_FROM_DATANODES(relOid))
		PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_table_size", NULL));
#endif /* PGXC */

	PG_RETURN_INT64(calculate_table_size(relOid));
}

Datum
pg_indexes_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);

#ifdef PGXC
	if (COLLECT_FROM_DATANODES(relOid))
		PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_indexes_size", NULL));
#endif /* PGXC */

	PG_RETURN_INT64(calculate_indexes_size(relOid));
}

/*
 *	Compute the on-disk size of all files for the relation,
 *	including heap data, index data, toast data, FSM, VM.
 */
static int64
calculate_total_relation_size(Oid Relid)
{
	int64		size;

	/*
	 * Aggregate the table size, this includes size of the heap, toast and
	 * toast index with free space and visibility map
	 */
	size = calculate_table_size(Relid);

	/*
	 * Add size of all attached indexes as well
	 */
	size += calculate_indexes_size(Relid);

	return size;
}

Datum
pg_total_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

#ifdef PGXC
	if (COLLECT_FROM_DATANODES(relid))
		PG_RETURN_INT64(pgxc_exec_sizefunc(relid, "pg_total_relation_size", NULL));
#endif /* PGXC */

	PG_RETURN_INT64(calculate_total_relation_size(relid));
}

/*
 * formatting with size units
 */
Datum
pg_size_pretty(PG_FUNCTION_ARGS)
{
	int64		size = PG_GETARG_INT64(0);
	char		buf[64];
	int64		limit = 10 * 1024;
	int64		limit2 = limit * 2 - 1;

	if (size < limit)
		snprintf(buf, sizeof(buf), INT64_FORMAT " bytes", size);
	else
	{
		size >>= 9;				/* keep one extra bit for rounding */
		if (size < limit2)
			snprintf(buf, sizeof(buf), INT64_FORMAT " kB",
					 (size + 1) / 2);
		else
		{
			size >>= 10;
			if (size < limit2)
				snprintf(buf, sizeof(buf), INT64_FORMAT " MB",
						 (size + 1) / 2);
			else
			{
				size >>= 10;
				if (size < limit2)
					snprintf(buf, sizeof(buf), INT64_FORMAT " GB",
							 (size + 1) / 2);
				else
				{
					size >>= 10;
					snprintf(buf, sizeof(buf), INT64_FORMAT " TB",
							 (size + 1) / 2);
				}
			}
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

/*
 * Get the filenode of a relation
 *
 * This is expected to be used in queries like
 *		SELECT pg_relation_filenode(oid) FROM pg_class;
 * That leads to a couple of choices.  We work from the pg_class row alone
 * rather than actually opening each relation, for efficiency.	We don't
 * fail if we can't find the relation --- some rows might be visible in
 * the query's MVCC snapshot but already dead according to SnapshotNow.
 * (Note: we could avoid using the catcache, but there's little point
 * because the relation mapper also works "in the now".)  We also don't
 * fail if the relation doesn't have storage.  In all these cases it
 * seems better to quietly return NULL.
 */
Datum
pg_relation_filenode(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Oid			result;
	HeapTuple	tuple;
	Form_pg_class relform;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
			/* okay, these have storage */
			if (relform->relfilenode)
				result = relform->relfilenode;
			else	/* Consult the relation mapper */
				result = RelationMapOidToFilenode(relid,
												  relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			result = InvalidOid;
			break;
	}

	ReleaseSysCache(tuple);

	if (!OidIsValid(result))
		PG_RETURN_NULL();

	PG_RETURN_OID(result);
}

/*
 * Get the pathname (relative to $PGDATA) of a relation
 *
 * See comments for pg_relation_filenode.
 */
Datum
pg_relation_filepath(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	HeapTuple	tuple;
	Form_pg_class relform;
	RelFileNode rnode;
	BackendId	backend;
	char	   *path;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
			/* okay, these have storage */

			/* This logic should match RelationInitPhysicalAddr */
			if (relform->reltablespace)
				rnode.spcNode = relform->reltablespace;
			else
				rnode.spcNode = MyDatabaseTableSpace;
			if (rnode.spcNode == GLOBALTABLESPACE_OID)
				rnode.dbNode = InvalidOid;
			else
				rnode.dbNode = MyDatabaseId;
			if (relform->relfilenode)
				rnode.relNode = relform->relfilenode;
			else	/* Consult the relation mapper */
				rnode.relNode = RelationMapOidToFilenode(relid,
													   relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			rnode.relNode = InvalidOid;
			/* some compilers generate warnings without these next two lines */
			rnode.dbNode = InvalidOid;
			rnode.spcNode = InvalidOid;
			break;
	}

	if (!OidIsValid(rnode.relNode))
	{
		ReleaseSysCache(tuple);
		PG_RETURN_NULL();
	}

	/* Determine owning backend. */
	switch (relform->relpersistence)
	{
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			break;
		case RELPERSISTENCE_TEMP:
			if (isTempOrToastNamespace(relform->relnamespace))
				backend = MyBackendId;
			else
			{
				/* Do it the hard way. */
				backend = GetTempNamespaceBackendId(relform->relnamespace);
				Assert(backend != InvalidBackendId);
			}
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relform->relpersistence);
			backend = InvalidBackendId; /* placate compiler */
			break;
	}

	ReleaseSysCache(tuple);

	path = relpathbackend(rnode, backend, MAIN_FORKNUM);

	PG_RETURN_TEXT_P(cstring_to_text(path));
}


#ifdef PGXC

/*
 * pgxc_database_size
 * Given a dboid, return sum of pg_database_size() executed on all the datanodes
 */
static Datum
pgxc_database_size(Oid dbOid)
{
	StringInfoData  buf;
	char           *dbname = get_database_name(dbOid);
	Oid				*coOids, *dnOids;
	int numdnodes, numcoords;

	if (!dbname)
		ereport(ERROR,
			(ERRCODE_UNDEFINED_DATABASE,
			 errmsg("database with OID %u does not exist", dbOid)));

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT pg_catalog.pg_database_size('%s')", dbname);

	PgxcNodeListAndCount(&coOids, &dnOids, &numcoords, &numdnodes);

	return pgxc_execute_on_nodes(numdnodes, dnOids, buf.data);
}


/*
 * pgxc_execute_on_nodes
 * Execute 'query' on all the nodes in 'nodelist', and returns int64 datum
 * which has the sum of all the results. If multiples nodes are involved, it
 * assumes that the query returns exactly one row with one attribute of type
 * int64. If there is a single node, it just returns the datum as-is without
 * checking the type of the returned value.
 */
Datum
pgxc_execute_on_nodes(int numnodes, Oid *nodelist, char *query)
{
	StringInfoData  buf;
	int             ret;
	TupleDesc       spi_tupdesc;
	int             i;
	int64           total_size = 0;
	int64           size = 0;
	bool            isnull;
	char           *nodename;
	Datum           datum;

	/*
	 * Connect to SPI manager
	 */
	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "SPI connect failure - returned %d", ret);

	initStringInfo(&buf);

	/* Get pg_***_size function results from all data nodes */
	for (i = 0; i < numnodes; i++)
	{
		nodename = get_pgxc_nodename(nodelist[i]);
		resetStringInfo(&buf);
		appendStringInfo(&buf, "EXECUTE DIRECT ON %s %s",
		                 nodename, quote_literal_cstr(query));

		ret = SPI_execute(buf.data, false, 0);
		spi_tupdesc = SPI_tuptable->tupdesc;

		if (ret != SPI_OK_SELECT)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to execute query '%s' on node '%s'",
					        query, nodename)));
		}

		/*
		 * The query must always return one row having one column:
		 */
		Assert(SPI_processed == 1 && spi_tupdesc->natts == 1);

		datum = SPI_getbinval(SPI_tuptable->vals[0], spi_tupdesc, 1, &isnull);

		/* For single node, don't assume the type of datum. It can be bool also. */
		if (numnodes == 1)
			break;

		size = DatumGetInt64(datum);
		total_size += size;
	}

	SPI_finish();

	if (numnodes == 1)
		PG_RETURN_DATUM(datum);
	else
		PG_RETURN_INT64(total_size);
}


/*
 * pgxc_exec_sizefunc
 * Execute the given object size system function on all the datanodes associated
 * with relOid, and return the sum of all.
 *
 * Args:
 *
 * relOid: Oid of the table for which the object size function is to be executed.
 *
 * funcname: Name of the system function.
 *
 * extra_arg: The first argument to such sys functions is always table name.
 * Some functions can have a second argument. To pass this argument, extra_arg
 * is used. Currently only pg_relation_size() is the only one that requires
 * a 2nd argument: fork text.
 */
static int64
pgxc_exec_sizefunc(Oid relOid, char *funcname, char *extra_arg)
{
	int             numnodes;
	Oid            *nodelist;
	char           *relname;
	StringInfoData  buf;
	Relation        rel;

	rel = relation_open(relOid, AccessShareLock);

	if (rel->rd_locator_info)
	/* get relation name including any needed schema prefix and quoting */
	relname = quote_qualified_identifier(get_namespace_name(rel->rd_rel->relnamespace),
	                                     RelationGetRelationName(rel));
	initStringInfo(&buf);
	if (!extra_arg)
		appendStringInfo(&buf, "SELECT pg_catalog.%s('%s')", funcname, relname);
	else
		appendStringInfo(&buf, "SELECT pg_catalog.%s('%s', '%s')", funcname, relname, extra_arg);

	numnodes = get_pgxc_classnodes(RelationGetRelid(rel), &nodelist);

	relation_close(rel, AccessShareLock);

	return DatumGetInt64(pgxc_execute_on_nodes(numnodes, nodelist, buf.data));
}

#endif /* PGXC */
