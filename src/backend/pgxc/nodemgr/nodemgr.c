/*-------------------------------------------------------------------------
 *
 * nodemgr.c
 *	  Routines to support manipulation of the pgxc_node catalog
 *	  Support concerns CREATE/ALTER/DROP on NODE object.
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"

/* Global number of nodes */
int         NumDataNodes = 2;
int         NumCoords = 1;

/*
 * Check list of options and return things filled.
 * This includes check on option values.
 */
static void
check_node_options(const char *node_name, List *options, char **node_host,
			int *node_port, char *node_type, 
			bool *is_primary, bool *is_preferred)
{
	ListCell   *option;

	if (!options)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("No options specified")));

	/* Filter options */
	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "port") == 0)
		{
			*node_port = defGetTypeLength(defel);

			if (*node_port < 1 || *node_port > 65535)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("port value is out of range")));
		}
		else if (strcmp(defel->defname, "host") == 0)
		{
			*node_host = defGetString(defel);
		}
		else if (strcmp(defel->defname, "type") == 0)
		{
			char *type_loc;

			type_loc = defGetString(defel);

			if (strcmp(type_loc, "coordinator") != 0 &&
				strcmp(type_loc, "datanode") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("type value is incorrect, specify 'coordinator or 'datanode'")));

			if (strcmp(type_loc, "coordinator") == 0)
				*node_type = PGXC_NODE_COORDINATOR;
			else
				*node_type = PGXC_NODE_DATANODE;
		}
		else if (strcmp(defel->defname, "primary") == 0)
		{
			*is_primary = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "preferred") == 0)
		{
			*is_preferred = defGetBoolean(defel);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("incorrect option: %s", defel->defname)));
		}
	}

	/* Checks on primary and preferred nodes */
	if (*is_primary && *node_type != PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode",
						node_name)));
	if (*is_primary && OidIsValid(primary_data_node))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: two nodes cannot be primary",
						node_name)));

	if (*is_preferred && *node_type != PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode",
						node_name)));

	/* Node type check */
	if (*node_type == PGXC_NODE_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: Node type not specified",
						node_name)));
}

/* --------------------------------
 *  cmp_nodes
 * 
 *  Compare the Oids of two XC nodes
 *  to sort them in ascending order by their names
 * --------------------------------
 */
static int
cmp_nodes(const void *p1, const void *p2)
{
	Oid n1 = *((Oid *)p1);
	Oid n2 = *((Oid *)p2);

	if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) < 0)
		return -1;

	if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) == 0)
		return 0;

	return 1;
}

/*
 * PgxcNodeCount
 *
 * Count number of PGXC nodes based on catalog information and return
 * an ordered list of node Oids for each PGXC node type.
 */
void
PgxcNodeListAndCount(Oid **coOids, Oid **dnOids, int *num_coords, int *num_dns)
{
	Relation rel;
	HeapScanDesc scan;
	HeapTuple   tuple;

	*num_coords = 0;
	*num_dns = 0;

	/* Don't forget to reinitialize primary and preferred nodes also */
	primary_data_node = InvalidOid;
	num_preferred_data_nodes = 0;

	/* 
	 * Node information initialization is made in one scan:
	 * 1) Scan pgxc_node catalog to find the number of nodes for
	 *	each node type and make proper allocations
	 * 2) Then extract the node Oid
	 * 3) Complete primary/preferred node information
	 */
	rel = heap_open(PgxcNodeRelationId, AccessShareLock);
	scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		int numnodes;
		Oid **nodes;
		Form_pgxc_node  nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);

		/* Take data for given node type */
		switch (nodeForm->node_type)
		{
			case PGXC_NODE_COORDINATOR:
				(*num_coords)++;
				numnodes = *num_coords;
				nodes = coOids;
				break;
			case PGXC_NODE_DATANODE:
				(*num_dns)++;
				numnodes = *num_dns;
				nodes = dnOids;
				break;
			default:
				break;
		}

		if (numnodes == 1)
			*nodes = (Oid *) palloc(numnodes * sizeof(Oid));
		else
			*nodes = (Oid *) repalloc(*nodes, numnodes * sizeof(Oid));

		(*nodes)[numnodes - 1] = get_pgxc_nodeoid(NameStr(nodeForm->node_name));

		/*
		 * Save data related to preferred and primary node
		 * Preferred and primaries use node Oids
		 */
		if (nodeForm->nodeis_primary)
			primary_data_node = get_pgxc_nodeoid(NameStr(nodeForm->node_name));
		if (nodeForm->nodeis_preferred)
		{
			preferred_data_node[num_preferred_data_nodes] =
				get_pgxc_nodeoid(NameStr(nodeForm->node_name));
			num_preferred_data_nodes++;
		}
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	/* Finally sort the lists to be sent back */
	if (NumCoords != 0)
		qsort(*coOids, *num_coords, sizeof(Oid), cmp_nodes);
	if (NumDataNodes != 0)
		qsort(*dnOids, *num_dns, sizeof(Oid), cmp_nodes);
}

/*
 * PgxcNodeCreate
 * 
 * Add a PGXC node
 */
void
PgxcNodeCreate(CreateNodeStmt *stmt)
{
	Relation	pgxcnodesrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_node];
	Datum		values[Natts_pgxc_node];
	const char *node_name = stmt->node_name;
	int		i;
	/* Options with default values */
	char	   *node_host = NULL;
	char		node_type = PGXC_NODE_NONE;
	int			node_port = 0;
	bool		is_primary = false;
	bool		is_preferred = false;

	/* Only a DB administrator can add nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create cluster nodes")));

	/* Check that node name is node in use */
	if (OidIsValid(get_pgxc_nodeoid(node_name)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("PGXC Node %s: object already defined",
						node_name)));

	/* Check length of node name */
	if (strlen(node_name) > PGXC_NODENAME_LENGTH)
		ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("Node name \"%s\" is too long",
                        node_name)));

	/* Filter options */
	check_node_options(node_name, stmt->options, &node_host,
				&node_port, &node_type,
				&is_primary, &is_preferred);

	/*
	 * Then assign default values if necessary
	 * First for port.
	 */
	if (node_port == 0)
	{
		node_port = 5432;
		elog(LOG, "PGXC node %s: Applying default port value: %d",
			 node_name, node_port);
	}

	/* Then apply default value for host */
	if (!node_host)
	{
		node_host = strdup("localhost");
		elog(LOG, "PGXC node %s: Applying default host value: %s",
			 node_name, node_host);
	}

	/* Iterate through all attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_node; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/*
	 * Open the relation for insertion
	 * This is necessary to generate a unique Oid for the new node
	 * There could be a relation race here if a similar Oid
	 * being created before the heap is inserted.
	 */
	pgxcnodesrel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

	/* Build entry tuple */
	values[Anum_pgxc_node_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_name));
	values[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
	values[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
	values[Anum_pgxc_node_host - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host));
	values[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
	values[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);

	htup = heap_form_tuple(pgxcnodesrel->rd_att, values, nulls);

	/* Insert tuple in catalog */
	simple_heap_insert(pgxcnodesrel, htup);
		
	CatalogUpdateIndexes(pgxcnodesrel, htup);

	heap_close(pgxcnodesrel, RowExclusiveLock);
}

/*
 * PgxcNodeAlter
 * 
 * Alter a PGXC node
 */
void
PgxcNodeAlter(AlterNodeStmt *stmt)
{
	const char *node_name = stmt->node_name;
	char	   *node_host;
	char		node_type, node_type_old;
	int			node_port;
	bool		is_preferred;
	bool		is_primary;
	HeapTuple	oldtup, newtup;
	Oid			nodeOid = get_pgxc_nodeoid(node_name);
	Relation	rel;
	Datum		new_record[Natts_pgxc_node];
	bool		new_record_nulls[Natts_pgxc_node];
	bool		new_record_repl[Natts_pgxc_node];

	/* Only a DB administrator can alter cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to change cluster nodes")));

	/* Look at the node tuple, and take exclusive lock on it */
	rel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

	/* Check that node exists */
	if (!OidIsValid(nodeOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));

	/* Open new tuple, checks are performed on it and new values */
	oldtup = SearchSysCacheCopy1(PGXCNODEOID, ObjectIdGetDatum(nodeOid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for object %u", nodeOid);

	/*
	 * check_options performs some internal checks on option values
	 * so set up values.
	 */
	node_host = get_pgxc_nodehost(nodeOid);
	node_port = get_pgxc_nodeport(nodeOid);
	is_preferred = is_pgxc_nodepreferred(nodeOid);
	is_primary = is_pgxc_nodeprimary(nodeOid);
	node_type = get_pgxc_nodetype(nodeOid);
	node_type_old = node_type;

	/* Filter options */
	check_node_options(node_name, stmt->options, &node_host,
				&node_port, &node_type,
				&is_primary, &is_preferred);

	/* Check type dependency */
	if (node_type_old == PGXC_NODE_COORDINATOR &&
		node_type == PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot alter Coordinator to Datanode",
						node_name)));
	else if (node_type_old == PGXC_NODE_DATANODE &&
			 node_type == PGXC_NODE_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot alter Datanode to Coordinator",
						node_name)));

	/* Update values for catalog entry */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));
	new_record[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
	new_record_repl[Anum_pgxc_node_port - 1] = true;
	new_record[Anum_pgxc_node_host - 1] = 
		DirectFunctionCall1(namein, CStringGetDatum(node_host));
	new_record_repl[Anum_pgxc_node_host - 1] = true;
	new_record[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
	new_record_repl[Anum_pgxc_node_type - 1] = true;
	new_record[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
	new_record_repl[Anum_pgxc_node_is_primary - 1] = true;
	new_record[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);
	new_record_repl[Anum_pgxc_node_is_preferred - 1] = true;

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	simple_heap_update(rel, &oldtup->t_self, newtup);

	/* Update indexes */
	CatalogUpdateIndexes(rel, newtup);

	/* Release lock at Commit */
	heap_close(rel, NoLock);
}


/*
 * PgxcNodeRemove
 * 
 * Remove a PGXC node
 */
void
PgxcNodeRemove(DropNodeStmt *stmt)
{
	Relation	relation;
	HeapTuple	tup;
	const char	*node_name = stmt->node_name;
	Oid		noid = get_pgxc_nodeoid(node_name);

	/* Only a DB administrator can remove cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to remove cluster nodes")));

	/* Check if node is defined */
	if (!OidIsValid(noid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));

	if (strcmp(node_name, PGXCNodeName) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC Node %s: cannot drop local node",
						node_name)));

	/* PGXCTODO:
	 * Is there any group which has this node as member
	 * XC Tables will also have this as a member in their array
	 * Do this search in the local data structure.
	 * If a node is removed, it is necessary to check if there is a distributed
	 * table on it. If there are only replicated table it is OK.
	 * However, we have to be sure that there are no pooler agents in the cluster pointing to it.
	 */

	/* Delete the pgxc_node tuple */
	relation = heap_open(PgxcNodeRelationId, RowExclusiveLock);
	tup = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(noid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}
