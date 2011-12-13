/*-------------------------------------------------------------------------
 *
 * nodemgr.c
 *	  Routines to support manipulation of the pgxc_node catalog
 *	  Support concerns CREATE/ALTER/DROP on NODE object.
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
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
check_options(List *options, DefElem **dhost,
			DefElem **dport, DefElem **dtype, 
			DefElem **is_primary, DefElem **is_preferred)
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
			int port_value;

			if (*dport)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			/* Value is mandatory */
			if (!defel->arg)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("port value is not specified")));

			*dport = defel;

			/* Value type check */
			if (!IsA(defel->arg, Integer))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("port value is not an integer")));

			port_value = intVal(defel->arg);
			if (port_value < 1 || port_value > 65535)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("port value is out of range")));

		}
		else if (strcmp(defel->defname, "host") == 0)
		{
			if (*dhost)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			/* Value is mandatory */
			if (!defel->arg)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("host value is not specified")));

			/* Value type check */
			if (!IsA(defel->arg, String))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("host value is not a string")));

			*dhost = defel;
		}
		else if (strcmp(defel->defname, "type") == 0)
		{
			char *nodetype;

			if (*dtype)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			*dtype = defel;

			/* Value is mandatory */
			if (!defel->arg)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("type value is not specified")));

			/* Value type check */
			if (!IsA(defel->arg, String))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("type value is not a string")));

			nodetype = strVal(defel->arg);
			if (strcmp(nodetype, "coordinator") != 0 &&
				strcmp(nodetype, "datanode") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("type value is incorrect, specify 'coordinator or 'datanode'")));
		}
		else if (strcmp(defel->defname, "primary") == 0)
		{
			if (*is_primary)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			if (defel->arg)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("a value cannot be specified with primary")));

			*is_primary = defel;
		}
		else if (strcmp(defel->defname, "preferred") == 0)
		{
			if (*is_preferred)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			if (defel->arg)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("a value cannot be specified with preferred")));

			*is_preferred = defel;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("incorrect option: %s", defel->defname)));
		}
	}
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
	const char	*node_name = stmt->node_name;
	int		i;
	/* Options */
	DefElem		*dhost = NULL;
	DefElem		*dport = NULL;
	DefElem		*dtype = NULL;
	DefElem		*is_primary = NULL;
	DefElem		*is_preferred = NULL;
	const char	*node_host = NULL;
	char		node_type;
	int			node_port;
	bool		nodeis_primary = false;
	bool		nodeis_preferred = false;

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

	/* Filter options */
	check_options(stmt->options, &dhost,
				&dport, &dtype,
				&is_primary, &is_preferred);

	/* Then assign default values if necessary */
	if (dport)
	{
		node_port = intVal(dport->arg);
	}
	else
	{
		/* Apply default */
		node_port = 5432;
		elog(LOG, "PGXC node %s: Applying default port value: %d",
			 node_name, node_port);
	}

	/* For host */
	if (dhost)
	{
		node_host = strVal(dhost->arg);
	}
	else
	{
		/* Apply default */
		node_host = strdup("localhost");
		elog(LOG, "PGXC node %s: Applying default host value: %s",
			 node_name, node_host);
	}

	/* For node type */
	if (dtype)
	{
		char *loc;
		loc = strVal(dtype->arg);
		if (strcmp(loc, "coordinator") == 0)
			node_type = PGXC_NODE_COORDINATOR;
		else
			node_type = PGXC_NODE_DATANODE;
	}
	else
	{
		/* Type not specified? */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: Node type not specified",
						node_name)));
	}

	/* Iterate through all attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_node; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	if (is_primary)
	{
		if (node_type != PGXC_NODE_DATANODE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode",
							node_name)));

		if (OidIsValid(primary_data_node))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("PGXC node %s: two nodes cannot be primary",
							node_name)));
		nodeis_primary = true;
	}

	if (is_preferred)
	{
		if (node_type != PGXC_NODE_DATANODE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode",
							node_name)));
		nodeis_preferred = true;
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
	values[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(nodeis_primary);
	values[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(nodeis_preferred);

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
	DefElem		*dhost = NULL;
	DefElem		*dport = NULL;
	DefElem		*dtype = NULL;
	DefElem		*is_primary = NULL;
	DefElem		*is_preferred = NULL;
	const char	*node_name = stmt->node_name;
	const char	*node_host = NULL;
	char		node_type = PGXC_NODE_NONE;
	int			node_port = 0;
	bool		nodeis_preferred = false;
	bool		nodeis_primary = false;
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

	/* Filter options */
	check_options(stmt->options, &dhost,
				&dport, &dtype,
				&is_primary, &is_preferred);

	/* Host value */
	if (dhost)
		node_host = strVal(dhost->arg);

	/* Port value */
	if (dport)
		node_port = intVal(dport->arg);

	/* Primary node */
	if (is_primary)
	{
		if (get_pgxc_nodetype(nodeOid) != PGXC_NODE_DATANODE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode",
							node_name)));

		if (OidIsValid(primary_data_node))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("PGXC node %s: two nodes cannot be primary",
							node_name)));
		nodeis_primary = true;
	}

	/* Preferred node */
	if (is_preferred)
	{
		if (get_pgxc_nodetype(nodeOid) != PGXC_NODE_DATANODE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode",
							node_name)));
		nodeis_preferred = true;
	}

	/* For node type */
	if (dtype)
	{
		char *loc;
		Form_pgxc_node loctup = (Form_pgxc_node) GETSTRUCT(oldtup);
		char node_type_old = loctup->node_type;

		loc = strVal(dtype->arg);
		if (strcmp(loc, "coordinator") == 0)
			node_type = PGXC_NODE_COORDINATOR;
		else
			node_type = PGXC_NODE_DATANODE;

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
	}

	/* Update values for catalog entry */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));
	if (node_port > 0)
	{
		new_record[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
		new_record_repl[Anum_pgxc_node_port - 1] = true;
	}
	if (node_host)
	{
		new_record[Anum_pgxc_node_host - 1] = 
			DirectFunctionCall1(namein, CStringGetDatum(node_host));
		new_record_repl[Anum_pgxc_node_host - 1] = true;
	}
	if (node_type != PGXC_NODE_NONE)
	{
		new_record[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
		new_record_repl[Anum_pgxc_node_type - 1] = true;
	}
	if (is_primary)
	{
		new_record[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(nodeis_primary);
		new_record_repl[Anum_pgxc_node_is_primary - 1] = true;
	}
	if (is_preferred)
	{
		new_record[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(nodeis_preferred);
		new_record_repl[Anum_pgxc_node_is_preferred - 1] = true;
	}

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
