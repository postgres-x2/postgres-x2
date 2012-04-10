/*-------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
 *
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_H
#define LOCATOR_H

#define LOCATOR_TYPE_REPLICATED 'R'
#define LOCATOR_TYPE_HASH 'H'
#define LOCATOR_TYPE_RANGE 'G'
#define LOCATOR_TYPE_SINGLE 'S'
#define LOCATOR_TYPE_RROBIN 'N'
#define LOCATOR_TYPE_CUSTOM 'C'
#define LOCATOR_TYPE_MODULO 'M'
#define LOCATOR_TYPE_NONE 'O'
#define LOCATOR_TYPE_DISTRIBUTED 'D'	/* for distributed table without specific
										 * scheme, e.g. result of JOIN of
										 * replicated and distributed table */

/* Maximum number of preferred datanodes that can be defined in cluster */
#define MAX_PREFERRED_NODES 64

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsLocatorReplicated(x) (x == LOCATOR_TYPE_REPLICATED)
#define IsLocatorNone(x) (x == LOCATOR_TYPE_NONE)
#define IsLocatorReplicated(x) (x == LOCATOR_TYPE_REPLICATED)
#define IsLocatorColumnDistributed(x) (x == LOCATOR_TYPE_HASH || \
									   x == LOCATOR_TYPE_RROBIN || \
									   x == LOCATOR_TYPE_MODULO || \
									   x == LOCATOR_TYPE_DISTRIBUTED)
#define IsLocatorDistributedByValue(x) (x == LOCATOR_TYPE_HASH || \
										x == LOCATOR_TYPE_MODULO || \
										x == LOCATOR_TYPE_RANGE)

#include "nodes/primnodes.h"
#include "utils/relcache.h"

typedef int PartAttrNumber;

/*
 * How relation is accessed in the query
 */
typedef enum
{
	RELATION_ACCESS_READ,				/* SELECT */
	RELATION_ACCESS_READ_FOR_UPDATE,	/* SELECT FOR UPDATE */
	RELATION_ACCESS_UPDATE,				/* UPDATE OR DELETE */
	RELATION_ACCESS_INSERT				/* INSERT */
} RelationAccessType;

typedef struct
{
	Oid		relid;
	char		locatorType;
	PartAttrNumber	partAttrNum;	/* if partitioned */
	char		*partAttrName;		/* if partitioned */
	List		*nodeList;			/* Node Indices */
	ListCell	*roundRobinNode;	/* index of the next one to use */
} RelationLocInfo;

/*
 * Nodes to execute on
 * primarynodelist is for replicated table writes, where to execute first.
 * If it succeeds, only then should it be executed on nodelist.
 * primarynodelist should be set to NULL if not doing replicated write operations
 */
typedef struct
{
	NodeTag		type;
	List		*primarynodelist;
	List		*nodeList;
	char		baselocatortype;
	Expr		*en_expr;		/* expression to evaluate at execution time if planner
						 * can not determine execution nodes */
	Oid		en_relid;		/* Relation to determine execution nodes */
	RelationAccessType accesstype;		/* Access type to determine execution nodes */
} ExecNodes;

/* Extern variables related to locations */
extern Oid primary_data_node;
extern Oid preferred_data_node[MAX_PREFERRED_NODES];
extern int num_preferred_data_nodes;

extern void InitRelationLocInfo(void);
extern char GetLocatorType(Oid relid);
extern char ConvertToLocatorType(int disttype);

extern char *GetRelationHashColumn(RelationLocInfo *rel_loc_info);
extern RelationLocInfo *GetRelationLocInfo(Oid relid);
extern RelationLocInfo *CopyRelationLocInfo(RelationLocInfo *src_info);
extern char GetRelationLocType(Oid relid);
extern bool IsTableDistOnPrimary(RelationLocInfo *rel_loc_info);
extern ExecNodes *GetRelationNodes(RelationLocInfo *rel_loc_info, Datum valueForDistCol,
									bool isValueNull, Oid typeOfValueForDistCol,
									RelationAccessType accessType);
extern bool IsHashColumn(RelationLocInfo *rel_loc_info, char *part_col_name);
extern bool IsHashColumnForRelId(Oid relid, char *part_col_name);
extern int	GetRoundRobinNode(Oid relid);

extern bool IsHashDistributable(Oid col_type);
extern List *GetAllDataNodes(void);
extern List *GetAllCoordNodes(void);
extern List *GetAnyDataNode(List *relNodes);
extern void RelationBuildLocator(Relation rel);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);

extern bool IsModuloDistributable(Oid col_type);
extern char *GetRelationModuloColumn(RelationLocInfo * rel_loc_info);
extern bool IsModuloColumn(RelationLocInfo *rel_loc_info, char *part_col_name);
extern bool IsModuloColumnForRelId(Oid relid, char *part_col_name);
extern char *GetRelationDistColumn(RelationLocInfo * rel_loc_info);
extern bool IsDistColumnForRelId(Oid relid, char *part_col_name);
extern void FreeExecNodes(ExecNodes **exec_nodes);

#endif   /* LOCATOR_H */
