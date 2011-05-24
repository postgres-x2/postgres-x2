/*-------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
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

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsReplicated(x) (x->locatorType == LOCATOR_TYPE_REPLICATED)

#define PGXC_COORDINATOR_SCHEMA	"__pgxc_coordinator_schema__"
#define PGXC_DATA_NODE_SCHEMA	"__pgxc_datanode_schema__"
#define PREPARED_XACTS_TABLE	"pg_prepared_xacts"

#include "nodes/primnodes.h"
#include "utils/relcache.h"


typedef int PartAttrNumber;

/* track if tables use pg_catalog */
typedef enum
{
	TABLE_USAGE_TYPE_NO_TABLE,
	TABLE_USAGE_TYPE_PGCATALOG,
	TABLE_USAGE_TYPE_SEQUENCE,
	TABLE_USAGE_TYPE_USER,
	TABLE_USAGE_TYPE_USER_REPLICATED,  /* based on a replicated table */
	TABLE_USAGE_TYPE_MIXED
} TableUsageType;

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
	Oid			relid;
	char		locatorType;
	PartAttrNumber partAttrNum; /* if partitioned */
	char	   *partAttrName;	/* if partitioned */
	int			nodeCount;
	List	   *nodeList;
	ListCell   *roundRobinNode; /* points to next one to use */
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
	List	   *primarynodelist;
	List	   *nodelist;
	char	    baselocatortype;
	TableUsageType tableusagetype;  /* track pg_catalog usage */
	Expr	   *expr; /* expression to evaluate at execution time if planner
					   * can not determine execution nodes */
	Oid			relid; /* Relation to determine execution nodes */
	RelationAccessType accesstype; /* Access type to determine execution nodes */
} ExecNodes;


extern char *PreferredDataNodes;

extern void InitRelationLocInfo(void);
extern char GetLocatorType(Oid relid);
extern char ConvertToLocatorType(int disttype);

extern char *GetRelationHashColumn(RelationLocInfo *rel_loc_info);
extern RelationLocInfo *GetRelationLocInfo(Oid relid);
extern RelationLocInfo *CopyRelationLocInfo(RelationLocInfo *src_info);
extern ExecNodes *GetRelationNodes(RelationLocInfo *rel_loc_info, Datum valueForDistCol, Oid typeOfValueForDistCol, RelationAccessType accessType);
extern bool IsHashColumn(RelationLocInfo *rel_loc_info, char *part_col_name);
extern bool IsHashColumnForRelId(Oid relid, char *part_col_name);
extern int	GetRoundRobinNode(Oid relid);

extern bool IsHashDistributable(Oid col_type);
extern List *GetAllDataNodes(void);
extern List *GetAllCoordNodes(void);
extern List *GetAnyDataNode(void);
extern void RelationBuildLocator(Relation rel);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);

extern bool IsModuloDistributable(Oid col_type);
extern char *GetRelationModuloColumn(RelationLocInfo * rel_loc_info);
extern bool IsModuloColumn(RelationLocInfo *rel_loc_info, char *part_col_name);
extern bool IsModuloColumnForRelId(Oid relid, char *part_col_name);
extern char *GetRelationDistColumn(RelationLocInfo * rel_loc_info);
extern bool IsDistColumnForRelId(Oid relid, char *part_col_name);

#endif   /* LOCATOR_H */
