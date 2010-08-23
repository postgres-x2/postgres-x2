/*-------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
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

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsReplicated(x) (x->locatorType == LOCATOR_TYPE_REPLICATED)
#include "utils/relcache.h"


typedef int PartAttrNumber;

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


/* track if tables use pg_catalog */
typedef enum
{
	TABLE_USAGE_TYPE_NO_TABLE,
	TABLE_USAGE_TYPE_PGCATALOG,
	TABLE_USAGE_TYPE_USER,
	TABLE_USAGE_TYPE_USER_REPLICATED,  /* based on a replicated table */
	TABLE_USAGE_TYPE_MIXED
} TableUsageType;

/*
 * Nodes to execute on
 * primarynodelist is for replicated table writes, where to execute first.
 * If it succeeds, only then should it be executed on nodelist.
 * primarynodelist should be set to NULL if not doing replicated write operations
 */
typedef struct
{
	List	   *primarynodelist;
	List	   *nodelist;
	char	    baselocatortype;
	TableUsageType tableusagetype;  /* track pg_catalog usage */
} Exec_Nodes;


extern char *PreferredDataNodes;

extern void InitRelationLocInfo(void);
extern char GetLocatorType(Oid relid);
extern char ConvertToLocatorType(int disttype);

extern char *GetRelationHashColumn(RelationLocInfo *rel_loc_info);
extern RelationLocInfo *GetRelationLocInfo(Oid relid);
extern RelationLocInfo *CopyRelationLocInfo(RelationLocInfo *src_info);
extern Exec_Nodes *GetRelationNodes(RelationLocInfo *rel_loc_info, long *partValue,
				 int isRead);
extern bool IsHashColumn(RelationLocInfo *rel_loc_info, char *part_col_name);
extern bool IsHashColumnForRelId(Oid relid, char *part_col_name);
extern int	GetRoundRobinNode(Oid relid);

extern bool IsHashDistributable(Oid col_type);
extern List *GetAllNodes(void);
extern List *GetAnyDataNode(void);
extern void RelationBuildLocator(Relation rel);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);

#endif   /* LOCATOR_H */
