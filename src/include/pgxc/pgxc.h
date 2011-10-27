/*-------------------------------------------------------------------------
 *
 * pgxc.h
 *		PG-XC
 *
 *
 * Portions Copyright (c) 1996-2010  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */
#ifdef PGXC

#include "storage/lwlock.h"

extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;

typedef enum
{
	REMOTE_CONN_APP,
	REMOTE_CONN_COORD,
	REMOTE_CONN_DATANODE,
	REMOTE_CONN_GTM,
	REMOTE_CONN_GTM_PROXY
} RemoteConnTypes;

/* Determine remote connection type for a PGXC backend */
extern int		remoteConnType;

/* Local node name and numer */
extern char	*PGXCNodeName;
extern int	PGXCNodeId;

#define IS_PGXC_COORDINATOR isPGXCCoordinator
#define IS_PGXC_DATANODE isPGXCDataNode
#define REMOTE_CONN_TYPE remoteConnType

#define IsConnFromApp() (remoteConnType == REMOTE_CONN_APP)
#define IsConnFromCoord() (remoteConnType == REMOTE_CONN_COORD)
#define IsConnFromDatanode() (remoteConnType == REMOTE_CONN_DATANODE)
#define IsConnFromGtm() (remoteConnType == REMOTE_CONN_GTM)
#define IsConnFromGtmProxy() (remoteConnType == REMOTE_CONN_GTM_PROXY)
#endif   /* PGXC */
