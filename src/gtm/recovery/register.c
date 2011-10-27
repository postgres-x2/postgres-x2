/*-------------------------------------------------------------------------
 *
 * register.c
 *  PGXC Node Register on GTM and GTM Proxy, node registering functions
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/stringinfo.h"

#include "gtm/gtm_ip.h"

#define GTM_NODE_FILE			"register.node"
#define NODE_HASH_TABLE_SIZE	16
#define GTM_NODE_FILE_MAX_PATH	1024

typedef struct GTM_NodeInfoHashBucket
{
	gtm_List        *nhb_list;
	GTM_RWLock  nhb_lock;
} GTM_PGXCNodeInfoHashBucket;

static char GTMPGXCNodeFile[GTM_NODE_FILE_MAX_PATH];

/* Lock access of record file when necessary */
static GTM_RWLock RegisterFileLock;

static int NodeRegisterMagic = 0xeaeaeaea;
static int NodeUnregisterMagic = 0xebebebeb; 
static int NodeEndMagic = 0xefefefef;

static GTM_PGXCNodeInfoHashBucket GTM_PGXCNodes[NODE_HASH_TABLE_SIZE];

static GTM_PGXCNodeInfo *pgxcnode_find_info(GTM_PGXCNodeType type, char *node_name);
static uint32 pgxcnode_gethash(char *nodename);
static int pgxcnode_remove_info(GTM_PGXCNodeInfo *node);
static int pgxcnode_add_info(GTM_PGXCNodeInfo *node);
static char *pgxcnode_copy_char(const char *str);

#define pgxcnode_type_equal(type1,type2) (type1 == type2)
#define pgxcnode_port_equal(port1,port2) (port1 == port2)

size_t
pgxcnode_get_all(GTM_PGXCNodeInfo **data, size_t maxlen)
{
	GTM_PGXCNodeInfoHashBucket *bucket;
	gtm_ListCell *elem;
	int node = 0;
	int i;

	for (i = 0; i < NODE_HASH_TABLE_SIZE; i++)
	{
		bucket = &GTM_PGXCNodes[i];

		GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, bucket->nhb_list)
		{
			GTM_PGXCNodeInfo *curr_nodeinfo = NULL;

			curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
			if (curr_nodeinfo != NULL)
			{
				data[node] = curr_nodeinfo;
				node++;
			}

			if (node == maxlen)
				break;
		}

		GTM_RWLockRelease(&bucket->nhb_lock);
	}

	return node;
}

size_t
pgxcnode_find_by_type(GTM_PGXCNodeType type, GTM_PGXCNodeInfo **data, size_t maxlen)
{
	GTM_PGXCNodeInfoHashBucket *bucket;
	gtm_ListCell *elem;
	int node = 0;
	int i;

	for (i = 0; i < NODE_HASH_TABLE_SIZE; i++)
	{
		bucket = &GTM_PGXCNodes[i];

		GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, bucket->nhb_list)
		{
			GTM_PGXCNodeInfo *cur = NULL;

			cur = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);

			if (cur != NULL && cur->type == type)
			{
				data[node] = cur;
				elog(LOG, "pgxcnode_find_by_type: cur=%p, ipaddress=%s", cur, cur->ipaddress);
				node++;
			}

			if (node == maxlen)
				break;
		}

		GTM_RWLockRelease(&bucket->nhb_lock);
	}

	return node;
}

/*
 * Find the pgxcnode info structure for the given node type and number key.
 */
static GTM_PGXCNodeInfo *
pgxcnode_find_info(GTM_PGXCNodeType type, char *node_name)
{
	uint32 hash = pgxcnode_gethash(node_name);
	GTM_PGXCNodeInfoHashBucket *bucket;
	gtm_ListCell *elem;
	GTM_PGXCNodeInfo *curr_nodeinfo = NULL;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

	gtm_foreach(elem, bucket->nhb_list)
	{
		curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
		if (pgxcnode_type_equal(curr_nodeinfo->type, type) &&
			(strcmp(curr_nodeinfo->nodename, node_name) == 0))
			break;
		curr_nodeinfo = NULL;
	}

	GTM_RWLockRelease(&bucket->nhb_lock);

	return curr_nodeinfo;
}

/*
 * Get the Hash Key depending on the node name
 * We do not except to have hundreds of nodes yet,
 * This function could be replaced by a better one
 * such as a double hash function indexed on type and Node Name
 */
static uint32
pgxcnode_gethash(char *nodename)
{
	int			i;
	int			length;
	int			value;
	uint32			hash = 0;

	if (nodename == NULL || nodename == '\0')
	{
		return 0;
	}

	length = strlen(nodename);

	value = 0x238F13AF * length;

	for (i = 0; i < length; i++)
	{
		value = value + ((nodename[i] << i * 5 % 24) & 0x7fffffff);
	}

	hash = (1103515243 * value + 12345) % 65537 & 0x00000FFF;

	return (hash % NODE_HASH_TABLE_SIZE);
}

/*
 * Remove a PGXC Node Info structure from the global hash table
 */
static int
pgxcnode_remove_info(GTM_PGXCNodeInfo *nodeinfo)
{
	uint32 hash = pgxcnode_gethash(nodeinfo->nodename);
	GTM_PGXCNodeInfoHashBucket   *bucket;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_WRITE);
	GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

	bucket->nhb_list = gtm_list_delete(bucket->nhb_list, nodeinfo);

	GTM_RWLockRelease(&nodeinfo->node_lock);
	GTM_RWLockRelease(&bucket->nhb_lock);

    return 0;
}

/*
 * Add a PGXC Node info structure to the global hash table
 */
static int
pgxcnode_add_info(GTM_PGXCNodeInfo *nodeinfo)
{
	uint32 hash = pgxcnode_gethash(nodeinfo->nodename);
	GTM_PGXCNodeInfoHashBucket   *bucket;
	gtm_ListCell *elem;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_WRITE);

	gtm_foreach(elem, bucket->nhb_list)
	{
		GTM_PGXCNodeInfo *curr_nodeinfo = NULL;
		curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);

		/* GTM Proxy are always registered as they do not have Identification numbers yet */
		if (pgxcnode_type_equal(curr_nodeinfo->type, nodeinfo->type) &&
			(strcmp(curr_nodeinfo->nodename, nodeinfo->nodename) == 0))
		{
			if (curr_nodeinfo->status == NODE_CONNECTED)
			{
				GTM_RWLockRelease(&bucket->nhb_lock);
				ereport(LOG,
						(EEXIST,
						 errmsg("Node with the given ID number already exists")));
				return EEXIST;
			}
			else
			{
				/*
				 * Node has been disconnected abruptly.
				 * And we are sure that disconnections are not done by other node
				 * trying to use the same ID.
				 * So check if its data (port, datafolder and remote IP) has changed
				 * and modify it.
				 */
				if (!pgxcnode_port_equal(curr_nodeinfo->port, nodeinfo->port))
					curr_nodeinfo->port = nodeinfo->port;

				if (strlen(curr_nodeinfo->datafolder) == strlen(nodeinfo->datafolder))
				{
					if (memcpy(curr_nodeinfo->datafolder,
							   nodeinfo->datafolder,
							   strlen(nodeinfo->datafolder)) != 0)
					{
						pfree(curr_nodeinfo->ipaddress);
						curr_nodeinfo->ipaddress = nodeinfo->ipaddress;
					}
				}

				if (strlen(curr_nodeinfo->ipaddress) == strlen(nodeinfo->ipaddress))
				{
					if (memcpy(curr_nodeinfo->datafolder,
							   nodeinfo->datafolder,
							   strlen(nodeinfo->datafolder)) != 0)
					{
						pfree(curr_nodeinfo->datafolder);
						curr_nodeinfo->datafolder = nodeinfo->datafolder;
					}
				}

				/* Reconnect a disconnected node */
				curr_nodeinfo->status = NODE_CONNECTED;

				/* Set socket number with the new one */
				curr_nodeinfo->socket = nodeinfo->socket;
				GTM_RWLockRelease(&bucket->nhb_lock);
				return 0;
			}
		}
	}

	/*
	 * Safe to add the structure to the list
	 */
	bucket->nhb_list = gtm_lappend(bucket->nhb_list, nodeinfo);
	GTM_RWLockRelease(&bucket->nhb_lock);

    return 0;
}

/*
 * Makes a copy of given string in TopMostMemoryContext
 */
static char *
pgxcnode_copy_char(const char *str)
{
	char *retstr = NULL;

	/*
	 * We must use the TopMostMemoryContext because the node information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	retstr = (char *) MemoryContextAlloc(TopMostMemoryContext,
										 strlen(str) + 1);

	if (retstr == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	memcpy(retstr, str, strlen(str));
	retstr[strlen(str)] = '\0';

	return retstr;
}

/*
 * Unregister the given node
 */
int
Recovery_PGXCNodeUnregister(GTM_PGXCNodeType type, char *node_name, bool in_recovery, int socket)
{
	GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, node_name);

	if (nodeinfo != NULL)
	{
		/*
		 * Unregistration has to be made by the same connection as the one used for registration
		 * or the one that reconnected the node.
		 */
		pgxcnode_remove_info(nodeinfo);

		/* Add a record to file on disk saying that this node has been unregistered correctly */
		if (!in_recovery)
			Recovery_RecordRegisterInfo(nodeinfo, false);

		pfree(nodeinfo->nodename);
		pfree(nodeinfo->ipaddress);
		pfree(nodeinfo->datafolder);
		pfree(nodeinfo);
	}
	else
		return EINVAL;

	return 0;
}

int
Recovery_PGXCNodeRegister(GTM_PGXCNodeType	type,
						  char			*nodename,
						  GTM_PGXCNodePort	port,
						  char			*proxyname,
						  GTM_PGXCNodeStatus	status,
						  char			*ipaddress,
						  char			*datafolder,
						  bool			in_recovery,
						  int			socket)
{
	GTM_PGXCNodeInfo *nodeinfo = NULL;
	int errcode = 0;

	nodeinfo = (GTM_PGXCNodeInfo *) palloc(sizeof (GTM_PGXCNodeInfo));

	if (nodeinfo == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	GTM_RWLockInit(&nodeinfo->node_lock);

	/* Fill in structure */
	nodeinfo->type = type;
	nodeinfo->nodename = pgxcnode_copy_char(nodename);
	nodeinfo->port = port;
	nodeinfo->proxyname = pgxcnode_copy_char(proxyname);
	nodeinfo->datafolder = pgxcnode_copy_char(datafolder);
	nodeinfo->ipaddress = pgxcnode_copy_char(ipaddress);
	nodeinfo->status = status;
	nodeinfo->socket = socket;

	elog(LOG, "Recovery_PGXCNodeRegister Request info: type=%d, nodename=%s, port=%d," \
			  "datafolder=%s, ipaddress=%s, status=%d",
			  type, nodename, port, datafolder, ipaddress, status);
	elog(LOG, "Recovery_PGXCNodeRegister Node info: type=%d, nodename=%s, port=%d, "\
			  "datafolder=%s, ipaddress=%s, status=%d",
			  nodeinfo->type, nodeinfo->nodename, nodeinfo->port,
			  nodeinfo->datafolder, nodeinfo->ipaddress, nodeinfo->status);

	/* Add PGXC Node Info to the global hash table */
	errcode = pgxcnode_add_info(nodeinfo);

	/*
	 * Add a Record to file disk saying that this node
	 * with given data has been correctly registered
	 */
	if (!in_recovery && errcode == 0)
		Recovery_RecordRegisterInfo(nodeinfo, true);

	return errcode;
}


/*
 * Process MSG_NODE_REGISTER
 */
void
ProcessPGXCNodeRegister(Port *myport, StringInfo message)
{
	GTM_PGXCNodeType	type;
	GTM_PGXCNodePort	port;
	char			remote_host[NI_MAXHOST];
	char			datafolder[NI_MAXHOST];
	char			node_name[NI_MAXHOST];
	char			proxyname[NI_MAXHOST];
	char			*ipaddress;
	MemoryContext		oldContext;
	int			len;
	StringInfoData		buf;
	GTM_PGXCNodeStatus	status;

	/* Read Node Type */
	memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
			sizeof (GTM_PGXCNodeType));

	 /* Read Node name */
	len = pq_getmsgint(message, sizeof (int));
	if (len >= NI_MAXHOST)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Invalid name length.")));

	memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
	node_name[len] = '\0';

	 /* Read Host name */
	len = pq_getmsgint(message, sizeof (int));
	memcpy(remote_host, (char *)pq_getmsgbytes(message, len), len);
	remote_host[len] = '\0';
	ipaddress = remote_host;

	/* Read Port Number */
	memcpy(&port, pq_getmsgbytes(message, sizeof (GTM_PGXCNodePort)),
			sizeof (GTM_PGXCNodePort));

	/* Read Proxy name (empty string if no proxy used) */
	len = pq_getmsgint(message, sizeof (GTM_StrLen));
	if (len >= NI_MAXHOST)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Invalid proxy name length.")));
	memcpy(proxyname, (char *)pq_getmsgbytes(message, len), len);
	proxyname[len] = '\0';

	elog(LOG, "ProcessPGXCNodeRegister: ipaddress = %s", ipaddress);

	/*
	 * Finish by reading Data Folder (length and then string)
	 */
	len = pq_getmsgint(message, sizeof (GTM_StrLen));

	memcpy(datafolder, (char *)pq_getmsgbytes(message, len), len);
	datafolder[len] = '\0';

	status = pq_getmsgint(message, sizeof (GTM_PGXCNodeStatus));

	if ((type!=PGXC_NODE_GTM_PROXY) &&
		(type!=PGXC_NODE_GTM_PROXY_POSTMASTER) &&
		(type!=PGXC_NODE_COORDINATOR) &&
		(type!=PGXC_NODE_DATANODE) &&
		(type!=PGXC_NODE_GTM) &&
		(type!=PGXC_NODE_DEFAULT))
		ereport(ERROR,
				(EINVAL,
				 errmsg("Unknown node type.")));

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if (Recovery_PGXCNodeRegister(type, node_name, port,
								  proxyname, NODE_CONNECTED,
								  ipaddress, datafolder, false, myport->sock))
	{
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to Register node")));
	}

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	/*
	 * Send a SUCCESS message back to the client
	 */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, NODE_REGISTER_RESULT, 4);
	if (myport->remote_type == PGXC_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&type, sizeof(GTM_PGXCNodeType));
	/* Node name length */
	pq_sendint(&buf, strlen(node_name), 4);
	/* Node name (var-len) */
	pq_sendbytes(&buf, node_name, strlen(node_name));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != PGXC_NODE_GTM_PROXY)
		pq_flush(myport);

	if (GetMyThreadInfo->thr_conn->standby)
	{
		int _rc;
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(LOG, "calling node_register_internal() for standby GTM %p.",
		 GetMyThreadInfo->thr_conn->standby);

retry:
		_rc = node_register_internal(GetMyThreadInfo->thr_conn->standby,
									 type,
									 ipaddress,
									 port,
									 node_name,
									 datafolder,
									 status);

		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		elog(LOG, "node_register_internal() returns rc %d.", _rc);
	}
}

/*
 * Process MSG_NODE_UNREGISTER
 */
void
ProcessPGXCNodeUnregister(Port *myport, StringInfo message)
{
	GTM_PGXCNodeType	type;
	MemoryContext		oldContext;
	StringInfoData		buf;
	int			len;
	char			node_name[NI_MAXHOST];

	/* Read Node Type and number */
	memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
			sizeof (GTM_PGXCNodeType));

	 /* Read Node name */
	len = pq_getmsgint(message, sizeof (int));
	if (len >= NI_MAXHOST)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Invalid node name length")));
	memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
	node_name[len] = '\0';

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if (Recovery_PGXCNodeUnregister(type, node_name, false, myport->sock))
	{
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to Unregister node")));
	}

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	/*
	 * Send a SUCCESS message back to the client
	 */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, NODE_UNREGISTER_RESULT, 4);
	if (myport->remote_type == PGXC_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&type, sizeof(GTM_PGXCNodeType));
	/* Node name length */
	pq_sendint(&buf, strlen(node_name), 4);
	/* Node name (var-len) */
	pq_sendbytes(&buf, node_name, strlen(node_name));

	pq_endmessage(myport, &buf);

	if (myport->remote_type != PGXC_NODE_GTM_PROXY)
		pq_flush(myport);

	if (GetMyThreadInfo->thr_conn->standby)
	{
		int _rc;
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(LOG, "calling node_unregister() for standby GTM %p.",
		 GetMyThreadInfo->thr_conn->standby);
		
retry:
		_rc = node_unregister(GetMyThreadInfo->thr_conn->standby,
							  type,
							  node_name);
		
		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		elog(LOG, "node_unregister() returns rc %d.", _rc);
	}
}

/*
 * Process MSG_NODE_LIST
 */
void
ProcessPGXCNodeList(Port *myport, StringInfo message)
{
	MemoryContext		oldContext;
	StringInfoData		buf;
	int			num_node = 13;
	int i;

	GTM_PGXCNodeInfo *data[MAX_NODES];
	char *s_data[MAX_NODES];
	size_t s_datalen[MAX_NODES];

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	memset(data, 0, sizeof(GTM_PGXCNodeInfo *) * MAX_NODES);
	memset(s_data, 0, sizeof(char *) * MAX_NODES);

	num_node = pgxcnode_get_all(data, MAX_NODES);

	for (i = 0; i < num_node; i++)
	{
		size_t s_len;

		s_len = gtm_get_pgxcnodeinfo_size(data[i]);

		/*
		 * Allocate memory blocks for serialized GTM_PGXCNodeInfo data.
		 */
		s_data[i] = (char *)malloc(s_len+1);
		memset(s_data[i], 0, s_len+1);

		s_datalen[i] = gtm_serialize_pgxcnodeinfo(data[i], s_data[i], s_len+1);

		elog(LOG, "gtm_get_pgxcnodeinfo_size: s_len=%ld, s_datalen=%ld", s_len, s_datalen[i]);
	}

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	/*
	 * Send a SUCCESS message back to the client
	 */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, NODE_LIST_RESULT, 4);
	if (myport->remote_type == PGXC_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendint(&buf, num_node, sizeof(int));   /* number of nodes */

	/*
	 * Send pairs of GTM_PGXCNodeInfo size and serialized GTM_PGXCNodeInfo body.
	 */
	for (i = 0; i < num_node; i++)
	{
		pq_sendint(&buf, s_datalen[i], sizeof(int));
		pq_sendbytes(&buf, s_data[i], s_datalen[i]);
	}

	pq_endmessage(myport, &buf);

	if (myport->remote_type != PGXC_NODE_GTM_PROXY)
		pq_flush(myport);

	/*
	 * Release memory blocks for the serialized data.
	 */
	for (i = 0; i < num_node; i++)
	{
		free(s_data[i]);
	}

	elog(LOG, "ProcessPGXCNodeList() ok.");
}

/*
 * Called at GTM shutdown, rewrite on disk register information
 * and write only data of nodes currently registered.
 */
void
Recovery_SaveRegisterInfo(void)
{
GTM_PGXCNodeInfoHashBucket *bucket;
gtm_ListCell *elem;
GTM_PGXCNodeInfo *nodeinfo = NULL;
int hash, ctlfd;
	char filebkp[GTM_NODE_FILE_MAX_PATH];

	GTM_RWLockAcquire(&RegisterFileLock, GTM_LOCKMODE_WRITE);

	/* Create a backup file in case their is a problem during file writing */
	sprintf(filebkp, "%s.bkp", GTMPGXCNodeFile);

	ctlfd = open(filebkp, O_WRONLY | O_CREAT | O_TRUNC,
				 S_IRUSR | S_IWUSR);

	if (ctlfd < 0)
	{
		GTM_RWLockRelease(&RegisterFileLock);
		return;
	}

for (hash = 0; hash < NODE_HASH_TABLE_SIZE; hash++)
	{
		bucket = &GTM_PGXCNodes[hash];

		GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

		/* Write one by one information about registered nodes */
		gtm_foreach(elem, bucket->nhb_list)
		{
			int len;

			nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
			if (nodeinfo == NULL)
				break;

			GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

			write(ctlfd, &NodeRegisterMagic, sizeof (NodeRegisterMagic));

			write(ctlfd, &nodeinfo->type, sizeof (GTM_PGXCNodeType));
			len = strlen(nodeinfo->nodename);
			write(ctlfd, &len, sizeof(uint32));
			write(ctlfd, nodeinfo->nodename, len);
			write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));

			len = strlen(nodeinfo->proxyname);
			write(ctlfd, &len, sizeof(uint32));
			write(ctlfd, nodeinfo->proxyname, len);

			write(ctlfd, &nodeinfo->status, sizeof (GTM_PGXCNodeStatus));

			len = strlen(nodeinfo->ipaddress);
			write(ctlfd, &len, sizeof(uint32));
			write(ctlfd, nodeinfo->ipaddress, len);

			len = strlen(nodeinfo->datafolder);
			write(ctlfd, &len, sizeof(uint32));
			write(ctlfd, nodeinfo->datafolder, len);

			write(ctlfd, &NodeEndMagic, sizeof(NodeEndMagic));

			GTM_RWLockRelease(&nodeinfo->node_lock);
		}

		GTM_RWLockRelease(&bucket->nhb_lock);
	}

	close(ctlfd);

	/* Replace former file by backup file */
	if (rename(filebkp, GTMPGXCNodeFile) < 0)
	{
		elog(LOG, "Cannot save register file");
	}

	GTM_RWLockRelease(&RegisterFileLock);
}

/*
 * Add a Register or Unregister record on PGXC Node file on disk.
 */
void
Recovery_RecordRegisterInfo(GTM_PGXCNodeInfo *nodeinfo, bool is_register)
{
	int ctlfd;
	int len;

	GTM_RWLockAcquire(&RegisterFileLock, GTM_LOCKMODE_WRITE);

	ctlfd = open(GTMPGXCNodeFile, O_WRONLY | O_CREAT | O_APPEND,
				 S_IRUSR | S_IWUSR);

	if (ctlfd == -1 || nodeinfo == NULL)
	{
		GTM_RWLockRelease(&RegisterFileLock);
		return;
	}

	GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

	if (is_register)
		write(ctlfd, &NodeRegisterMagic, sizeof (NodeRegisterMagic));
	else
		write(ctlfd, &NodeUnregisterMagic, sizeof (NodeUnregisterMagic));

	write(ctlfd, &nodeinfo->type, sizeof (GTM_PGXCNodeType));
	len = strlen(nodeinfo->nodename);
	write(ctlfd, &len, sizeof(uint32));
	write(ctlfd, nodeinfo->nodename, len);

	if (is_register)
	{
		int len;

		write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));

		len = strlen(nodeinfo->proxyname);
		write(ctlfd, &len, sizeof(uint32));
		write(ctlfd, nodeinfo->proxyname, len);

		write(ctlfd, &nodeinfo->status, sizeof (GTM_PGXCNodeStatus));

		len = strlen(nodeinfo->ipaddress);
		write(ctlfd, &len, sizeof(uint32));
		write(ctlfd, nodeinfo->ipaddress, len);

		len = strlen(nodeinfo->datafolder);
		write(ctlfd, &len, sizeof(uint32));
		write(ctlfd, nodeinfo->datafolder, len);
	}

	write(ctlfd, &NodeEndMagic, sizeof(NodeEndMagic));

	GTM_RWLockRelease(&nodeinfo->node_lock);

	close(ctlfd);
	GTM_RWLockRelease(&RegisterFileLock);
}

void
Recovery_RestoreRegisterInfo(void)
{
	int magic;
	int ctlfd;

	/* This is made when GTM/Proxy restarts, so it is not necessary to take a lock */
	ctlfd = open(GTMPGXCNodeFile, O_RDONLY);

	if (ctlfd == -1)
		return;

	while (read(ctlfd, &magic, sizeof (NodeRegisterMagic)) == sizeof (NodeRegisterMagic))
	{
		GTM_PGXCNodeType	type;
		GTM_PGXCNodePort	port;
		GTM_PGXCNodeStatus	status;
		char			*ipaddress, *datafolder, *nodename, *proxyname;
		int			len;

		if (magic != NodeRegisterMagic && magic != NodeUnregisterMagic)
		{
			elog(WARNING, "Start magic mismatch %x", magic);
			break;
		}

		read(ctlfd, &type, sizeof (GTM_PGXCNodeType));
		/* Read size of nodename string */
		read(ctlfd, &len, sizeof (uint32));
		nodename = (char *) palloc(len);
		read(ctlfd, nodename, len);

		if (magic == NodeRegisterMagic)
		{
			read(ctlfd, &port, sizeof (GTM_PGXCNodePort));

			/* Read size of proxyname string */
			read(ctlfd, &len, sizeof (uint32));
			proxyname = (char *) palloc(len);
			read(ctlfd, proxyname, len);

			read(ctlfd, &status, sizeof (GTM_PGXCNodeStatus));

			/* Read size of ipaddress string */
			read(ctlfd, &len, sizeof (uint32));
			ipaddress = (char *) palloc(len);
			read(ctlfd, ipaddress, len);

			/* Read size of datafolder string */
			read(ctlfd, &len, sizeof (uint32));
			datafolder = (char *) palloc(len);
			read(ctlfd, datafolder, len);
		}

		/* Rebuild based on the records */
		if (magic == NodeRegisterMagic)
			Recovery_PGXCNodeRegister(type, nodename, port, proxyname, status,
									  ipaddress, datafolder, true, 0);
		else
			Recovery_PGXCNodeUnregister(type, nodename, true, 0);

		read(ctlfd, &magic, sizeof(NodeEndMagic));

		if (magic != NodeEndMagic)
		{
			elog(WARNING, "Corrupted control file");
				return;
		}
	}

	close(ctlfd);
}

void
Recovery_SaveRegisterFileName(char *dir)
{
	if (!dir)
		return;

	sprintf(GTMPGXCNodeFile, "%s/%s", dir, GTM_NODE_FILE);
}

/*
 * Disconnect node whose master connection has been cut with GTM
 */
void
Recovery_PGXCNodeDisconnect(Port *myport)
{
	GTM_PGXCNodeType	type = myport->remote_type;
	char			*nodename = myport->node_name;
	GTM_PGXCNodeInfo	*nodeinfo = NULL;
	MemoryContext		oldContext;

	/* Only a master connection can disconnect a node */
	if (!myport->is_postmaster)
		return;

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	nodeinfo = pgxcnode_find_info(type, nodename);

	if (nodeinfo != NULL)
	{
		/*
		 * Disconnection cannot be made with another socket than the one used for registration.
		 * socket may have a dummy value (-1) under GTM standby node.
		 */
		if (nodeinfo->socket >= 0 && myport->sock != nodeinfo->socket)
			return;

		GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

		nodeinfo->status = NODE_DISCONNECTED;
		nodeinfo->socket = 0;

		GTM_RWLockRelease(&nodeinfo->node_lock);
	}

	MemoryContextSwitchTo(oldContext);
}

int
Recovery_PGXCNodeBackendDisconnect(GTM_PGXCNodeType type, char *nodename, int socket)
{
	GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, nodename);
	int errcode = 0;


	if (nodeinfo != NULL)
	{
		/*
		 * A node can be only disconnected by the same connection as the one used for registration
		 * or reconnection.
		 */
		if (socket != nodeinfo->socket)
			return -1;

		GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

		nodeinfo->status = NODE_DISCONNECTED;
		nodeinfo->socket = 0;

		GTM_RWLockRelease(&nodeinfo->node_lock);
	}
	else
		errcode = -1;

	return errcode;
}

/*
 * Process MSG_BACKEND_DISCONNECT
 *
 * A Backend has disconnected on a Proxy.
 * If this backend is postmaster, mark the referenced node as disconnected.
 */
void
ProcessPGXCNodeBackendDisconnect(Port *myport, StringInfo message)
{
	MemoryContext		oldContext;
	GTM_PGXCNodeType	type;
	bool			is_postmaster;
	char			node_name[NI_MAXHOST];
	int			len;

	is_postmaster = pq_getmsgbyte(message);

	if (is_postmaster)
	{
		/* Read Node Type and name */
		memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)), sizeof (GTM_PGXCNodeType));

		/* Read Node name */
		len = pq_getmsgint(message, sizeof (int));
		if (len >= NI_MAXHOST)
		{
			elog(LOG, "Invalid node name length %d", len);
			return;
		}
		memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
		node_name[len] = '\0';
	}

	pq_getmsgend(message);

	if (!is_postmaster)
		return; /* Nothing to do */

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if (Recovery_PGXCNodeBackendDisconnect(type, node_name, myport->sock) < 0)
	{
		elog(LOG, "Cannot disconnect Unregistered node");
	}

	MemoryContextSwitchTo(oldContext);

	/*
	 * Forwarding MSG_BACKEND_DISCONNECT message to GTM standby.
	 * No need to wait any response.
	 */
	if (GetMyThreadInfo->thr_conn->standby)
	{
		int _rc;
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(LOG, "forwarding MSG_BACKEND_DISCONNECT to standby GTM %p.",
				  GetMyThreadInfo->thr_conn->standby);

retry:
		_rc = backend_disconnect(GetMyThreadInfo->thr_conn->standby,
					 is_postmaster,
					 type,
					 node_name);

		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		elog(LOG, "MSG_BACKEND_DISCONNECT rc=%d done.", _rc);
	}
}
