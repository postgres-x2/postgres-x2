/*-------------------------------------------------------------------------
 *
 * register.c
 *  PGXC Node Register on GTM and GTM Proxy, node registering functions
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/register.h"
#include "gtm/assert.h"
#include <stdio.h>
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"
#include "gtm/stringinfo.h"
#include "gtm/gtm_ip.h"

#define GTM_NODE_FILE			"register.node"
#define NODE_HASH_TABLE_SIZE	16
#define GTM_NODE_FILE_MAX_PATH	1024

typedef struct GTM_NodeInfoHashBucket
{
	List        *nhb_list;
	GTM_RWLock  nhb_lock;
} GTM_PGXCNodeInfoHashBucket;

static char GTMPGXCNodeFile[GTM_NODE_FILE_MAX_PATH];

/* Lock access of record file when necessary */
static GTM_RWLock RegisterFileLock;

static int NodeRegisterMagic = 0xeaeaeaea;
static int NodeUnregisterMagic = 0xebebebeb; 
static int NodeEndMagic = 0xefefefef;

static GTM_PGXCNodeInfoHashBucket GTM_PGXCNodes[NODE_HASH_TABLE_SIZE];

static GTM_PGXCNodeInfo *pgxcnode_find_info(GTM_PGXCNodeType	type,
											GTM_PGXCNodeId		nodenum);
static uint32 pgxcnode_gethash(GTM_PGXCNodeId nodenum);
static int pgxcnode_remove_info(GTM_PGXCNodeInfo *node);
static int pgxcnode_add_info(GTM_PGXCNodeInfo *node);
static char *pgxcnode_copy_char(const char *str);

#define pgxcnode_type_equal(type1,type2) (type1 == type2)
#define pgxcnode_nodenum_equal(num1,num2) (num1 == num2)
#define pgxcnode_port_equal(port1,port2) (port1 == port2)

/*
 * Find the pgxcnode info structure for the given node type and number key.
 */
static GTM_PGXCNodeInfo *
pgxcnode_find_info(GTM_PGXCNodeType	type,
				   GTM_PGXCNodeId	nodenum)
{
	uint32 hash = pgxcnode_gethash(nodenum);
	GTM_PGXCNodeInfoHashBucket *bucket;
	ListCell *elem;
	GTM_PGXCNodeInfo *curr_nodeinfo = NULL;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

	foreach(elem, bucket->nhb_list)
	{
		curr_nodeinfo = (GTM_PGXCNodeInfo *) lfirst(elem);
		if (pgxcnode_type_equal(curr_nodeinfo->type, type) &&
			pgxcnode_nodenum_equal(curr_nodeinfo->nodenum, nodenum))
			break;
		curr_nodeinfo = NULL;
	}

	GTM_RWLockRelease(&bucket->nhb_lock);

	return curr_nodeinfo;
}

/*
 * Get the Hash Key depending on the node number
 * We do not except to have hundreds of nodes yet,
 * This function could be replaced by a better one
 * such as a double hash function indexed on type and Node Number
 */
static uint32
pgxcnode_gethash(GTM_PGXCNodeId nodenum)
{
    uint32 hash = 0;

	hash = (uint32) nodenum;

	return (hash % NODE_HASH_TABLE_SIZE);
}

/*
 * Remove a PGXC Node Info structure from the global hash table
 */
static int
pgxcnode_remove_info(GTM_PGXCNodeInfo *nodeinfo)
{
	uint32 hash = pgxcnode_gethash(nodeinfo->nodenum);
	GTM_PGXCNodeInfoHashBucket   *bucket;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_WRITE);
	GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

	bucket->nhb_list = list_delete(bucket->nhb_list, nodeinfo);

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
	uint32 hash = pgxcnode_gethash(nodeinfo->nodenum);
	GTM_PGXCNodeInfoHashBucket   *bucket;
	ListCell *elem;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_WRITE);

	foreach(elem, bucket->nhb_list)
	{
		GTM_PGXCNodeInfo *curr_nodeinfo = NULL;
		curr_nodeinfo = (GTM_PGXCNodeInfo *) lfirst(elem);

		/* GTM Proxy are always registered as they do not have Identification numbers yet */
		if (pgxcnode_type_equal(curr_nodeinfo->type, nodeinfo->type) &&
			pgxcnode_nodenum_equal(curr_nodeinfo->nodenum, nodeinfo->nodenum))
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
				 * Check if its data (port, datafolder and remote IP) has changed
				 * and modify it
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
				GTM_RWLockRelease(&bucket->nhb_lock);
				return 0;
			}
		}
	}

	/*
	 * Safe to add the structure to the list
	 */
	bucket->nhb_list = lappend(bucket->nhb_list, nodeinfo);
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
										 strlen(str));

	if (retstr == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	memcpy(retstr, str, strlen(str));

	return retstr;
}

/*
 * Unregister the given node
 */
int
Recovery_PGXCNodeUnregister(GTM_PGXCNodeType type, GTM_PGXCNodeId nodenum, bool in_recovery)
{
	GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, nodenum);

	if (nodeinfo != NULL)
	{
		pgxcnode_remove_info(nodeinfo);

		/* Add a record to file on disk saying that this node has been unregistered correctly */
		if (!in_recovery)
			Recovery_RecordRegisterInfo(nodeinfo, false);

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
						  GTM_PGXCNodeId	nodenum,
						  GTM_PGXCNodePort	port,
						  GTM_PGXCNodeId	proxynum,
						  GTM_PGXCNodeStatus status,
						  char			   *ipaddress,
						  char			   *datafolder,
						  bool				in_recovery)
{
	GTM_PGXCNodeInfo *nodeinfo = NULL;
	int errcode = 0;

	nodeinfo = (GTM_PGXCNodeInfo *) palloc(sizeof (GTM_PGXCNodeInfo));

	if (nodeinfo == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	GTM_RWLockInit(&nodeinfo->node_lock);

	/* Fill in structure */
	nodeinfo->type = type;
	nodeinfo->nodenum = nodenum;
	nodeinfo->port = port;
	nodeinfo->proxynum = proxynum;
	nodeinfo->datafolder = pgxcnode_copy_char(datafolder);
	nodeinfo->ipaddress = pgxcnode_copy_char(ipaddress);
	nodeinfo->status = status;

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
	GTM_PGXCNodeId		nodenum, proxynum;
	GTM_PGXCNodePort	port;
	char				remote_host[NI_MAXHOST];
	char				remote_port[NI_MAXSERV];
	char			   *datafolder;
	char			   *ipaddress;
	MemoryContext		oldContext;
	int					strlen;
	StringInfoData		buf;

	/* Get the Remote node IP and port to register it */
	remote_host[0] = '\0';
	remote_port[0] = '\0';

	if (myport->remote_type != PGXC_NODE_GTM_PROXY)
	{
		if (gtm_getnameinfo_all(&myport->raddr.addr, myport->raddr.salen,
								remote_host, sizeof(remote_host),
								remote_port, sizeof(remote_port),
								NI_NUMERICSERV))
		{
			int	ret = gtm_getnameinfo_all(&myport->raddr.addr, myport->raddr.salen,
										 remote_host, sizeof(remote_host),
										 remote_port, sizeof(remote_port),
										 NI_NUMERICHOST | NI_NUMERICSERV);

			if (ret)
				ereport(WARNING,
						(errmsg_internal("gtm_getnameinfo_all() failed")));
		}
	}

	/* Read Node Type and number */
	memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
			sizeof (GTM_PGXCNodeType));
	memcpy(&nodenum, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeId)),
			sizeof (GTM_PGXCNodeId));

	/* Read Port Number */
	memcpy(&port, pq_getmsgbytes(message, sizeof (GTM_PGXCNodePort)),
			sizeof (GTM_PGXCNodePort));

	/* Read Proxy ID number (0 if no proxy used) */
	memcpy(&proxynum, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeId)),
			sizeof (GTM_PGXCNodeId));

	/*
	 * Message is received from a proxy, get also the remote node address
	 * In the case a proxy registering itself, the remote address
	 * is directly taken from socket.
	 */
	if (myport->remote_type == PGXC_NODE_GTM_PROXY &&
		!myport->is_postmaster)
	{
		strlen = pq_getmsgint(message, sizeof (GTM_StrLen));
		ipaddress = (char *)pq_getmsgbytes(message, strlen);
	}
	else
		ipaddress = remote_host;

	/*
	 * Finish by reading Data Folder (length and then string)
	 */

	strlen = pq_getmsgint(message, sizeof (GTM_StrLen));
	datafolder = (char *)pq_getmsgbytes(message, strlen);

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if (Recovery_PGXCNodeRegister(type, nodenum, port,
								  proxynum, NODE_CONNECTED,
								  ipaddress, datafolder, false))
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
	pq_sendbytes(&buf, (char *)&nodenum, sizeof(GTM_PGXCNodeId));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != PGXC_NODE_GTM_PROXY)
		pq_flush(myport);
}

/*
 * Process MSG_NODE_UNREGISTER
 */
void
ProcessPGXCNodeUnregister(Port *myport, StringInfo message)
{
	GTM_PGXCNodeType	type;
	GTM_PGXCNodeId		nodenum;
	MemoryContext		oldContext;
	StringInfoData		buf;

	/* Read Node Type and number */
	memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
			sizeof (GTM_PGXCNodeType));
	memcpy(&nodenum, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeId)),
			sizeof (GTM_PGXCNodeId));

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if (Recovery_PGXCNodeUnregister(type, nodenum, false))
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
	pq_sendbytes(&buf, (char *)&nodenum, sizeof(GTM_PGXCNodeId));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != PGXC_NODE_GTM_PROXY)
		pq_flush(myport);
}

/*
 * Called at GTM shutdown, rewrite on disk register information
 * and write only data of nodes currently registered.
 */
void
Recovery_SaveRegisterInfo(void)
{
    GTM_PGXCNodeInfoHashBucket *bucket;
    ListCell *elem;
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
		foreach(elem, bucket->nhb_list)
		{
			int len;

			nodeinfo = (GTM_PGXCNodeInfo *) lfirst(elem);
			if (nodeinfo == NULL)
				break;

			GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

			write(ctlfd, &NodeRegisterMagic, sizeof (NodeRegisterMagic));

			write(ctlfd, &nodeinfo->type, sizeof (GTM_PGXCNodeType));
			write(ctlfd, &nodeinfo->nodenum, sizeof (GTM_PGXCNodeId));

			write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));
			write(ctlfd, &nodeinfo->proxynum, sizeof (GTM_PGXCNodeId));
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
	write(ctlfd, &nodeinfo->nodenum, sizeof (GTM_PGXCNodeId));

	if (is_register)
	{
		int len;

		write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));
		write(ctlfd, &nodeinfo->proxynum, sizeof (GTM_PGXCNodeId));
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
		GTM_PGXCNodeId		nodenum, proxynum;
		GTM_PGXCNodePort	port;
		GTM_PGXCNodeStatus	status;
		char			   *ipaddress, *datafolder;
		int					len;

		if (magic != NodeRegisterMagic && magic != NodeUnregisterMagic)
		{
			elog(WARNING, "Start magic mismatch %x", magic);
			break;
		}

		read(ctlfd, &type, sizeof (GTM_PGXCNodeType));
		read(ctlfd, &nodenum, sizeof (GTM_PGXCNodeId));

		if (magic == NodeRegisterMagic)
		{
			read(ctlfd, &port, sizeof (GTM_PGXCNodePort));
			read(ctlfd, &proxynum, sizeof (GTM_PGXCNodeId));
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
			Recovery_PGXCNodeRegister(type, nodenum, port, proxynum, status,
									  ipaddress, datafolder, true);
		else
			Recovery_PGXCNodeUnregister(type, nodenum, true);

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
	GTM_PGXCNodeId		nodenum = myport->pgxc_node_id;
	GTM_PGXCNodeInfo   *nodeinfo = NULL;
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

	nodeinfo = pgxcnode_find_info(type, nodenum);

	if (nodeinfo != NULL)
	{
		GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

		nodeinfo->status = NODE_DISCONNECTED;

		GTM_RWLockRelease(&nodeinfo->node_lock);
	}

	MemoryContextSwitchTo(oldContext);
}

int
Recovery_PGXCNodeBackendDisconnect(GTM_PGXCNodeType type, GTM_PGXCNodeId nodenum)
{
	GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, nodenum);
	int errcode = 0;

	if (nodeinfo != NULL)
	{
		GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

		nodeinfo->status = NODE_DISCONNECTED;

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
	GTM_PGXCNodeId		nodenum;
	GTM_PGXCNodeType	type;
	bool				is_postmaster;

	is_postmaster = pq_getmsgbyte(message);

	if (is_postmaster)
	{
		/* Read Node Type and number */
		memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
				sizeof (GTM_PGXCNodeType));
		memcpy(&nodenum, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeId)),
				sizeof (GTM_PGXCNodeId));
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

	if (Recovery_PGXCNodeBackendDisconnect(type, nodenum) < 0)
	{
		elog(LOG, "Cannot disconnect Unregistered node");
	}

	MemoryContextSwitchTo(oldContext);
}
