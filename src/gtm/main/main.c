/*-------------------------------------------------------------------------
 *
 * main.c
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
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <getopt.h>
#include <stdio.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/elog.h"
#include "gtm/memutils.h"
#include "gtm/gtm_list.h"
#include "gtm/libpq.h"
#include "gtm/libpq-be.h"
#include "gtm/pqsignal.h"
#include "gtm/pqformat.h"
#include "gtm/assert.h"
#include "gtm/register.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_msg.h"

extern int	optind;
extern char *optarg;

#define GTM_MAX_PATH			1024
#define GTM_DEFAULT_HOSTNAME	"*"
#define GTM_DEFAULT_PORT		6666
#define GTM_CONTROL_FILE		"gtm.control"
#define GTM_PID_FILE			"gtm.pid"
#define GTM_LOG_FILE			"gtm.log"

static char *progname = "gtm";
char	   *ListenAddresses;
int			GTMPortNumber;
char		GTMControlFile[GTM_MAX_PATH];
char		*GTMDataDir;

/* The socket(s) we're listening to. */
#define MAXLISTEN	64
static int	ListenSocket[MAXLISTEN];

pthread_key_t	threadinfo_key;
static bool		GTMAbortPending = false;

static Port *ConnCreate(int serverFd);
static int ServerLoop(void);
static int initMasks(fd_set *rmask);
void *GTM_ThreadMain(void *argp);
static int GTMAddConnection(Port *port);
static int ReadCommand(Port *myport, StringInfo inBuf);

static void ProcessCommand(Port *myport, StringInfo input_message);
static void ProcessPGXCNodeCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessTransactionCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessSnapshotCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessSequenceCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
static void ProcessQueryCommand(Port *myport, GTM_MessageType mtype, StringInfo message);

static void GTM_RegisterPGXCNode(Port *myport, GTM_PGXCNodeId pgxc_node_id);
static void GTM_UnregisterPGXCNode(Port *myport, GTM_PGXCNodeId pgxc_node_id);

static bool CreateOptsFile(int argc, char *argv[]);
static void CreateDataDirLockFile(void);
static void CreateLockFile(const char *filename, const char *refName);
static void ChangeToDataDir(void);
static void checkDataDir(void);
static void DeleteLockFile(const char *filename);

/*
 * One-time initialization. It's called immediately after the main process
 * starts
 */ 
static GTM_ThreadInfo *
MainThreadInit()
{
	GTM_ThreadInfo *thrinfo;

	pthread_key_create(&threadinfo_key, NULL);
	
	/*
	 * Initialize the lock protecting the global threads info
	 */
	GTM_RWLockInit(&GTMThreads->gt_lock);

	/*
	 * We are called even before memory context management is setup. We must
	 * use malloc
	 */
	thrinfo = (GTM_ThreadInfo *)malloc(sizeof (GTM_ThreadInfo));

	if (thrinfo == NULL)
	{
		fprintf(stderr, "malloc failed: %d", errno);
		fflush(stdout);
		fflush(stderr);
	}

	if (SetMyThreadInfo(thrinfo))
	{
		fprintf(stderr, "SetMyThreadInfo failed: %d", errno);
		fflush(stdout);
		fflush(stderr);
	}

	return thrinfo;
}

static void
BaseInit()
{
	GTM_ThreadInfo *thrinfo;

	thrinfo = MainThreadInit();

	MyThreadID = pthread_self();

	MemoryContextInit();

	checkDataDir();
	ChangeToDataDir();
	CreateDataDirLockFile();

	sprintf(GTMControlFile, "%s/%s", GTMDataDir, GTM_CONTROL_FILE);
	if (GTMLogFile == NULL)
	{
		GTMLogFile = (char *) malloc(GTM_MAX_PATH);
		sprintf(GTMLogFile, "%s/%s", GTMDataDir, GTM_LOG_FILE);
	}

	/* Save Node Register File in register.c */
	Recovery_SaveRegisterFileName(GTMDataDir);

	DebugFileOpen();

	GTM_InitTxnManager();
	GTM_InitSeqManager();

	/*
	 * The memory context is now set up.
	 * Add the thrinfo structure in the global array
	 */
	if (GTM_ThreadAdd(thrinfo) == -1)
	{
		fprintf(stderr, "GTM_ThreadAdd for main thread failed: %d", errno);
		fflush(stdout);
		fflush(stderr);
	}
}

static void
GTM_SigleHandler(int signal)
{
	fprintf(stderr, "Received signal %d", signal);

	switch (signal)
	{
		case SIGKILL:
		case SIGTERM:
		case SIGQUIT:
		case SIGINT:
		case SIGHUP:
			break;

		default:
			fprintf(stderr, "Unknown signal %d\n", signal);
			return;
	}

	/*
	 * XXX We should do a clean shutdown here.
	 */

	/* Rewrite Register Information (clean up unregister records) */
	Recovery_SaveRegisterInfo();

	/* Delete pid file before shutting down */
	DeleteLockFile(GTM_PID_FILE);

	PG_SETMASK(&BlockSig);
	GTMAbortPending = true;

	return;
}

/*
 * Help display should match 
 */
static void
help(const char *progname)
{
	printf(_("This is the GTM server.\n\n"));
	printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
	printf(_("Options:\n"));
	printf(_("  -h hostname     GTM server hostname/IP\n"));
	printf(_("  -p port			GTM server port number\n"));
	printf(_("  -x xid			Starting GXID \n"));
	printf(_("  -D directory	GTM working directory\n"));
	printf(_("  -l filename		GTM server log file name \n"));
	printf(_("  --help          show this help, then exit\n"));
}

int
main(int argc, char *argv[])
{
	int			opt;
	int			status;
	int			i;
	GlobalTransactionId next_gxid = InvalidGlobalTransactionId;
	int			ctlfd;

	/*
	 * Catch standard options before doing much else
	 */
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help(argv[0]);
			exit(0);
		}
	}

	ListenAddresses = GTM_DEFAULT_HOSTNAME;
	GTMPortNumber = GTM_DEFAULT_PORT;
	
	/*
	 * Parse the command like options and set variables
	 */
	while ((opt = getopt(argc, argv, "h:p:x:D:l:")) != -1)
	{
		switch (opt)
		{
			case 'h':
				ListenAddresses = strdup(optarg);
				break;

			case 'p':
				GTMPortNumber = atoi(optarg);
				break;

			case 'x':
				next_gxid = (GlobalTransactionId )atoll(optarg);
				break;

			case 'D':
				GTMDataDir = strdup(optarg);
				canonicalize_path(GTMDataDir);
				break;

			case 'l':
				GTMLogFile = strdup(optarg);
				break;

			default:
				write_stderr("Try \"%s --help\" for more information.\n",
							 progname);
		}
	}

	if (GTMDataDir == NULL)
	{
		write_stderr("GTM data directory must be specified\n");
		write_stderr("Try \"%s --help\" for more information.\n",
					 progname);
		exit(1);
	}
	/*
	 * GTM accepts no non-option switch arguments.
	 */
	if (optind < argc)
	{
		write_stderr("%s: invalid argument: \"%s\"\n",
					 progname, argv[optind]);
		write_stderr("Try \"%s --help\" for more information.\n",
					 progname);
		exit(1);
	}

	/*
	 * Some basic initialization must happen before we do anything
	 * useful
	 */
	BaseInit();

	elog(DEBUG3, "Starting GTM server at (%s:%d) -- control file %s", ListenAddresses, GTMPortNumber, GTMControlFile);

	/*
	 * Read the last GXID and start from there
	 */

	ctlfd = open(GTMControlFile, O_RDONLY);

	GTM_RestoreTxnInfo(ctlfd, next_gxid);
	GTM_RestoreSeqInfo(ctlfd);

	close(ctlfd);

	/* Recover Data of Registered nodes. */
	Recovery_RestoreRegisterInfo();

	/*
	 * Establish input sockets.
	 */
	for (i = 0; i < MAXLISTEN; i++)
		ListenSocket[i] = -1;

	if (ListenAddresses)
	{
		int			success = 0;

			status = StreamServerPort(AF_UNSPEC, ListenAddresses,
									  (unsigned short) GTMPortNumber,
									  ListenSocket, MAXLISTEN);
		if (status == STATUS_OK)
			success++;
		else
			ereport(FATAL,
					(errmsg("could not create listen socket for \"%s\"",
							ListenAddresses)));
	}

	/*
	 * check that we have some socket to listen on
	 */
	if (ListenSocket[0] == -1)
		ereport(FATAL,
				(errmsg("no socket created for listening")));

	/*
	 * Record gtm options.  We delay this till now to avoid recording
	 * bogus options
	 */
	if (!CreateOptsFile(argc, argv))
		exit(1);

	pqsignal(SIGHUP, GTM_SigleHandler);
	pqsignal(SIGKILL, GTM_SigleHandler);
	pqsignal(SIGQUIT, GTM_SigleHandler);
	pqsignal(SIGTERM, GTM_SigleHandler);
	pqsignal(SIGINT, GTM_SigleHandler);

	pqinitmask();

	/*
	 * Accept any new connections. Fork a new thread for each incoming
	 * connection
	 */
	status = ServerLoop();

	/*
	 * ServerLoop probably shouldn't ever return, but if it does, close down.
	 */
	exit(status != STATUS_OK);

	return 0;					/* not reached */
}

/*
 * ConnCreate -- create a local connection data structure
 */
static Port *
ConnCreate(int serverFd)
{
	Port	   *port;

	if (!(port = (Port *) calloc(1, sizeof(Port))))
	{
		ereport(LOG,
				(ENOMEM,
				 errmsg("out of memory")));
		exit(1);
	}

	if (StreamConnection(serverFd, port) != STATUS_OK)
	{
		if (port->sock >= 0)
			StreamClose(port->sock);
		ConnFree(port);
		port = NULL;
	}

	port->conn_id = InvalidGTMProxyConnID;
	return port;
}

/*
 * ConnFree -- free a local connection data structure
 */
void
ConnFree(Port *conn)
{
	free(conn);
}

/*
 * Main idle loop of postmaster
 */
static int
ServerLoop(void)
{
	fd_set		readmask;
	int			nSockets;

	nSockets = initMasks(&readmask);

	for (;;)
	{
		fd_set		rmask;
		int			selres;

		//MemoryContextStats(TopMostMemoryContext);
		
		/*
		 * Wait for a connection request to arrive.
		 *
		 * We wait at most one minute, to ensure that the other background
		 * tasks handled below get done even when no requests are arriving.
		 */
		memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));

		PG_SETMASK(&UnBlockSig);

		if (GTMAbortPending)
		{
			int ctlfd;

			/*
			 * XXX We should do a clean shutdown here. For the time being, just
			 * write the next GXID to be issued in the control file and exit
			 * gracefully
			 */

			/*
			 * Tell GTM that we are shutting down so that no new GXIDs are
			 * issued this point onwards
			 */
			GTM_SetShuttingDown();

			ctlfd = open(GTMControlFile, O_WRONLY | O_TRUNC | O_CREAT,
						 S_IRUSR | S_IWUSR);
			if (ctlfd == -1)
			{
				fprintf(stderr, "Failed to create/open the control file\n");
				exit(2);
			}

			GTM_SaveTxnInfo(ctlfd);
			GTM_SaveSeqInfo(ctlfd);

			close(ctlfd);

			exit(1);
		}

		{
			/* must set timeout each time; some OSes change it! */
			struct timeval timeout;

			timeout.tv_sec = 60;
			timeout.tv_usec = 0;

			selres = select(nSockets, &rmask, NULL, NULL, &timeout);
		}

		/*
		 * Block all signals until we wait again.  (This makes it safe for our
		 * signal handlers to do nontrivial work.)
		 */
		PG_SETMASK(&BlockSig);

		/* Now check the select() result */
		if (selres < 0)
		{
			if (errno != EINTR && errno != EWOULDBLOCK)
			{
				ereport(LOG,
						(EACCES,
						 errmsg("select() failed in postmaster: %m")));
				return STATUS_ERROR;
			}
		}

		/*
		 * New connection pending on any of our sockets? If so, fork a child
		 * process to deal with it.
		 */
		if (selres > 0)
		{
			int			i;

			for (i = 0; i < MAXLISTEN; i++)
			{
				if (ListenSocket[i] == -1)
					break;
				if (FD_ISSET(ListenSocket[i], &rmask))
				{
					Port	   *port;

					port = ConnCreate(ListenSocket[i]);
					if (port)
					{
						if (GTMAddConnection(port) != STATUS_OK)
						{
							elog(ERROR, "Too many connections");
							StreamClose(port->sock);
							ConnFree(port);
						}
					}
				}
			}
		}
	}
}

/*
 * Initialise the masks for select() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */
static int
initMasks(fd_set *rmask)
{
	int			maxsock = -1;
	int			i;

	FD_ZERO(rmask);

	for (i = 0; i < MAXLISTEN; i++)
	{
		int			fd = ListenSocket[i];

		if (fd == -1)
			break;
		FD_SET(fd, rmask);
		if (fd > maxsock)
			maxsock = fd;
	}

	return maxsock + 1;
}


void *
GTM_ThreadMain(void *argp)
{
	GTM_ThreadInfo *thrinfo = (GTM_ThreadInfo *)argp;
	int qtype;
	StringInfoData input_message;
	sigjmp_buf  local_sigjmp_buf;

	elog(DEBUG3, "Starting the connection helper thread");
	

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 *
	 * This context is thread-specific
	 */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE,
										   false);
	

	{
		/*
		 * We expect a startup message at the very start. The message type is
		 * REGISTER_COORD, followed by the 4 byte coordinator ID
		 */
		char startup_type;
		GTM_StartupPacket sp;
		StringInfoData inBuf;

		startup_type = pq_getbyte(thrinfo->thr_conn->con_port);

		if (startup_type != 'A')
			ereport(ERROR,
					(EPROTO,
					 errmsg("Expecting a startup message, but received %c",
						 startup_type)));

		initStringInfo(&inBuf);
		
		/*
		 * All frontend messages have a length word next
		 * after the type code; we can read the message contents independently of
		 * the type.
		 */
		if (pq_getmessage(thrinfo->thr_conn->con_port, &inBuf, 0))
			ereport(ERROR,
					(EPROTO,
					 errmsg("Expecting coordinator ID, but received EOF")));

		memcpy(&sp,
			   pq_getmsgbytes(&inBuf, sizeof (GTM_StartupPacket)),
			   sizeof (GTM_StartupPacket));
		pq_getmsgend(&inBuf);

		GTM_RegisterPGXCNode(thrinfo->thr_conn->con_port, sp.sp_cid);
		thrinfo->thr_conn->con_port->remote_type = sp.sp_remotetype;
		thrinfo->thr_conn->con_port->is_postmaster = sp.sp_ispostmaster;
	}

	{
		/*
		 * Send a dummy authentication request message 'R' as the client
		 * expects that in the current protocol
		 */
		StringInfoData buf;
		pq_beginmessage(&buf, 'R');
		pq_endmessage(thrinfo->thr_conn->con_port, &buf);
		pq_flush(thrinfo->thr_conn->con_port);

		elog(DEBUG3, "Sent connection authentication message to the client");
	}

	/*
	 * Get the input_message in the TopMemoryContext so that we don't need to
	 * free/palloc it for every incoming message. Unlike Postgres, we don't
	 * expect the incoming messages to be of arbitrary sizes
	 */

	initStringInfo(&input_message);

	/*
	 * POSTGRES main processing loop begins here
	 *
	 * If an exception is encountered, processing resumes here so we abort the
	 * current transaction and start a new one.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 */

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*
		 * NOTE: if you are tempted to add more code in this if-block,
		 * consider the high probability that it should be in
		 * AbortTransaction() instead.	The only stuff done directly here
		 * should be stuff that is guaranteed to apply *only* for outer-level
		 * error recovery, such as adjusting the FE/BE protocol status.
		 */
		
		/* Report the error to the client and/or server log */
		if (thrinfo->thr_conn)
			EmitErrorReport(thrinfo->thr_conn->con_port);
		else
			EmitErrorReport(NULL);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;


	for (;;)
	{
		/*
		 * Release storage left over from prior query cycle, and create a new
		 * query input buffer in the cleared MessageContext.
		 */
		MemoryContextSwitchTo(MessageContext);
		MemoryContextResetAndDeleteChildren(MessageContext);

		/*
		 * Just reset the input buffer to avoid repeated palloc/pfrees
		 *
		 * XXX We should consider resetting the MessageContext periodically to
		 * handle any memory leaks
		 */
		resetStringInfo(&input_message);

		/*
		 * (3) read a command (loop blocks here)
		 */
		qtype = ReadCommand(thrinfo->thr_conn->con_port, &input_message);

		switch(qtype)
		{
			case 'C':
				ProcessCommand(thrinfo->thr_conn->con_port, &input_message);
				break;
			
			case 'X':
			case EOF:
				/*
				 * Connection termination request
				 * Remove all transactions opened within the thread 
				 */
				GTM_RemoveAllTransInfos(-1);

				/* Disconnect node if necessary */
				Recovery_PGXCNodeDisconnect(thrinfo->thr_conn->con_port);
				pthread_exit(thrinfo);
				break;
			
			case 'F':
				/*
				 * Flush all the outgoing data on the wire. Consume the message
				 * type field for sanity
				 */
				pq_getmsgint(&input_message, sizeof (GTM_MessageType));
				pq_getmsgend(&input_message);
				pq_flush(thrinfo->thr_conn->con_port);
				break;

			default:
				/*
				 * Remove all transactions opened within the thread 
				 */
				GTM_RemoveAllTransInfos(-1);

				/* Disconnect node if necessary */
				Recovery_PGXCNodeDisconnect(thrinfo->thr_conn->con_port);

				ereport(FATAL,
						(EPROTO,
						 errmsg("invalid frontend message type %d",
								qtype)));
				break;
		}
		
	}

	/* can't get here because the above loop never exits */
	Assert(false);

	return thrinfo;
}

void
ProcessCommand(Port *myport, StringInfo input_message)
{
	GTM_MessageType mtype;
	GTM_ProxyMsgHeader proxyhdr;

	if (myport->remote_type == PGXC_NODE_GTM_PROXY)
		pq_copymsgbytes(input_message, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	else
		proxyhdr.ph_conid = InvalidGTMProxyConnID;

	myport->conn_id = proxyhdr.ph_conid;
	mtype = pq_getmsgint(input_message, sizeof (GTM_MessageType));

	switch (mtype)
	{
		case MSG_NODE_REGISTER:
		case MSG_NODE_UNREGISTER:
			ProcessPGXCNodeCommand(myport, mtype, input_message);
			break;

		case MSG_TXN_BEGIN:
		case MSG_TXN_BEGIN_GETGXID:
		case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
		case MSG_TXN_PREPARE:
		case MSG_TXN_START_PREPARED:
		case MSG_TXN_COMMIT:
		case MSG_TXN_COMMIT_PREPARED:
		case MSG_TXN_ROLLBACK:
		case MSG_TXN_GET_GXID:
		case MSG_TXN_BEGIN_GETGXID_MULTI:
		case MSG_TXN_COMMIT_MULTI:
		case MSG_TXN_ROLLBACK_MULTI:
		case MSG_TXN_GET_GID_DATA:
			ProcessTransactionCommand(myport, mtype, input_message);
			break;

		case MSG_SNAPSHOT_GET:
		case MSG_SNAPSHOT_GXID_GET:
		case MSG_SNAPSHOT_GET_MULTI:
			ProcessSnapshotCommand(myport, mtype, input_message);
			break;

		case MSG_SEQUENCE_INIT:
		case MSG_SEQUENCE_GET_CURRENT:
		case MSG_SEQUENCE_GET_NEXT:
		case MSG_SEQUENCE_GET_LAST:
		case MSG_SEQUENCE_SET_VAL:
		case MSG_SEQUENCE_RESET:
		case MSG_SEQUENCE_CLOSE:
		case MSG_SEQUENCE_RENAME:
		case MSG_SEQUENCE_ALTER:
			ProcessSequenceCommand(myport, mtype, input_message);
			break;

		case MSG_TXN_GET_STATUS:
		case MSG_TXN_GET_ALL_PREPARED:
			ProcessQueryCommand(myport, mtype, input_message);
			break;

		case MSG_BACKEND_DISCONNECT:
			GTM_RemoveAllTransInfos(proxyhdr.ph_conid);

			/* Mark PGXC Node as disconnected if backend disconnected is postmaster */
			ProcessPGXCNodeBackendDisconnect(myport, input_message);
			break;

		default:
			ereport(FATAL,
					(EPROTO,
					 errmsg("invalid frontend message type %d",
							mtype)));
	}
}

static int
GTMAddConnection(Port *port)
{
	GTM_ConnectionInfo *conninfo = NULL;

	conninfo = (GTM_ConnectionInfo *)palloc(sizeof (GTM_ConnectionInfo));

	if (conninfo == NULL)
	{
		ereport(ERROR,
				(ENOMEM,
				 	errmsg("Out of memory")));
		return STATUS_ERROR;
	}
		
	elog(DEBUG3, "Started new connection");
	conninfo->con_port = port;

	/*
	 * XXX Start the thread
	 */
	if (GTM_ThreadCreate(conninfo, GTM_ThreadMain) == NULL)
	{
		elog(ERROR, "failed to create a new thread");
		return STATUS_ERROR;
	}

	return STATUS_OK;
}

/* ----------------
 *		ReadCommand reads a command from either the frontend or
 *		standard input, places it in inBuf, and returns the
 *		message type code (first byte of the message).
 *		EOF is returned if end of file.
 * ----------------
 */
static int
ReadCommand(Port *myport, StringInfo inBuf)
{
	int 			qtype;

	/*
	 * Get message type code from the frontend.
	 */
	qtype = pq_getbyte(myport);

	if (qtype == EOF)			/* frontend disconnected */
	{
		ereport(COMMERROR,
				(EPROTO,
				 errmsg("unexpected EOF on client connection")));
		return EOF;
	}

	/*
	 * Validate message type code before trying to read body; if we have lost
	 * sync, better to say "command unknown" than to run out of memory because
	 * we used garbage as a length word.
	 *
	 * This also gives us a place to set the doing_extended_query_message flag
	 * as soon as possible.
	 */
	switch (qtype)
	{
		case 'C':
			break;

		case 'X':
			break;

		case 'F':
			break;

		default:

			/*
			 * Otherwise we got garbage from the frontend.	We treat this as
			 * fatal because we have probably lost message boundary sync, and
			 * there's no good way to recover.
			 */
			ereport(ERROR,
					(EPROTO,
					 errmsg("invalid frontend message type %d", qtype)));

			break;
	}

	/*
	 * In protocol version 3, all frontend messages have a length word next
	 * after the type code; we can read the message contents independently of
	 * the type.
	 */
	if (pq_getmessage(myport, inBuf, 0))
		return EOF;			/* suitable message already logged */

	return qtype;
}

static void
ProcessPGXCNodeCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
	switch (mtype)
	{
		case MSG_NODE_REGISTER:
			ProcessPGXCNodeRegister(myport, message);
			break;

		case MSG_NODE_UNREGISTER:
			ProcessPGXCNodeUnregister(myport, message);
			break;

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}
}

static void
ProcessTransactionCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
	elog(DEBUG1, "ProcessTransactionCommand: mtype:%d", mtype);

	switch (mtype)
	{
		case MSG_TXN_BEGIN:
			ProcessBeginTransactionCommand(myport, message);
			break;

		case MSG_TXN_BEGIN_GETGXID:
			ProcessBeginTransactionGetGXIDCommand(myport, message);
			break;

		case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
			ProcessBeginTransactionGetGXIDAutovacuumCommand(myport, message);
			break;

		case MSG_TXN_BEGIN_GETGXID_MULTI:
			ProcessBeginTransactionGetGXIDCommandMulti(myport, message);
			break;

		case MSG_TXN_START_PREPARED:
			ProcessStartPreparedTransactionCommand(myport, message);
			break;

		case MSG_TXN_PREPARE:
			ProcessPrepareTransactionCommand(myport, message);
			break;

		case MSG_TXN_COMMIT:
			ProcessCommitTransactionCommand(myport, message);
			break;

		case MSG_TXN_COMMIT_PREPARED:
			ProcessCommitPreparedTransactionCommand(myport, message);
			break;

		case MSG_TXN_ROLLBACK:
			ProcessRollbackTransactionCommand(myport, message);
			break;

		case MSG_TXN_COMMIT_MULTI:
			ProcessCommitTransactionCommandMulti(myport, message);
			break;

		case MSG_TXN_ROLLBACK_MULTI:
			ProcessRollbackTransactionCommandMulti(myport, message);
			break;

		case MSG_TXN_GET_GXID:
			ProcessGetGXIDTransactionCommand(myport, message);
			break;

		case MSG_TXN_GET_GID_DATA:
			ProcessGetGIDDataTransactionCommand(myport, message);

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}
}

static void
ProcessSnapshotCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
	switch (mtype)
	{
		case MSG_SNAPSHOT_GET:
			ProcessGetSnapshotCommand(myport, message, false);
			break;

		case MSG_SNAPSHOT_GET_MULTI:
			ProcessGetSnapshotCommandMulti(myport, message);
			break;

		case MSG_SNAPSHOT_GXID_GET:
			ProcessGetSnapshotCommand(myport, message, true);
			break;

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}

}

static void
ProcessSequenceCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
	switch (mtype)
	{
		case MSG_SEQUENCE_INIT:
			ProcessSequenceInitCommand(myport, message);
			break;

		case MSG_SEQUENCE_ALTER:
			ProcessSequenceAlterCommand(myport, message);
			break;

		case MSG_SEQUENCE_GET_CURRENT:
			ProcessSequenceGetCurrentCommand(myport, message);
			break;

		case MSG_SEQUENCE_GET_NEXT:
			ProcessSequenceGetNextCommand(myport, message);
			break;

		case MSG_SEQUENCE_SET_VAL:
			ProcessSequenceSetValCommand(myport, message);
			break;

		case MSG_SEQUENCE_RESET:
			ProcessSequenceResetCommand(myport, message);
			break;

		case MSG_SEQUENCE_CLOSE:
			ProcessSequenceCloseCommand(myport, message);
			break;

		case MSG_SEQUENCE_RENAME:
			ProcessSequenceRenameCommand(myport, message);
			break;

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}

}

static void
ProcessQueryCommand(Port *myport, GTM_MessageType mtype, StringInfo message)
{
	switch (mtype)
	{
		case MSG_TXN_GET_STATUS:
		case MSG_TXN_GET_ALL_PREPARED:
			break;

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}

}

static void
GTM_RegisterPGXCNode(Port *myport, GTM_PGXCNodeId cid)
{
	elog(DEBUG3, "Registering coordinator with cid %d", cid);
	myport->pgxc_node_id = cid;
}


static void
GTM_UnregisterPGXCNode(Port *myport, GTM_PGXCNodeId cid)
{
	/*
	 * Do a clean shutdown
	 */
	return;
}

/*
 * Validate the proposed data directory
 */
static void
checkDataDir(void)
{
	struct stat stat_buf;

	Assert(GTMDataDir);

retry:
	if (stat(GTMDataDir, &stat_buf) != 0)
	{
		if (errno == ENOENT)
		{
			if (mkdir(GTMDataDir, 0700) != 0)
			{
				ereport(FATAL,
						(errno,
						 errmsg("failed to create the directory \"%s\"",
							 GTMDataDir)));
			}
			goto retry;
		}
		else
			ereport(FATAL,
					(EPERM,
				 errmsg("could not read permissions of directory \"%s\": %m",
						GTMDataDir)));
	}

	/* eventual chdir would fail anyway, but let's test ... */
	if (!S_ISDIR(stat_buf.st_mode))
		ereport(FATAL,
				(EINVAL,
				 errmsg("specified data directory \"%s\" is not a directory",
						GTMDataDir)));

	/*
	 * Check that the directory belongs to my userid; if not, reject.
	 *
	 * This check is an essential part of the interlock that prevents two
	 * postmasters from starting in the same directory (see CreateLockFile()).
	 * Do not remove or weaken it.
	 *
	 * XXX can we safely enable this check on Windows?
	 */
#if !defined(WIN32) && !defined(__CYGWIN__)
	if (stat_buf.st_uid != geteuid())
		ereport(FATAL,
				(EINVAL,
				 errmsg("data directory \"%s\" has wrong ownership",
						GTMDataDir),
				 errhint("The server must be started by the user that owns the data directory.")));
#endif
}

/*
 * Change working directory to DataDir.  Most of the postmaster and backend
 * code assumes that we are in DataDir so it can use relative paths to access
 * stuff in and under the data directory.  For convenience during path
 * setup, however, we don't force the chdir to occur during SetDataDir.
 */
static void
ChangeToDataDir(void)
{
	if (chdir(GTMDataDir) < 0)
		ereport(FATAL,
				(EINVAL,
				 errmsg("could not change directory to \"%s\": %m",
						GTMDataDir)));
}

/*
 * Create the data directory lockfile.
 *
 * When this is called, we must have already switched the working
 * directory to DataDir, so we can just use a relative path.  This
 * helps ensure that we are locking the directory we should be.
 */
static void
CreateDataDirLockFile()
{
	CreateLockFile(GTM_PID_FILE, GTMDataDir);
}

/*
 * Create a lockfile.
 *
 * filename is the name of the lockfile to create.
 * amPostmaster is used to determine how to encode the output PID.
 * isDDLock and refName are used to determine what error message to produce.
 */
static void
CreateLockFile(const char *filename, const char *refName)
{
	int			fd;
	char		buffer[MAXPGPATH + 100];
	int			ntries;
	int			len;
	int			encoded_pid;
	pid_t		other_pid;
	pid_t		my_pid = getpid();

	/*
	 * We need a loop here because of race conditions.	But don't loop forever
	 * (for example, a non-writable $PGDATA directory might cause a failure
	 * that won't go away).  100 tries seems like plenty.
	 */
	for (ntries = 0;; ntries++)
	{
		/*
		 * Try to create the lock file --- O_EXCL makes this atomic.
		 *
		 * Think not to make the file protection weaker than 0600.	See
		 * comments below.
		 */
		fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			break;				/* Success; exit the retry loop */

		/*
		 * Couldn't create the pid file. Probably it already exists.
		 */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			ereport(FATAL,
					(EINVAL,
					 errmsg("could not create lock file \"%s\": %m",
							filename)));

		/*
		 * Read the file to get the old owner's PID.  Note race condition
		 * here: file might have been deleted since we tried to create it.
		 */
		fd = open(filename, O_RDONLY, 0600);
		if (fd < 0)
		{
			if (errno == ENOENT)
				continue;		/* race condition; try again */
			ereport(FATAL,
					(EINVAL,
					 errmsg("could not open lock file \"%s\": %m",
							filename)));
		}
		if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
			ereport(FATAL,
					(EINVAL,
					 errmsg("could not read lock file \"%s\": %m",
							filename)));
		close(fd);

		buffer[len] = '\0';
		encoded_pid = atoi(buffer);
		other_pid = (pid_t) encoded_pid;

		if (other_pid <= 0)
			elog(FATAL, "bogus data in lock file \"%s\": \"%s\"",
				 filename, buffer);

		/*
		 * Check to see if the other process still exists
		 *
		 * If the PID in the lockfile is our own PID or our parent's PID, then
		 * the file must be stale (probably left over from a previous system
		 * boot cycle).  We need this test because of the likelihood that a
		 * reboot will assign exactly the same PID as we had in the previous
		 * reboot.	Also, if there is just one more process launch in this
		 * reboot than in the previous one, the lockfile might mention our
		 * parent's PID.  We can reject that since we'd never be launched
		 * directly by a competing postmaster.	We can't detect grandparent
		 * processes unfortunately, but if the init script is written
		 * carefully then all but the immediate parent shell will be
		 * root-owned processes and so the kill test will fail with EPERM.
		 *
		 * We can treat the EPERM-error case as okay because that error
		 * implies that the existing process has a different userid than we
		 * do, which means it cannot be a competing postmaster.  A postmaster
		 * cannot successfully attach to a data directory owned by a userid
		 * other than its own.	(This is now checked directly in
		 * checkDataDir(), but has been true for a long time because of the
		 * restriction that the data directory isn't group- or
		 * world-accessible.)  Also, since we create the lockfiles mode 600,
		 * we'd have failed above if the lockfile belonged to another userid
		 * --- which means that whatever process kill() is reporting about
		 * isn't the one that made the lockfile.  (NOTE: this last
		 * consideration is the only one that keeps us from blowing away a
		 * Unix socket file belonging to an instance of Postgres being run by
		 * someone else, at least on machines where /tmp hasn't got a
		 * stickybit.)
		 *
		 * Windows hasn't got getppid(), but doesn't need it since it's not
		 * using real kill() either...
		 *
		 * Normally kill() will fail with ESRCH if the given PID doesn't
		 * exist.
		 */
		if (other_pid != my_pid
#ifndef WIN32
			&& other_pid != getppid()
#endif
			)
		{
			if (kill(other_pid, 0) == 0 ||
				(errno != ESRCH && errno != EPERM))
			{
				/* lockfile belongs to a live process */
				ereport(FATAL,
						(EINVAL,
						 errmsg("lock file \"%s\" already exists",
								filename),
						  errhint("Is another GTM (PID %d) running in data directory \"%s\"?",
								  (int) other_pid, refName)));
			}
		}

		/*
		 * Looks like nobody's home.  Unlink the file and try again to create
		 * it.	Need a loop because of possible race condition against other
		 * would-be creators.
		 */
		if (unlink(filename) < 0)
			ereport(FATAL,
					(EACCES,
					 errmsg("could not remove old lock file \"%s\": %m",
							filename),
					 errhint("The file seems accidentally left over, but "
						   "it could not be removed. Please remove the file "
							 "by hand and try again.")));
	}

	/*
	 * Successfully created the file, now fill it.
	 */
	snprintf(buffer, sizeof(buffer), "%d\n%s\n",
			 (int) my_pid, GTMDataDir);
	errno = 0;
	if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
	{
		int			save_errno = errno;

		close(fd);
		unlink(filename);
		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(FATAL,
				(EACCES,
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}
	if (close(fd))
	{
		int			save_errno = errno;

		unlink(filename);
		errno = save_errno;
		ereport(FATAL,
				(EACCES,
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}
}

/*
 * Create the opts file
 */
static bool
CreateOptsFile(int argc, char *argv[])
{
	FILE	   *fp;
	int			i;

#define OPTS_FILE	"gtm.opts"

	if ((fp = fopen(OPTS_FILE, "w")) == NULL)
	{
		elog(LOG, "could not create file \"%s\": %m", OPTS_FILE);
		return false;
	}

	for (i = 1; i < argc; i++)
		fprintf(fp, " \"%s\"", argv[i]);
	fputs("\n", fp);

	if (fclose(fp))
	{
		elog(LOG, "could not write file \"%s\": %m", OPTS_FILE);
		return false;
	}

	return true;
}

/* delete pid file */
static void
DeleteLockFile(const char *filename)
{
	if (unlink(filename) < 0)
		ereport(FATAL,
			(EACCES,
			 errmsg("could not remove old lock file \"%s\": %m",
					filename),
			 errhint("The file seems accidentally left over, but "
					 "it could not be removed. Please remove the file "
					 "by hand and try again.")));
}
