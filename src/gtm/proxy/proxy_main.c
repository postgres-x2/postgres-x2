/*-------------------------------------------------------------------------
 *
 * proxy_main.c
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

#include "gtm/gtm_c.h"
#include "gtm/gtm_proxy.h"
#include "gtm/elog.h"
#include "gtm/memutils.h"
#include "gtm/gtm_list.h"
#include "gtm/libpq.h"
#include "gtm/libpq-be.h"
#include "gtm/libpq-fe.h"
#include "gtm/pqsignal.h"
#include "gtm/pqformat.h"
#include "gtm/assert.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_msg.h"
#include "gtm/libpq-int.h"

extern int	optind;
extern char *optarg;

#define GTM_MAX_PATH			1024
#define GTM_PROXY_DEFAULT_HOSTNAME	"*"
#define GTM_PROXY_DEFAULT_PORT		6666
#define GTM_PROXY_DEFAULT_WORKERS	2
#define GTM_PID_FILE			"gtm_proxy.pid"
#define GTM_LOG_FILE			"gtm_proxy.log"

static char *progname = "gtm_proxy";
char	   *ListenAddresses;
int			GTMProxyPortNumber;
int			GTMProxyWorkerThreads;
char		*GTMProxyDataDir;

char		*GTMServerHost;
int			GTMServerPortNumber;

/* The socket(s) we're listening to. */
#define MAXLISTEN	64
static int	ListenSocket[MAXLISTEN];

pthread_key_t	threadinfo_key;
static bool		GTMProxyAbortPending = false;

static Port *ConnCreate(int serverFd);
static void ConnFree(Port *conn);
static int ServerLoop(void);
static int initMasks(fd_set *rmask);
void *GTMProxy_ThreadMain(void *argp);
static int GTMProxyAddConnection(Port *port);
static int ReadCommand(GTMProxy_ConnectionInfo *conninfo, StringInfo inBuf);
static void GTMProxy_HandshakeConnection(GTMProxy_ConnectionInfo *conninfo);
static void GTMProxy_HandleDisconnect(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn);

static void GTMProxy_ProxyCommand(GTMProxy_ConnectionInfo *conninfo,
		GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);

static void ProcessCommand(GTMProxy_ConnectionInfo *conninfo,
		GTM_Conn *gtm_conn, StringInfo input_message);
static void ProcessCoordinatorCommand(GTMProxy_ConnectionInfo *conninfo,
		GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);
static void ProcessTransactionCommand(GTMProxy_ConnectionInfo *conninfo,
		GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);
static void ProcessSnapshotCommand(GTMProxy_ConnectionInfo *conninfo,
	   	GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);
static void ProcessSequenceCommand(GTMProxy_ConnectionInfo *conninfo,
	   	GTM_Conn *gtm_conn, GTM_MessageType mtype, StringInfo message);

static void GTMProxy_RegisterCoordinator(GTMProxy_ConnectionInfo *conninfo,
		GTM_CoordinatorId coordinator_id);
static void GTMProxy_UnregisterCoordinator(GTMProxy_ConnectionInfo *conninfo,
		GTM_CoordinatorId coordinator_id);

static void ProcessResponse(GTMProxy_ThreadInfo *thrinfo,
		GTMProxy_CommandInfo *cmdinfo, GTM_Result *res);

static void GTMProxy_ProcessPendingCommands(GTMProxy_ThreadInfo *thrinfo);
static void GTMProxy_CommandPending(GTMProxy_ConnectionInfo *conninfo,
		GTM_MessageType mtype, GTMProxy_CommandData cmd_data);

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
static GTMProxy_ThreadInfo *
MainThreadInit()
{
	GTMProxy_ThreadInfo *thrinfo;

	pthread_key_create(&threadinfo_key, NULL);
	
	/*
	 * Initialize the lock protecting the global threads info
	 */
	GTM_RWLockInit(&GTMProxyThreads->gt_lock);

	/*
	 * We are called even before memory context management is setup. We must
	 * use malloc
	 */
	thrinfo = (GTMProxy_ThreadInfo *)malloc(sizeof (GTMProxy_ThreadInfo));

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
	GTMProxy_ThreadInfo *thrinfo;

	thrinfo = MainThreadInit();

	MyThreadID = pthread_self();

	MemoryContextInit();

	checkDataDir();
	ChangeToDataDir();
	CreateDataDirLockFile();

	if (GTMLogFile == NULL)
	{
		GTMLogFile = (char *) malloc(GTM_MAX_PATH);
		sprintf(GTMLogFile, "%s/%s", GTMProxyDataDir, GTM_LOG_FILE);
	}

	DebugFileOpen();

	/*
	 * The memory context is now set up.
	 * Add the thrinfo structure in the global array
	 */
	if (GTMProxy_ThreadAdd(thrinfo) == -1)
	{
		fprintf(stderr, "GTMProxy_ThreadAdd for main thread failed: %d", errno);
		fflush(stdout);
		fflush(stderr);
	}
}

static void
GTMProxy_SigleHandler(int signal)
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
	/* Delete pid file before shutting down */
	DeleteLockFile(GTM_PID_FILE);

	PG_SETMASK(&BlockSig);
	GTMProxyAbortPending = true;

	return;
}

/*
 * Help display should match 
 */
static void
help(const char *progname)
{
	printf(_("This is the GTM proxy.\n\n"));
	printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
	printf(_("Options:\n"));
	printf(_("  -h hostname     GTM proxy hostname/IP\n"));
	printf(_("  -p port			GTM proxy port number\n"));
	printf(_("  -s hostname		GTM server hostname/IP \n"));
	printf(_("  -t port			GTM server port number\n"));
	printf(_("  -n count		Number of worker threads\n"));
	printf(_("  -D directory	GTM proxy working directory\n"));
	printf(_("  -l filename		GTM proxy log file name \n"));
	printf(_("  --help          show this help, then exit\n"));
}


int
main(int argc, char *argv[])
{
	int			opt;
	int			status;
	int			i;

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

	ListenAddresses = GTM_PROXY_DEFAULT_HOSTNAME;
	GTMProxyPortNumber = GTM_PROXY_DEFAULT_PORT;
	GTMProxyWorkerThreads = GTM_PROXY_DEFAULT_WORKERS;
	
	/*
	 * Parse the command like options and set variables
	 */
	while ((opt = getopt(argc, argv, "h:p:n:D:l:s:t:")) != -1)
	{
		switch (opt)
		{
			case 'h':
				/* Listen address of the proxy */
				ListenAddresses = strdup(optarg);
				break;

			case 'p':
				/* Port number for the proxy to listen on */
				GTMProxyPortNumber = atoi(optarg);
				break;

			case 'n':
				/* Number of worker threads */
				GTMProxyWorkerThreads = atoi(optarg);
				break;

			case 'D':
				GTMProxyDataDir = strdup(optarg);
				canonicalize_path(GTMProxyDataDir);
				break;

			case 'l':
				/* The log file */
				GTMLogFile = strdup(optarg);
				break;

			case 's':
				/* GTM server host name */
				GTMServerHost = strdup(optarg);
				break;

			case 't':
				/* GTM server port number */
				GTMServerPortNumber = atoi(optarg);
				break;

			default:
				write_stderr("Try \"%s --help\" for more information.\n",
							 progname);
		}
	}

	if (GTMProxyDataDir == NULL)
	{
		write_stderr("GTM Proxy data directory must be specified\n");
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

	elog(DEBUG3, "Starting GTM proxy at (%s:%d)", ListenAddresses, GTMProxyPortNumber);

	/*
	 * Establish input sockets.
	 */
	for (i = 0; i < MAXLISTEN; i++)
		ListenSocket[i] = -1;

	if (ListenAddresses)
	{
		int			success = 0;

			status = StreamServerPort(AF_UNSPEC, ListenAddresses,
									  (unsigned short) GTMProxyPortNumber,
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
	 * Record gtm proxy options.  We delay this till now to avoid recording
	 * bogus options
	 */
	if (!CreateOptsFile(argc, argv))
		exit(1);

	pqsignal(SIGHUP, GTMProxy_SigleHandler);
	pqsignal(SIGKILL, GTMProxy_SigleHandler);
	pqsignal(SIGQUIT, GTMProxy_SigleHandler);
	pqsignal(SIGTERM, GTMProxy_SigleHandler);
	pqsignal(SIGINT, GTMProxy_SigleHandler);

	pqinitmask();

	/*
	 * Pre-fork so many worker threads
	 */

	for (i = 0; i < GTMProxyWorkerThreads; i++)
	{
		/*
		 * XXX Start the worker thread
		 */
		if (GTMProxy_ThreadCreate(GTMProxy_ThreadMain) == NULL)
		{
			elog(ERROR, "failed to create a new thread");
			return STATUS_ERROR;
		}
	}

	/*
	 * Accept any new connections. Add for each incoming connection to one of
	 * the pre-forked threads.
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
static void
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

		/*
		 * Wait for a connection request to arrive.
		 *
		 * We wait at most one minute, to ensure that the other background
		 * tasks handled below get done even when no requests are arriving.
		 */
		memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));

		PG_SETMASK(&UnBlockSig);

		if (GTMProxyAbortPending)
		{
			/*
			 * Tell everybody that we are shutting down
			 * 
			 * !! TODO
			 */
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
		 * New connection pending on any of our sockets? If so, accept the
		 * connection and add it to one of the worker threads.
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
						if (GTMProxyAddConnection(port) != STATUS_OK)
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

/*
 * The main worker thread routine
 */
void *
GTMProxy_ThreadMain(void *argp)
{
	GTMProxy_ThreadInfo *thrinfo = (GTMProxy_ThreadInfo *)argp;
	int qtype;
	StringInfoData input_message;
	sigjmp_buf  local_sigjmp_buf;
	int32 saved_seqno = -1;
	int ii, nrfds;
	char gtm_connect_string[1024];

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

	/*
	 * Set up connection with the GTM server
	 */
	sprintf(gtm_connect_string, "host=%s port=%d coordinator_id=1 proxy=1",
			GTMServerHost, GTMServerPortNumber);

	thrinfo->thr_gtm_conn = PQconnectGTM(gtm_connect_string);

	if (thrinfo->thr_gtm_conn == NULL)
		elog(FATAL, "GTM connection failed");

	/*
	 * Get the input_message in the TopMemoryContext so that we don't need to
	 * free/palloc it for every incoming message. Unlike Postgres, we don't
	 * expect the incoming messages to be of arbitrary sizes
	 */

	initStringInfo(&input_message);

	/*
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
		if (thrinfo->thr_conn_count > 0)
		{
			for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
			{
				GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[ii];
				/*
				 * Now clean up disconnected connections
				 */
				if (conninfo->con_disconnected)
				{
					GTMProxy_ThreadRemoveConnection(thrinfo, conninfo);
					pfree(conninfo);
					ii--;
				}
				else
				{
					/*
					 * Consume all the pending data on this connection and send
					 * error report
					 */
					if (conninfo->con_pending_msg != MSG_TYPE_INVALID)
					{
						conninfo->con_port->PqRecvPointer = conninfo->con_port->PqRecvLength = 0;
						conninfo->con_pending_msg = MSG_TYPE_INVALID;
						EmitErrorReport(conninfo->con_port);
					}
				}
			}
		}
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
		ListCell *elem = NULL;
		GTM_Result *res = NULL;

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
		 * Check if there are any changes to the connection array assigned to
		 * this thread. If so, we need to rebuild the fd array.
		 */
		GTM_MutexLockAcquire(&thrinfo->thr_lock);
		if (saved_seqno != thrinfo->thr_seqno)
		{
			saved_seqno = thrinfo->thr_seqno;

			while (thrinfo->thr_conn_count <= 0)
			{
				/*
				 * No connections assigned to the thread. Wait for at least one
				 * connection to be assgined to us
				 */
				GTM_CVWait(&thrinfo->thr_cv, &thrinfo->thr_lock);
			}

			memset(thrinfo->thr_poll_fds, 0, sizeof (thrinfo->thr_poll_fds));

			/*
			 * Now grab all the open connections. We are holding the lock so no
			 * new connections can be added.
			 */
			for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
			{
				GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[ii];

				/* We detect if the connection has been dropped to avoid
				 * a segmentation fault. 
				*/
				if (conninfo->con_port == NULL)
				{
					conninfo->con_disconnected = true;
					continue;
				} 

				/*
				 * If this is a newly added connection, complete the handshake
				 */
				if (!conninfo->con_authenticated)
					GTMProxy_HandshakeConnection(conninfo);

				thrinfo->thr_poll_fds[ii].fd = conninfo->con_port->sock;
				thrinfo->thr_poll_fds[ii].events = POLLIN;
				thrinfo->thr_poll_fds[ii].revents = 0;
			}
		}
		GTM_MutexLockRelease(&thrinfo->thr_lock);

		while (true)
		{
			nrfds = poll(thrinfo->thr_poll_fds, thrinfo->thr_conn_count, 1000);

			if (nrfds < 0)
			{
				if (errno == EINTR)
					continue;
				elog(FATAL, "poll returned with error %d", nrfds);
			}
			else
				break;
		}

		if (nrfds == 0)
			continue;

		/*
		 * Initialize the lists
		 */
		thrinfo->thr_processed_commands = NIL;
		memset(thrinfo->thr_pending_commands, 0, sizeof (thrinfo->thr_pending_commands));

		/*
		 * Now, read command from each of the connections that has some data to
		 * be read.
		 */
		for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
		{
			GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[ii];
			thrinfo->thr_conn = conninfo;

			if (thrinfo->thr_poll_fds[ii].revents & POLLHUP)
			{
				/*
				 * The fd has become invalid. The connection is broken. Add it
				 * to the remove_list and cleanup at the end of this round of
				 * cleanup.
				 */
				GTMProxy_HandleDisconnect(thrinfo->thr_conn, thrinfo->thr_gtm_conn);
				continue;
			}

			if (thrinfo->thr_poll_fds[ii].revents & POLLIN)
			{
				/*
				 * (3) read a command (loop blocks here)
				 */
				qtype = ReadCommand(thrinfo->thr_conn, &input_message);

				switch(qtype)
				{
					case 'C':
						ProcessCommand(thrinfo->thr_conn, thrinfo->thr_gtm_conn,
								&input_message);
						break;

					case 'X':
					case EOF:
						/*
						 * Connection termination request
						 *
						 * Close the socket and remember the connection
						 * as disconnected. All such connections will be
						 * removed after the command processing is over. We
						 * can't remove it just yet because we pass the slot id
						 * to the server to quickly find the backend connection
						 * while processing proxied messages.
						 */
						GTMProxy_HandleDisconnect(thrinfo->thr_conn, thrinfo->thr_gtm_conn);
						break;
					default:
						/*
						 * Also disconnect if protocol error
						 */
						GTMProxy_HandleDisconnect(thrinfo->thr_conn, thrinfo->thr_gtm_conn);
						elog(ERROR, "Unexpected message, or client disconnected abruptly.");
						break;
				}

			}
		}

		/*
		 * Ok. All the commands are processed. Commands which can be proxied
		 * directly have been already sent to the GTM server. Now, group the
		 * remaining commands, send them to the server and flush the data.
		 */
		GTMProxy_ProcessPendingCommands(thrinfo);

		/*
		 * Add a special marker to tell the GTM server that we are done with
		 * one round of messages and the GTM server should flush all the
		 * pending responses after seeing this message.
		 */
		if (gtmpqPutMsgStart('F', true, thrinfo->thr_gtm_conn) ||
			gtmpqPutInt(MSG_DATA_FLUSH, sizeof (GTM_MessageType), thrinfo->thr_gtm_conn) ||
			gtmpqPutMsgEnd(thrinfo->thr_gtm_conn))
			elog(ERROR, "Error sending flush message");

		/*
		 * Make sure everything is on wire now
		 */
		gtmpqFlush(thrinfo->thr_gtm_conn);

		/*
		 * Read back the responses and put them on to the right backend
		 * connection.
		 */
		foreach(elem, thrinfo->thr_processed_commands)
		{
			GTMProxy_CommandInfo *cmdinfo = (GTMProxy_CommandInfo *)lfirst(elem);

			/*
			 * If this is a continuation of a multi-part command response, we
			 * don't need to read another result from the stream. The previous
			 * result contains our response and we should just read from it.
			 */
			if (cmdinfo->ci_res_index == 0)
			{
				if ((res = GTMPQgetResult(thrinfo->thr_gtm_conn)) == NULL)
					elog(ERROR, "GTMPQgetResult failed");
			}

			ProcessResponse(thrinfo, cmdinfo, res);
		}

		list_free_deep(thrinfo->thr_processed_commands);
		thrinfo->thr_processed_commands = NIL;

		/*
		 * Now clean up disconnected connections
		 */
		for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
		{
			GTMProxy_ConnectionInfo *conninfo = thrinfo->thr_all_conns[ii];
			if (conninfo->con_disconnected)
			{
				GTMProxy_ThreadRemoveConnection(thrinfo, conninfo);
				pfree(conninfo);
				ii--;
			}
		}
	}

	/* can't get here because the above loop never exits */
	Assert(false);

	return thrinfo;
}

/*
 * Add the accepted connection to the pool
 */
static int
GTMProxyAddConnection(Port *port)
{
	GTMProxy_ConnectionInfo *conninfo = NULL;

	conninfo = (GTMProxy_ConnectionInfo *)palloc0(sizeof (GTMProxy_ConnectionInfo));

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
	 * Add the conninfo struct to the next worker thread in round-robin manner
	 */
	GTMProxy_ThreadAddConnection(conninfo);

	return STATUS_OK;
}

void
ProcessCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
		StringInfo input_message)
{
	GTM_MessageType mtype;

	mtype = pq_getmsgint(input_message, sizeof (GTM_MessageType));

	switch (mtype)
	{
		case MSG_UNREGISTER_COORD:
			ProcessCoordinatorCommand(conninfo, gtm_conn, mtype, input_message);
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
		case MSG_TXN_GET_GID_DATA:
			ProcessTransactionCommand(conninfo, gtm_conn, mtype, input_message);
			break;

		case MSG_SNAPSHOT_GET:
		case MSG_SNAPSHOT_GXID_GET:
			ProcessSnapshotCommand(conninfo, gtm_conn, mtype, input_message);
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
			ProcessSequenceCommand(conninfo, gtm_conn, mtype, input_message);
			break;

		default:
			ereport(FATAL,
					(EPROTO,
					 errmsg("invalid frontend message type %d",
							mtype)));
	}

	conninfo->con_pending_msg = mtype;
}

static void
ProcessResponse(GTMProxy_ThreadInfo *thrinfo, GTMProxy_CommandInfo *cmdinfo,
		GTM_Result *res)
{
	StringInfoData buf;
	GlobalTransactionId gxid;
	GTM_Timestamp timestamp;

	switch (cmdinfo->ci_mtype)
	{
		case MSG_TXN_BEGIN_GETGXID:
			/*
			 * This is a grouped command. We send just the transaction count to
			 * the GTM server which responds back with the start GXID. We
			 * derive our GXID from the start GXID and the our position in the
			 * command queue
			 */
			if (res->gr_status == 0)
			{
				if (res->gr_type != TXN_BEGIN_GETGXID_MULTI_RESULT)
					elog(ERROR, "Wrong result");
				if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_get_multi.txn_count)
					elog(ERROR, "Too few GXIDs");

				gxid = res->gr_resdata.grd_txn_get_multi.start_gxid + cmdinfo->ci_res_index;

				/* Handle wraparound */
				if (gxid < res->gr_resdata.grd_txn_get_multi.start_gxid)
					gxid += FirstNormalGlobalTransactionId;

				/* Send back to each client the same timestamp value asked in this message */
				timestamp = res->gr_resdata.grd_txn_get_multi.timestamp;

				pq_beginmessage(&buf, 'S');
				pq_sendint(&buf, TXN_BEGIN_GETGXID_RESULT, 4);
				pq_sendbytes(&buf, (char *)&gxid, sizeof (GlobalTransactionId));
				pq_sendbytes(&buf, (char *)&timestamp, sizeof (GTM_Timestamp));
				pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
				pq_flush(cmdinfo->ci_conn->con_port);
			}
			else
			{
				pq_beginmessage(&buf, 'E');
				pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
				pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
				pq_flush(cmdinfo->ci_conn->con_port);
			}
			cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
			break;

		case MSG_TXN_COMMIT:
			if (res->gr_type != TXN_COMMIT_MULTI_RESULT)
				elog(ERROR, "Wrong result");
			/*
			 * These are grouped messages. We send an array of GXIDs to commit
			 * or rollback and the server sends us back an array of status
			 * codes.
			 */
			if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_rc_multi.txn_count)
				elog(ERROR, "Too few GXIDs");

			if (res->gr_resdata.grd_txn_rc_multi.status[cmdinfo->ci_res_index] == STATUS_OK)
			{
				pq_beginmessage(&buf, 'S');
				pq_sendint(&buf, TXN_COMMIT_RESULT, 4);
				pq_sendbytes(&buf, (char *)&cmdinfo->ci_data.cd_rc.gxid, sizeof (GlobalTransactionId));
				pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
				pq_flush(cmdinfo->ci_conn->con_port);
			}
			else
				ereport(ERROR2, (EINVAL, errmsg("Transaction commit failed")));
			cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
			break;

		case MSG_TXN_ROLLBACK:
			if (res->gr_type != TXN_ROLLBACK_MULTI_RESULT)
				elog(ERROR, "Wrong result");
			/*
			 * These are grouped messages. We send an array of GXIDs to commit
			 * or rollback and the server sends us back an array of status
			 * codes.
			 */
			if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_rc_multi.txn_count)
				elog(ERROR, "Too few GXIDs");

			if (res->gr_resdata.grd_txn_rc_multi.status[cmdinfo->ci_res_index] == STATUS_OK)
			{
				pq_beginmessage(&buf, 'S');
				pq_sendint(&buf, TXN_ROLLBACK_RESULT, 4);
				pq_sendbytes(&buf, (char *)&cmdinfo->ci_data.cd_rc.gxid, sizeof (GlobalTransactionId));
				pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
				pq_flush(cmdinfo->ci_conn->con_port);
			}
			else
				ereport(ERROR2, (EINVAL, errmsg("Transaction commit failed")));
			cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
			break;

		case MSG_SNAPSHOT_GET:
			if ((res->gr_type != SNAPSHOT_GET_RESULT) &&
				(res->gr_type != SNAPSHOT_GET_MULTI_RESULT))
				elog(ERROR, "Wrong result");

			if (cmdinfo->ci_res_index >= res->gr_resdata.grd_txn_snap_multi.txn_count)
				elog(ERROR, "Too few GXIDs");

			if (res->gr_resdata.grd_txn_snap_multi.status[cmdinfo->ci_res_index] == STATUS_OK)
			{
				int txn_count = 1;
				int status = STATUS_OK;

				pq_beginmessage(&buf, 'S');
				pq_sendint(&buf, SNAPSHOT_GET_RESULT, 4);
				pq_sendbytes(&buf, (char *)&cmdinfo->ci_data.cd_snap.gxid, sizeof (GlobalTransactionId));
				pq_sendbytes(&buf, (char *)&txn_count, sizeof (txn_count));
				pq_sendbytes(&buf, (char *)&status, sizeof (status));
				pq_sendbytes(&buf, (char *)&res->gr_snapshot.sn_xmin, sizeof (GlobalTransactionId));
				pq_sendbytes(&buf, (char *)&res->gr_snapshot.sn_xmax, sizeof (GlobalTransactionId));
				pq_sendbytes(&buf, (char *)&res->gr_snapshot.sn_recent_global_xmin, sizeof (GlobalTransactionId));
				pq_sendint(&buf, res->gr_snapshot.sn_xcnt, sizeof (int));
				pq_sendbytes(&buf, (char *)res->gr_snapshot.sn_xip,
							 sizeof(GlobalTransactionId) * res->gr_snapshot.sn_xcnt);
				pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
				pq_flush(cmdinfo->ci_conn->con_port);
			}
			else
				ereport(ERROR2, (EINVAL, errmsg("snapshot request failed")));
			cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
			break;

		case MSG_TXN_BEGIN:
		case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
		case MSG_TXN_PREPARE:
		case MSG_TXN_START_PREPARED:
		/* There are not so many 2PC from application messages, so just proxy it. */
		case MSG_TXN_COMMIT_PREPARED:
		case MSG_TXN_GET_GXID:
		case MSG_TXN_GET_GID_DATA:
		case MSG_SNAPSHOT_GXID_GET:
		case MSG_SEQUENCE_INIT:
		case MSG_SEQUENCE_GET_CURRENT:
		case MSG_SEQUENCE_GET_NEXT:
		case MSG_SEQUENCE_GET_LAST:
		case MSG_SEQUENCE_SET_VAL:
		case MSG_SEQUENCE_RESET:
		case MSG_SEQUENCE_CLOSE:
		case MSG_SEQUENCE_RENAME:
		case MSG_SEQUENCE_ALTER:
			if ((res->gr_proxyhdr.ph_conid == InvalidGTMProxyConnID) ||
				(res->gr_proxyhdr.ph_conid >= GTM_PROXY_MAX_CONNECTIONS) ||
				(thrinfo->thr_all_conns[res->gr_proxyhdr.ph_conid] != cmdinfo->ci_conn))
				elog(PANIC, "Invalid response or synchronization loss");

			/*
			 * These are just proxied messages.. so just forward the response
			 * back after stripping the conid part.
			 *
			 * !!TODO As we start adding support for message grouping for
			 * messages, those message types would be removed from the above
			 * and handled separately. 
			 */
			switch (res->gr_status)
			{
				case 0:
					pq_beginmessage(&buf, 'S');
					pq_sendint(&buf, res->gr_type, 4);
					pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
					pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
					pq_flush(cmdinfo->ci_conn->con_port);
					break;

				default:
					pq_beginmessage(&buf, 'E');
					pq_sendbytes(&buf, res->gr_proxy_data, res->gr_msglen);
					pq_endmessage(cmdinfo->ci_conn->con_port, &buf);
					pq_flush(cmdinfo->ci_conn->con_port);
					break;
			}
			cmdinfo->ci_conn->con_pending_msg = MSG_TYPE_INVALID;
			break;

		default:
			ereport(FATAL,
					(EPROTO,
					 errmsg("invalid frontend message type %d",
							cmdinfo->ci_mtype)));
	}
}

/* ----------------
 *		ReadCommand reads a command from either the frontend or
 *		standard input, places it in inBuf, and returns the
 *		message type code (first byte of the message).
 *		EOF is returned if end of file.
 * ----------------
 */
static int
ReadCommand(GTMProxy_ConnectionInfo *conninfo, StringInfo inBuf)
{
	int 			qtype;

	/*
	 * Get message type code from the frontend.
	 */
	qtype = pq_getbyte(conninfo->con_port);

	if (qtype == EOF)			/* frontend disconnected */
	{
		ereport(COMMERROR,
				(EPROTO,
				 errmsg("unexpected EOF on client connection")));
		return qtype;
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
	if (pq_getmessage(conninfo->con_port, inBuf, 0))
		return EOF;			/* suitable message already logged */

	return qtype;
}

static void
ProcessCoordinatorCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
		GTM_MessageType mtype, StringInfo message)
{
	GTM_CoordinatorId cid;

	cid = pq_getmsgint(message, sizeof (GTM_CoordinatorId));
	
	switch (mtype)
	{
		case MSG_UNREGISTER_COORD:
			GTMProxy_UnregisterCoordinator(conninfo, cid);
			break;

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}
	pq_getmsgend(message);
}

static void
ProcessTransactionCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
		GTM_MessageType mtype, StringInfo message)
{
	GTMProxy_CommandData cmd_data;

	switch (mtype)
	{
		case MSG_TXN_BEGIN_GETGXID:
			cmd_data.cd_beg.iso_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
			cmd_data.cd_beg.rdonly = pq_getmsgbyte(message);
			GTMProxy_CommandPending(conninfo, mtype, cmd_data);
			break;

		case MSG_TXN_COMMIT:
		case MSG_TXN_ROLLBACK:
			cmd_data.cd_rc.isgxid = pq_getmsgbyte(message);
			if (cmd_data.cd_rc.isgxid)
			{
				const char *data = pq_getmsgbytes(message,
						sizeof (GlobalTransactionId));
				if (data == NULL)
					ereport(ERROR,
							(EPROTO,
							 errmsg("Message does not contain valid GXID")));
				memcpy(&cmd_data.cd_rc.gxid, data, sizeof (GlobalTransactionId));
			}
			else
			{
				const char *data = pq_getmsgbytes(message,
						sizeof (GTM_TransactionHandle));
				if (data == NULL)
					ereport(ERROR,
							(EPROTO,
							 errmsg("Message does not contain valid Transaction Handle")));
				memcpy(&cmd_data.cd_rc.handle, data, sizeof (GTM_TransactionHandle));
			}
			pq_getmsgend(message);
			GTMProxy_CommandPending(conninfo, mtype, cmd_data);
			break;

		case MSG_TXN_BEGIN:	
		case MSG_TXN_GET_GXID:
			elog(FATAL, "Support not yet added for these message types");
			break;

		case MSG_TXN_BEGIN_GETGXID_AUTOVACUUM:
		case MSG_TXN_PREPARE:
		case MSG_TXN_START_PREPARED:
		case MSG_TXN_GET_GID_DATA:
		case MSG_TXN_COMMIT_PREPARED:
			GTMProxy_ProxyCommand(conninfo, gtm_conn, mtype, message);
			break;

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}
}

static void
ProcessSnapshotCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
		GTM_MessageType mtype, StringInfo message)
{
	bool canbe_grouped = false;
	GTMProxy_CommandData cmd_data;

	switch (mtype)
	{
		case MSG_SNAPSHOT_GET:
			canbe_grouped = pq_getmsgbyte(message);
			if (!canbe_grouped)
				GTMProxy_ProxyCommand(conninfo, gtm_conn, mtype, message);
			else
			{
				cmd_data.cd_snap.isgxid = pq_getmsgbyte(message);
				if (cmd_data.cd_snap.isgxid)
				{
					const char *data = pq_getmsgbytes(message,
							sizeof (GlobalTransactionId));
					if (data == NULL)
						ereport(ERROR,
								(EPROTO,
								 errmsg("Message does not contain valid GXID")));
					memcpy(&cmd_data.cd_snap.gxid, data, sizeof (GlobalTransactionId));
				}
				else
				{
					const char *data = pq_getmsgbytes(message,
							sizeof (GTM_TransactionHandle));
					if (data == NULL)
						ereport(ERROR,
								(EPROTO,
								 errmsg("Message does not contain valid Transaction Handle")));
					memcpy(&cmd_data.cd_snap.handle, data, sizeof (GTM_TransactionHandle));
				}
				pq_getmsgend(message);
				GTMProxy_CommandPending(conninfo, mtype, cmd_data);
			}
			break;

		case MSG_SNAPSHOT_GXID_GET:
			elog(ERROR, "Message not yet support");
			break;

		default:
			Assert(0);			/* Shouldn't come here.. keep compiler quite */
	}

}

static void
ProcessSequenceCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
		GTM_MessageType mtype, StringInfo message)
{
	/*
	 * We proxy the Sequence messages as they are. Just add the connection
	 * identifier to it so that the response can be quickly sent back to the
	 * right backend.
	 *
	 * Write the message, but don't flush it just yet.
	 */
	return GTMProxy_ProxyCommand(conninfo, gtm_conn, mtype, message);
}

/*
 * Proxy the incoming message to the GTM server after adding our own identifier
 * to it. The rest of the message is forwarded as it is without even reading
 * its contents.
 */
static void
GTMProxy_ProxyCommand(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn,
		GTM_MessageType mtype, StringInfo message)
{
	GTMProxy_CommandInfo *cmdinfo;
	GTMProxy_ThreadInfo *thrinfo = GetMyThreadInfo;
	GTM_ProxyMsgHeader proxyhdr;

	proxyhdr.ph_conid = conninfo->con_id;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, gtm_conn) ||
		gtmpqPutnchar((char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader), gtm_conn) ||
		gtmpqPutInt(mtype, sizeof (GTM_MessageType), gtm_conn) ||
		gtmpqPutnchar(pq_getmsgbytes(message, pq_getmsgunreadlen(message)),
					  pq_getmsgunreadlen(message), gtm_conn))
		elog(ERROR, "Error proxing data");

	/*
	 * Add the message to the pending command list
	 */
	cmdinfo = palloc0(sizeof (GTMProxy_CommandInfo));
	cmdinfo->ci_mtype = mtype;
	cmdinfo->ci_conn = conninfo;
	cmdinfo->ci_res_index = 0;
	thrinfo->thr_processed_commands = lappend(thrinfo->thr_processed_commands, cmdinfo);

	/* Finish the message. */
	if (gtmpqPutMsgEnd(gtm_conn))
		elog(ERROR, "Error finishing the message");

	return;
}


/*
 * Record the incoming message as per its type. After all messages of this type
 * are collected, they will be sent in a single message to the GTM server.
 */
static void
GTMProxy_CommandPending(GTMProxy_ConnectionInfo *conninfo, GTM_MessageType mtype,
		GTMProxy_CommandData cmd_data)
{
	GTMProxy_CommandInfo *cmdinfo;
	GTMProxy_ThreadInfo *thrinfo = GetMyThreadInfo;

	/*
	 * Add the message to the pending command list
	 */
	cmdinfo = palloc0(sizeof (GTMProxy_CommandInfo));
	cmdinfo->ci_mtype = mtype;
	cmdinfo->ci_conn = conninfo;
	cmdinfo->ci_res_index = 0;
	cmdinfo->ci_data = cmd_data;
	thrinfo->thr_pending_commands[mtype] = lappend(thrinfo->thr_pending_commands[mtype], cmdinfo);

	return;
}
static void
GTMProxy_RegisterCoordinator(GTMProxy_ConnectionInfo *conninfo, GTM_CoordinatorId cid)
{
	elog(DEBUG3, "Registering coordinator with cid %d", cid);
	conninfo->con_port->coordinator_id = cid;
}


static void
GTMProxy_UnregisterCoordinator(GTMProxy_ConnectionInfo *conninfo, GTM_CoordinatorId cid)
{
	/*
	 * Do a clean shutdown
	 */
	return;
}


static void
GTMProxy_HandshakeConnection(GTMProxy_ConnectionInfo *conninfo)
{
	/*
	 * We expect a startup message at the very start. The message type is
	 * REGISTER_COORD, followed by the 4 byte coordinator ID
	 */
	char startup_type;
	GTM_StartupPacket sp;
	StringInfoData inBuf;
	StringInfoData buf;

	startup_type = pq_getbyte(conninfo->con_port);

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
	if (pq_getmessage(conninfo->con_port, &inBuf, 0))
		ereport(ERROR,
				(EPROTO,
				 errmsg("Expecting coordinator ID, but received EOF")));

	memcpy(&sp,
		   pq_getmsgbytes(&inBuf, sizeof (GTM_StartupPacket)),
		   sizeof (GTM_StartupPacket));
	pq_getmsgend(&inBuf);

	GTMProxy_RegisterCoordinator(conninfo, sp.sp_cid);

	/*
	 * Send a dummy authentication request message 'R' as the client
	 * expects that in the current protocol
	 */
	pq_beginmessage(&buf, 'R');
	pq_endmessage(conninfo->con_port, &buf);
	pq_flush(conninfo->con_port);

	conninfo->con_authenticated = true;

	elog(DEBUG3, "Sent connection authentication message to the client");
}

static void
GTMProxy_HandleDisconnect(GTMProxy_ConnectionInfo *conninfo, GTM_Conn *gtm_conn)
{
	GTM_ProxyMsgHeader proxyhdr;

	conninfo->con_disconnected = true;
	if (conninfo->con_port->sock > 0)
		StreamClose(conninfo->con_port->sock);
	ConnFree(conninfo->con_port);
	conninfo->con_port = NULL;

	proxyhdr.ph_conid = conninfo->con_id;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, gtm_conn) ||
		gtmpqPutnchar((char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader), gtm_conn) ||
		gtmpqPutInt(MSG_BACKEND_DISCONNECT, sizeof (GTM_MessageType), gtm_conn))
		elog(ERROR, "Error proxing data");

	/* Finish the message. */
	if (gtmpqPutMsgEnd(gtm_conn))
		elog(ERROR, "Error finishing the message");

	return;
}

/*
 * Proces all the pending messages now.
 */
static void
GTMProxy_ProcessPendingCommands(GTMProxy_ThreadInfo *thrinfo)
{
	int ii;
	GTMProxy_CommandInfo *cmdinfo = NULL;
	GTM_ProxyMsgHeader proxyhdr;
	GTM_Conn *gtm_conn = thrinfo->thr_gtm_conn;
	ListCell *elem = NULL;

	for (ii = 0; ii < MSG_TYPE_COUNT; ii++)
	{
		int res_index = 0;

		if (list_length(thrinfo->thr_pending_commands[ii]) == 0)
			continue;

		/*
		 * Start a new group message and fill in the headers
		 */
		proxyhdr.ph_conid = InvalidGTMProxyConnID;

		if (gtmpqPutMsgStart('C', true, gtm_conn) ||
			gtmpqPutnchar((char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader), gtm_conn))
			elog(ERROR, "Error proxing data");

		switch (ii)
		{
			case MSG_TXN_BEGIN_GETGXID:
				if (list_length(thrinfo->thr_pending_commands[ii]) <=0 )
					elog(PANIC, "No pending commands of type %d", ii);

				if (gtmpqPutInt(MSG_TXN_BEGIN_GETGXID_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
					gtmpqPutInt(list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
					elog(ERROR, "Error sending data");
				foreach (elem, thrinfo->thr_pending_commands[ii])
				{
					cmdinfo = (GTMProxy_CommandInfo *)lfirst(elem);
					Assert(cmdinfo->ci_mtype == ii);
					cmdinfo->ci_res_index = res_index++;
					if (gtmpqPutInt(cmdinfo->ci_data.cd_beg.iso_level,
								sizeof (GTM_IsolationLevel), gtm_conn) ||
						gtmpqPutc(cmdinfo->ci_data.cd_beg.rdonly, gtm_conn) ||
						gtmpqPutInt(cmdinfo->ci_conn->con_id, sizeof (GTMProxy_ConnID), gtm_conn))
						elog(ERROR, "Error sending data");

				}

				/* Finish the message. */
				if (gtmpqPutMsgEnd(gtm_conn))
					elog(ERROR, "Error finishing the message");

				/*
				 * Move the entire list to the processed command
				 */
				thrinfo->thr_processed_commands = list_concat(thrinfo->thr_processed_commands,
						thrinfo->thr_pending_commands[ii]);
				thrinfo->thr_pending_commands[ii] = NIL;
				break;

			case MSG_TXN_COMMIT:
				if (gtmpqPutInt(MSG_TXN_COMMIT_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
					gtmpqPutInt(list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
					elog(ERROR, "Error sending data");

				foreach (elem, thrinfo->thr_pending_commands[ii])
				{
					cmdinfo = (GTMProxy_CommandInfo *)lfirst(elem);
					Assert(cmdinfo->ci_mtype == ii);
					cmdinfo->ci_res_index = res_index++;
					if (cmdinfo->ci_data.cd_rc.isgxid)
					{
						if (gtmpqPutc(true, gtm_conn) ||
							gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.gxid,
								sizeof (GlobalTransactionId), gtm_conn))
							elog(ERROR, "Error sending data");
					}
					else
					{
						if (gtmpqPutc(false, gtm_conn) ||
							gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.handle,
								sizeof (GTM_TransactionHandle), gtm_conn))
							elog(ERROR, "Error sending data");
					}
				}

				/* Finish the message. */
				if (gtmpqPutMsgEnd(gtm_conn))
					elog(ERROR, "Error finishing the message");

				/*
				 * Move the entire list to the processed command
				 */
				thrinfo->thr_processed_commands = list_concat(thrinfo->thr_processed_commands,
						thrinfo->thr_pending_commands[ii]);
				thrinfo->thr_pending_commands[ii] = NIL;
				break;

				break;

			case MSG_TXN_ROLLBACK:
				if (gtmpqPutInt(MSG_TXN_ROLLBACK_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
					gtmpqPutInt(list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
					elog(ERROR, "Error sending data");

				foreach (elem, thrinfo->thr_pending_commands[ii])
				{
					cmdinfo = (GTMProxy_CommandInfo *)lfirst(elem);
					Assert(cmdinfo->ci_mtype == ii);
					cmdinfo->ci_res_index = res_index++;
					if (cmdinfo->ci_data.cd_rc.isgxid)
					{
						if (gtmpqPutc(true, gtm_conn) ||
							gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.gxid,
								sizeof (GlobalTransactionId), gtm_conn))
							elog(ERROR, "Error sending data");
					}
					else
					{
						if (gtmpqPutc(false, gtm_conn) ||
							gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.handle,
								sizeof (GTM_TransactionHandle), gtm_conn))
							elog(ERROR, "Error sending data");
					}
				}

				/* Finish the message. */
				if (gtmpqPutMsgEnd(gtm_conn))
					elog(ERROR, "Error finishing the message");


				/*
				 * Move the entire list to the processed command
				 */
				thrinfo->thr_processed_commands = list_concat(thrinfo->thr_processed_commands,
						thrinfo->thr_pending_commands[ii]);
				thrinfo->thr_pending_commands[ii] = NIL;
				break;

			case MSG_SNAPSHOT_GET:
				if (gtmpqPutInt(MSG_SNAPSHOT_GET_MULTI, sizeof (GTM_MessageType), gtm_conn) ||
					gtmpqPutInt(list_length(thrinfo->thr_pending_commands[ii]), sizeof(int), gtm_conn))
					elog(ERROR, "Error sending data");

				foreach (elem, thrinfo->thr_pending_commands[ii])
				{
					cmdinfo = (GTMProxy_CommandInfo *)lfirst(elem);
					Assert(cmdinfo->ci_mtype == ii);
					cmdinfo->ci_res_index = res_index++;
					if (cmdinfo->ci_data.cd_rc.isgxid)
					{
						if (gtmpqPutc(true, gtm_conn) ||
							gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.gxid,
								sizeof (GlobalTransactionId), gtm_conn))
							elog(ERROR, "Error sending data");
					}
					else
					{
						if (gtmpqPutc(false, gtm_conn) ||
							gtmpqPutnchar((char *)&cmdinfo->ci_data.cd_rc.handle,
								sizeof (GTM_TransactionHandle), gtm_conn))
							elog(ERROR, "Error sending data");
					}
				}

				/* Finish the message. */
				if (gtmpqPutMsgEnd(gtm_conn))
					elog(ERROR, "Error finishing the message");

				/*
				 * Move the entire list to the processed command
				 */
				thrinfo->thr_processed_commands = list_concat(thrinfo->thr_processed_commands,
						thrinfo->thr_pending_commands[ii]);
				thrinfo->thr_pending_commands[ii] = NIL;
				break;


			default:
				elog(ERROR, "This message type (%d) can not be grouped together", ii);
		}

	}
}

/*
 * Validate the proposed data directory
 */
static void
checkDataDir(void)
{
	struct stat stat_buf;

	Assert(GTMProxyDataDir);

retry:
	if (stat(GTMProxyDataDir, &stat_buf) != 0)
	{
		if (errno == ENOENT)
		{
			if (mkdir(GTMProxyDataDir, 0700) != 0)
			{
				ereport(FATAL,
						(errno,
						 errmsg("failed to create the directory \"%s\"",
							 GTMProxyDataDir)));
			}
			goto retry;
		}
		else
			ereport(FATAL,
					(EPERM,
				 errmsg("could not read permissions of directory \"%s\": %m",
						GTMProxyDataDir)));
	}

	/* eventual chdir would fail anyway, but let's test ... */
	if (!S_ISDIR(stat_buf.st_mode))
		ereport(FATAL,
				(EINVAL,
				 errmsg("specified data directory \"%s\" is not a directory",
						GTMProxyDataDir)));

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
						GTMProxyDataDir),
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
	if (chdir(GTMProxyDataDir) < 0)
		ereport(FATAL,
				(EINVAL,
				 errmsg("could not change directory to \"%s\": %m",
						GTMProxyDataDir)));
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
	CreateLockFile(GTM_PID_FILE, GTMProxyDataDir);
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
						  errhint("Is another GTM proxy (PID %d) running in data directory \"%s\"?",
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
			 (int) my_pid, GTMProxyDataDir);
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

#define OPTS_FILE	"gtm_proxy.opts"

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
