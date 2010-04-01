/*-------------------------------------------------------------------------
 *
 * gtm_ctl --- start/stops/restarts the GTM server/proxy
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010 Nippon Telegraph and Telephone Corporation
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"

#include <locale.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include "libpq/pqsignal.h"

/* PID can be negative for standalone backend */
typedef long pgpid_t;

typedef enum
{
	SMART_MODE,
	FAST_MODE,
	IMMEDIATE_MODE
} ShutdownMode;


typedef enum
{
	NO_COMMAND = 0,
	START_COMMAND,
	STOP_COMMAND,
	RESTART_COMMAND,
} CtlCommand;

#define DEFAULT_WAIT	60

static bool do_wait = false;
static bool wait_set = false;
static int	wait_seconds = DEFAULT_WAIT;
static bool silent_mode = false;
static ShutdownMode shutdown_mode = SMART_MODE;
static int	sig = SIGTERM;		/* default */
static CtlCommand ctl_command = NO_COMMAND;
static char *gtm_data = NULL;
static char *gtmdata_opt = NULL;
static char *gtm_opts = NULL;
static const char *progname;
static char *log_file = NULL;
static char *gtm_path = NULL;
static char *gtm_app = NULL;
static char *argv0 = NULL;

static void
write_stderr(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));
static void *pg_malloc(size_t size);
static char *xstrdup(const char *s);
static void do_advice(void);
static void do_help(void);
static void set_mode(char *modeopt);
static void do_start(void);
static void do_stop(void);
static void do_restart(void);
static void print_msg(const char *msg);

static pgpid_t get_pgpid(void);
static char **readfile(const char *path);
static int	start_gtm(void);
static void read_gtm_opts(void);

static bool test_gtm_connection();
static bool gtm_is_alive(pid_t pid);

static char gtmopts_file[MAXPGPATH];
static char pid_file[MAXPGPATH];

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available).
 */
static void
write_stderr(const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);

	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, fmt, ap);
	va_end(ap);
}

/*
 * routines to check memory allocations and fail noisily.
 */

static void *
pg_malloc(size_t size)
{
	void	   *result;

	result = malloc(size);
	if (!result)
	{
		write_stderr(_("%s: out of memory\n"), progname);
		exit(1);
	}
	return result;
}


static char *
xstrdup(const char *s)
{
	char	   *result;

	result = strdup(s);
	if (!result)
	{
		write_stderr(_("%s: out of memory\n"), progname);
		exit(1);
	}
	return result;
}

/*
 * Given an already-localized string, print it to stdout unless the
 * user has specified that no messages should be printed.
 */
static void
print_msg(const char *msg)
{
	if (!silent_mode)
	{
		fputs(msg, stdout);
		fflush(stdout);
	}
}

static pgpid_t
get_pgpid(void)
{
	FILE	   *pidf;
	long		pid;

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		/* No pid file, not an error on startup */
		if (errno == ENOENT)
			return 0;
		else
		{
			write_stderr(_("%s: could not open PID file \"%s\": %s\n"),
						 progname, pid_file, strerror(errno));
			exit(1);
		}
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		write_stderr(_("%s: invalid data in PID file \"%s\"\n"),
					 progname, pid_file);
		exit(1);
	}
	fclose(pidf);
	return (pgpid_t) pid;
}


/*
 * get the lines from a text file - return NULL if file can't be opened
 */
static char **
readfile(const char *path)
{
	FILE	   *infile;
	int			maxlength = 0,
				linelen = 0;
	int			nlines = 0;
	char	  **result;
	char	   *buffer;
	int			c;

	if ((infile = fopen(path, "r")) == NULL)
		return NULL;

	/* pass over the file twice - the first time to size the result */

	while ((c = fgetc(infile)) != EOF)
	{
		linelen++;
		if (c == '\n')
		{
			nlines++;
			if (linelen > maxlength)
				maxlength = linelen;
			linelen = 0;
		}
	}

	/* handle last line without a terminating newline (yuck) */
	if (linelen)
		nlines++;
	if (linelen > maxlength)
		maxlength = linelen;

	/* set up the result and the line buffer */
	result = (char **) pg_malloc((nlines + 1) * sizeof(char *));
	buffer = (char *) pg_malloc(maxlength + 1);

	/* now reprocess the file and store the lines */
	rewind(infile);
	nlines = 0;
	while (fgets(buffer, maxlength + 1, infile) != NULL)
		result[nlines++] = xstrdup(buffer);

	fclose(infile);
	free(buffer);
	result[nlines] = NULL;

	return result;
}



/*
 * start/test/stop routines
 */

static int
start_gtm(void)
{
	char		cmd[MAXPGPATH];
	/*
	 * Since there might be quotes to handle here, it is easier simply to pass
	 * everything to a shell to process them.
	 */

	if (gtm_path != NULL)
	{
		strcat(gtm_path, "/");
		strcat(gtm_path, gtm_app);
	}
	else
		gtm_path = gtm_app;

	if (log_file != NULL)
		snprintf(cmd, MAXPGPATH, SYSTEMQUOTE "\"%s\" %s%s < \"%s\" >> \"%s\" 2>&1 &" SYSTEMQUOTE,
				 gtm_path, gtmdata_opt, gtm_opts,
				 DEVNULL, log_file);
	else
		snprintf(cmd, MAXPGPATH, SYSTEMQUOTE "\"%s\" %s%s < \"%s\" 2>&1 &" SYSTEMQUOTE,
				 gtm_path, gtmdata_opt, gtm_opts, DEVNULL);

	return system(cmd);
}



/*
 * Find the pgport and try a connection
 */
static bool
test_gtm_connection()
{
	GTM_Conn	   *conn;
	bool		success = false;
	int			i;
	char		portstr[32];
	char	   *p;
	char	   *q;
	char		connstr[128];	/* Should be way more than enough! */

	*portstr = '\0';

	/*
	 * Look in gtm_opts for a -p switch.
	 *
	 * This parsing code is not amazingly bright; it could for instance
	 * get fooled if ' -p' occurs within a quoted argument value.  Given
	 * that few people pass complicated settings in gtm_opts, it's
	 * probably good enough.
	 */
	for (p = gtm_opts; *p;)
	{
		/* advance past whitespace */
		while (isspace((unsigned char) *p))
			p++;

		if (strncmp(p, "-p", 2) == 0)
		{
			p += 2;
			/* advance past any whitespace/quoting */
			while (isspace((unsigned char) *p) || *p == '\'' || *p == '"')
				p++;
			/* find end of value (not including any ending quote!) */
			q = p;
			while (*q &&
				   !(isspace((unsigned char) *q) || *q == '\'' || *q == '"'))
				q++;
			/* and save the argument value */
			strlcpy(portstr, p, Min((q - p) + 1, sizeof(portstr)));
			/* keep looking, maybe there is another -p */
			p = q;
		}
		/* Advance to next whitespace */
		while (*p && !isspace((unsigned char) *p))
			p++;
	}

	/*
	 * We need to set a connect timeout otherwise on Windows the SCM will
	 * probably timeout first
	 */
	snprintf(connstr, sizeof(connstr),
			 "host=localhost port=%s connect_timeout=5", portstr);

	for (i = 0; i < wait_seconds; i++)
	{
		if ((conn = PQconnectGTM(connstr)) != NULL &&
			(GTMPQstatus(conn) == CONNECTION_OK))
		{
			GTMPQfinish(conn);
			success = true;
			break;
		}
		else
		{
			GTMPQfinish(conn);
			print_msg(".");
			sleep(1); /* 1 sec */
		}
	}

	return success;
}

static void
read_gtm_opts(void)
{
	if (gtm_opts == NULL)
	{
		gtm_opts = "";		/* default */
		if (ctl_command == RESTART_COMMAND)
		{
			char	  **optlines;

			optlines = readfile(gtmopts_file);
			if (optlines == NULL)
			{
				write_stderr(_("%s: could not read file \"%s\"\n"), progname, gtmopts_file);
				exit(1);
			}
			else if (optlines[0] == NULL || optlines[1] != NULL)
			{
				write_stderr(_("%s: option file \"%s\" must have exactly one line\n"),
							 progname, gtmopts_file);
				exit(1);
			}
			else
			{
				int			len;
				char	   *optline;
				char	   *arg1;

				optline = optlines[0];
				/* trim off line endings */
				len = strcspn(optline, "\r\n");
				optline[len] = '\0';

				gtm_opts = arg1;
			}
		}
	}
}

static void
do_start(void)
{
	pgpid_t		pid;
	pgpid_t		old_pid = 0;
	int			exitcode;

	if (ctl_command != RESTART_COMMAND)
	{
		old_pid = get_pgpid();
		if (old_pid != 0)
			write_stderr(_("%s: another server might be running; "
						   "trying to start server anyway\n"),
						 progname);
	}

	read_gtm_opts();

	exitcode = start_gtm();
	if (exitcode != 0)
	{
		write_stderr(_("%s: could not start server: exit code was %d\n"),
					 progname, exitcode);
		exit(1);
	}

	if (old_pid != 0)
	{
		sleep(1);
		pid = get_pgpid();
		if (pid == old_pid)
		{
			write_stderr(_("%s: could not start server\n"
						   "Examine the log output.\n"),
						 progname);
			exit(1);
		}
	}

	if (do_wait)
	{
		print_msg(_("waiting for server to start..."));

		if (test_gtm_connection() == false)
		{
			printf(_("could not start server\n"));
			exit(1);
		}
		else
		{
			print_msg(_(" done\n"));
			print_msg(_("server started\n"));
		}
	}
	else
		print_msg(_("server starting\n"));
}


static void
do_stop(void)
{
	int			cnt;
	pgpid_t		pid;

	pid = get_pgpid();

	if (pid == 0)				/* no pid file */
	{
		write_stderr(_("%s: PID file \"%s\" does not exist\n"), progname, pid_file);
		write_stderr(_("Is server running?\n"));
		exit(1);
	}
	else if (pid < 0)			/* standalone backend, not gtm */
	{
		pid = -pid;
		write_stderr(_("%s: cannot stop server; "
					   "single-user server is running (PID: %ld)\n"),
					 progname, pid);
		exit(1);
	}

	if (kill((pid_t) pid, sig) != 0)
	{
		write_stderr(_("%s: could not send stop signal (PID: %ld): %s\n"), progname, pid,
					 strerror(errno));
		exit(1);
	}

	if (!do_wait)
	{
		print_msg(_("server shutting down\n"));
		return;
	}
	else
	{
		print_msg(_("waiting for server to shut down..."));

		for (cnt = 0; cnt < wait_seconds; cnt++)
		{
			if ((pid = get_pgpid()) != 0)
			{
				print_msg(".");
				sleep(1);		/* 1 sec */
			}
			else
				break;
		}

		if (pid != 0)			/* pid file still exists */
		{
			print_msg(_(" failed\n"));

			write_stderr(_("%s: server does not shut down\n"), progname);
			exit(1);
		}
		print_msg(_(" done\n"));

		printf(_("server stopped\n"));
	}
}


/*
 *	restart/reload routines
 */

static void
do_restart(void)
{
	int			cnt;
	pgpid_t		pid;

	pid = get_pgpid();

	if (pid == 0)				/* no pid file */
	{
		write_stderr(_("%s: PID file \"%s\" does not exist\n"),
					 progname, pid_file);
		write_stderr(_("Is server running?\n"));
		write_stderr(_("starting server anyway\n"));
		do_start();
		return;
	}
	else if (pid < 0)			/* standalone backend, not gtm */
	{
		pid = -pid;
		if (gtm_is_alive((pid_t) pid))
		{
			write_stderr(_("%s: cannot restart server; "
						   "single-user server is running (PID: %ld)\n"),
						 progname, pid);
			write_stderr(_("Please terminate the single-user server and try again.\n"));
			exit(1);
		}
	}

	if (gtm_is_alive((pid_t) pid))
	{
		if (kill((pid_t) pid, sig) != 0)
		{
			write_stderr(_("%s: could not send stop signal (PID: %ld): %s\n"), progname, pid,
						 strerror(errno));
			exit(1);
		}

		print_msg(_("waiting for server to shut down..."));

		/* always wait for restart */

		for (cnt = 0; cnt < wait_seconds; cnt++)
		{
			if ((pid = get_pgpid()) != 0)
			{
				print_msg(".");
				sleep(1);		/* 1 sec */
			}
			else
				break;
		}

		if (pid != 0)			/* pid file still exists */
		{
			print_msg(_(" failed\n"));

			write_stderr(_("%s: server does not shut down\n"), progname);
			exit(1);
		}

		print_msg(_(" done\n"));
		printf(_("server stopped\n"));
	}
	else
	{
		write_stderr(_("%s: old server process (PID: %ld) seems to be gone\n"),
					 progname, pid);
		write_stderr(_("starting server anyway\n"));
	}

	do_start();
}


/*
 *	utility routines
 */

static bool
gtm_is_alive(pid_t pid)
{
	/*
	 * Test to see if the process is still there.  Note that we do not
	 * consider an EPERM failure to mean that the process is still there;
	 * EPERM must mean that the given PID belongs to some other userid, and
	 * considering the permissions on $GTMDATA, that means it's not the
	 * gtm we are after.
	 *
	 * Don't believe that our own PID or parent shell's PID is the gtm,
	 * either.	(Windows hasn't got getppid(), though.)
	 */
	if (pid == getpid())
		return false;
#ifndef WIN32
	if (pid == getppid())
		return false;
#endif
	if (kill(pid, 0) == 0)
		return true;
	return false;
}

static void
do_advice(void)
{
	write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
}


static void
do_help(void)
{
	printf(_("%s is a utility to start, stop or restart,\n"
			 "a GTM server or GTM proxy.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s start   -S STARTUP_MODE [-w] [-t SECS] [-D DATADIR] [-s] [-l FILENAME] [-o \"OPTIONS\"]\n"), progname);
	printf(_("  %s stop    -S STARTUP_MODE [-W] [-t SECS] [-D DATADIR] [-s] [-m SHUTDOWN-MODE]\n"), progname);
	printf(_("  %s restart -S STARTUP_MODE [-w] [-t SECS] [-D DATADIR] [-s] [-m SHUTDOWN-MODE]\n"
		 "                 [-o \"OPTIONS\"]\n"), progname);

	printf(_("\nCommon options:\n"));
	printf(_("  -D DATADIR             location of the database storage area\n"));
	printf(_("  -S                     set gtm or gtm_proxy to launch one of them\n"));
	printf(_("  -s, 				   only print errors, no informational messages\n"));
	printf(_("  -t SECS                seconds to wait when using -w option\n"));
	printf(_("  -w                     wait until operation completes\n"));
	printf(_("  -W                     do not wait until operation completes\n"));
	printf(_("  --help                 show this help, then exit\n"));
	printf(_("(The default is to wait for shutdown, but not for start or restart.)\n\n"));

	printf(_("\nOptions for start or restart:\n"));
	printf(_("  -S STARTUP-MODE        can be \"gtm\" or \"gtm_proxy\"\n"));
	printf(_("  -l FILENAME            write (or append) server log to FILENAME\n"));
	printf(_("  -o OPTIONS             command line options to pass to gtm\n"
			 "                         (GTM server executable)\n"));
	printf(_("  -p PATH-TO-GTM/PROXY   path to gtm/gtm_proxy executables\n"));
	printf(_("\nOptions for stop or restart:\n"));
	printf(_("  -m SHUTDOWN-MODE   can be \"smart\", \"fast\", or \"immediate\"\n"));

	printf(_("\nShutdown modes are:\n"));
	printf(_("  smart       quit after all clients have disconnected\n"));
	printf(_("  fast        quit directly, with proper shutdown\n"));
	printf(_("  immediate   quit without complete shutdown; will lead to recovery on restart\n"));
}


static void
set_mode(char *modeopt)
{
	if (strcmp(modeopt, "s") == 0 || strcmp(modeopt, "smart") == 0)
	{
		shutdown_mode = SMART_MODE;
		sig = SIGTERM;
	}
	else if (strcmp(modeopt, "f") == 0 || strcmp(modeopt, "fast") == 0)
	{
		shutdown_mode = FAST_MODE;
		sig = SIGINT;
	}
	else if (strcmp(modeopt, "i") == 0 || strcmp(modeopt, "immediate") == 0)
	{
		shutdown_mode = IMMEDIATE_MODE;
		sig = SIGQUIT;
	}
	else
	{
		write_stderr(_("%s: unrecognized shutdown mode \"%s\"\n"), progname, modeopt);
		do_advice();
		exit(1);
	}
}

int
main(int argc, char **argv)
{
	int			c;

	progname = "gtm_ctl";

	/*
	 * save argv[0] so do_start() can look for the gtm if necessary. we
	 * don't look for gtm here because in many cases we won't need it.
	 */
	argv0 = argv[0];

	umask(077);

	/* support --help and --version even if invoked as root */
	if (argc > 1)
	{
		if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0 ||
			strcmp(argv[1], "-?") == 0)
		{
			do_help();
			exit(0);
		}
	}

	/*
	 * Disallow running as root, to forestall any possible security holes.
	 */
	if (geteuid() == 0)
	{
		write_stderr(_("%s: cannot be run as root\n"
					   "Please log in (using, e.g., \"su\") as the "
					   "(unprivileged) user that will\n"
					   "own the server process.\n"),
					 progname);
		exit(1);
	}

	/*
	 * 'Action' can be before or after args so loop over both. Some
	 * getopt_long() implementations will reorder argv[] to place all flags
	 * first (GNU?), but we don't rely on it. Our /port version doesn't do
	 * that.
	 */
	optind = 1;

	/* process command-line options */
	while (optind < argc)
	{
		while ((c = getopt(argc, argv, "D:l:m:o:p:S:t:wW")) != -1)
		{
			switch (c)
			{
				case 'D':
					{
						char	   *gtmdata_D;
						char	   *env_var = pg_malloc(strlen(optarg) + 9);

						gtmdata_D = xstrdup(optarg);
						canonicalize_path(gtmdata_D);
						snprintf(env_var, strlen(optarg) + 9, "GTMDATA=%s",
								 gtmdata_D);
						putenv(env_var);

						/*
						 * We could pass GTMDATA just in an environment
						 * variable but we do -D too for clearer gtm
						 * 'ps' display
						 */
						gtmdata_opt = pg_malloc(strlen(gtmdata_D) + 8);
						snprintf(gtmdata_opt, strlen(gtmdata_D) + 8,
								 "-D \"%s\" ",
								 gtmdata_D);
						break;
					}
				case 'l':
					log_file = xstrdup(optarg);
					break;
				case 'm':
					set_mode(optarg);
					break;
				case 'o':
					gtm_opts = xstrdup(optarg);
					break;
				case 'p':
					gtm_path = xstrdup(optarg);
					canonicalize_path(gtm_path);
					break;
				case 'S':
					gtm_app = xstrdup(optarg);
					if (strcmp(gtm_app,"gtm_proxy") != 0
						&& strcmp(gtm_app,"gtm") != 0)
					{
						write_stderr(_("%s: %s launch name set not correct\n"), progname, gtm_app);
						do_advice();
						exit(1);
					}
					break;
				case 't':
					wait_seconds = atoi(optarg);
					break;
				case 'w':
					do_wait = true;
					wait_set = true;
					break;
				case 'W':
					do_wait = false;
					wait_set = true;
					break;
				default:
					/* getopt_long already issued a suitable error message */
					do_advice();
					exit(1);
			}
		}

		/* Process an action */
		if (optind < argc)
		{
			if (ctl_command != NO_COMMAND)
			{
				write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
				do_advice();
				exit(1);
			}

			if (strcmp(argv[optind], "start") == 0)
				ctl_command = START_COMMAND;
			else if (strcmp(argv[optind], "stop") == 0)
				ctl_command = STOP_COMMAND;
			else if (strcmp(argv[optind], "restart") == 0)
				ctl_command = RESTART_COMMAND;
			else
			{
				write_stderr(_("%s: unrecognized operation mode \"%s\"\n"), progname, argv[optind]);
				do_advice();
				exit(1);
			}
			optind++;
		}
	}

	if (ctl_command == NO_COMMAND)
	{
		write_stderr(_("%s: no operation specified\n"), progname);
		do_advice();
		exit(1);
	}

	gtm_data = getenv("GTMDATA");

	if (gtm_data)
	{
		gtm_data = xstrdup(gtm_data);
		canonicalize_path(gtm_data);
	}

	if (!gtm_data)
	{
		write_stderr("%s: no database directory specified \n",
					 progname);
		do_advice();
		exit(1);
	}

	/*
	 * pid files of gtm and gtm proxy are named differently
	 * -S option has also to be set for STOP_COMMAND
	 * or gtm_ctl will not be able to find the correct pid_file
	 */
	if (!gtm_app)
	{
		write_stderr("%s: launcher name non specified, see option -S\n",
					 progname);
		do_advice();
		exit(1);
	}

	if (!wait_set)
	{
		switch (ctl_command)
		{
			case RESTART_COMMAND:
			case START_COMMAND:
				do_wait = false;
				break;
			case STOP_COMMAND:
				do_wait = true;
				break;
			default:
				break;
		}
	}

	if (gtm_data)
	{
		if (strcmp(gtm_app,"gtm_proxy") == 0)
		{
			snprintf(pid_file, MAXPGPATH, "%s/gtm_proxy.pid", gtm_data);
			snprintf(gtmopts_file, MAXPGPATH, "%s/gtm_proxy.opts", gtm_data);
		}
		else if (strcmp(gtm_app,"gtm") == 0)
		{
			snprintf(pid_file, MAXPGPATH, "%s/gtm.pid", gtm_data);
			snprintf(gtmopts_file, MAXPGPATH, "%s/gtm.opts", gtm_data);
		}
	}

	switch (ctl_command)
	{
		case START_COMMAND:
			do_start();
			break;
		case STOP_COMMAND:
			do_stop();
			break;
		case RESTART_COMMAND:
			do_restart();
			break;
		default:
			break;
	}

	exit(0);
}
