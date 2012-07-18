/*
 * -----------------------------------------------------------------------------
 *
 * pgxc_monitor utility
 *
 *  Monitors if a given node is running or not.
 *
 * Command syntax:
 *
 * pgxc_monitor -Z nodetype -p port -h host
 *
 * Options are:
 * -Z nodetype		What node type to monitor, gtm or node.
 *					gtm tests gtm, gtm_standby or gtm_proxy.
 *					node tests coordinator or datanode.
 * -p port			Port number of the monitored node.
 * -h host			Host name or IP address of the monitored node.
 * -n nodename      Specifies pgxc_monitor node name.   Default is "pgxc_monitor"
 * -q				Run in quiet mode.  Default is quiet mode.
 * -v				Run in verbose mdoe.
 * --help			Prints the help message and exit with 0.
 *
 * When monitoring coordinator or datanode, -p and -h options can be
 * supplied via .pgpass file.   If you use non-default target database name
 * and username, they must also be supplied by .pgpass file.
 * If password is needed, it must also be supplied by .pgpass file.
 *
 * Monitoring coordinaor and datanode uses system(3) function.  Therefore,
 * you should not use set-userid bit or set-groupid bit.   Also, because
 * this uses psql command, psql must be in your PATH.
 *
 * When testing coordinator/datanode, you must setup .pgpass file if you
 * need to supply password, as well as non-default database name and username.
 *
 * If invalid parameters are given, error message will be printed even if
 * -q is specified.
 *
 * --------------------------------------------------------------------------
 */


#include "gtm/gtm_client.h"
#include "gtm/libpq-fe.h"

#include <stdlib.h>
#include <getopt.h>

/*
 * GTM_Proxy and Datanode are for future extension.
 */
typedef enum
{
	None = 0,
	GTM,
	GTM_Proxy,
	Coordinator,
	Datanode
} nodetype_t;

static char 	*progname;
static nodetype_t nodetype = None;
static char		*port = NULL;
static char		*host = NULL;
static char		*nodename = NULL;
static int 		verbose = 0;

#define Free(x) do{if((x)) free((x)); x = NULL;} while(0)

static void usage(void);
static int do_gtm_ping(char *host, char *node, nodetype_t nodetype);
static int do_node_ping(char *host, char *node);


int main (int ac, char *av[])
{
	int opt;

	progname = strdup(av[0]);

	while ((opt = getopt(ac, av, "Z:h:n:p:qv")) != -1)
	{
		switch(opt)
		{
			case 'Z':
				if (strcmp(optarg, "gtm") == 0)
					nodetype = GTM;
				else if (strcmp(optarg, "node") == 0)
					nodetype = Coordinator;
				else
				{
					fprintf(stderr, "%s: invalid -Z option value.\n", progname);
					exit(3);
				}
				break;
			case 'h':
				Free(host);
				host = strdup(optarg);
				break;
			case 'n':
				Free(nodename);
				nodename = strdup(optarg);
				break;
			case 'p':
				Free(port);
				port = strdup(optarg);
				break;
			case 'q':
				verbose = 0;
				break;
			case 'v':
				verbose = 1;
				break;
			default:
				fprintf(stderr, "%s: unknow option %c.\n", progname, opt);
				exit(3);
		}
	}
	if (nodetype == None)
	{
		fprintf(stderr, "%s: -Z option is missing, it is mandatory.\n", progname);
		usage();
		exit(3);
	}
	switch(nodetype)
	{
		case GTM:
		case GTM_Proxy:
			exit(do_gtm_ping(host, port, nodetype));
		case Coordinator:
		case Datanode:
			exit(do_node_ping(host, port));
		case None:
		default:
			break;
	}
	fprintf(stderr, "%s: inernal error.\n", progname);
	exit(3);
}

static int do_gtm_ping(char *host, char* port, nodetype_t nodetype)
{
	char connect_str[256];
	GTM_Conn *conn;
	
	if (host == NULL)
	{
		fprintf(stderr, "%s: -h is mandatory for -Z gtm or -Z gtm_proxy\n", progname);
		exit(3);
	}
	if (port == NULL)
	{
		fprintf(stderr, "%s: -p is mandatory for -Z gtm or -Z gtm_proxy\n", progname);
		exit(3);
	}
	sprintf(connect_str, "host=%s port=%s node_name=%s remote_type=%d postmaster=0", 
			host, port, nodename ? nodename : "pgxc_monitor", GTM_NODE_COORDINATOR);
	if ((conn = PQconnectGTM(connect_str)) == NULL)
	{
		if (verbose)
			fprintf(stderr, "%s: Could not connect to %s\n", progname, nodetype == GTM ? "GTM" : "GTM_Proxy");
		exit(1);
	}
	GTMPQfinish(conn);
	if (verbose)
		printf("Running\n");
	return(0);
}

static int do_node_ping(char *host, char *node)
{
	int rc;
	int exitStatus;
	char command_line[512];
	char *quiet_out = "> /dev/null 2> /dev/null";
	char *verbose_out = "";
	char *out = verbose ? verbose_out : quiet_out;

	if (host && node)
		sprintf(command_line, "psql -h %s -p %s -w -q -c \"select 1 a\" %s", host, node, out);
	else if (host)
		sprintf(command_line, "psql -h %s -w -q -c \"select 1 a\" %s", host, out);
	else if (node)
		sprintf(command_line, "psql -p %p -w -q -c \"select 1 a\" %s", port, out);
	else
		sprintf(command_line, "psql -w -q -c \"select 1 a\" %s", out);
	rc = system(command_line);
	exitStatus = WEXITSTATUS(rc);
	if (verbose)
	{
		if (exitStatus == 0)
			printf("Running\n");
		else
			printf("Not running\n");
	}
	
	return(exitStatus);
}

static void usage(void)
{
	printf("pgxc_monitor -Z nodetype -p port -h host\n\n");
	printf("Options are:\n");
	printf("    -Z nodetype	    What node type to monitor, gtm, gtm_proxy,\n");
	printf("                    coordinator, or datanode.\n");
	printf("    -h host         Host name or IP address of the monitored node.  Mandatory for -Z gtm or -Z gtm_proxy\n");
	printf("    -n nodename     Nodename of this pgxc_monitor.   Only for -Z gtm or -Z gtm_proxy.  Default is pgxc_monitor\n");
	printf("    -p port         Port number of the monitored node.  Mandatory for -Z gtm or -Z gtm_proxy\n");
	printf("    -q              Quiet mode.\n");
	printf("    -v              Verbose mode.\n");
	printf("    --help          Prints the help message and exit with 0.\n");
}
