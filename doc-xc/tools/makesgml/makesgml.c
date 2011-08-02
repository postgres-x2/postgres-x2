#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>


typedef struct tokenlist
{
	struct tokenlist *next;
	char *token;
} tokenlist;


#define STARTTOKEN "<!##"

tokenlist *ignoreToks = NULL;
tokenlist *lastIgnoreToken = NULL;
tokenlist *includeToks = NULL;
tokenlist *lastIncludeToken = NULL;

FILE *inf;
FILE *outf;
int inf_lno;
char *progname;
int default_include = 0;

void make_sgml(int writeflag);
void usage(int exitcode);
void format_err(int lno);
int my_getline(char *buf);

main(int argc, char *argv[])
{
	int flags,opt;
	char *ifnam = NULL;
	char *ofnam = NULL;

	char *token;

	inf = stdin;
	outf = stdout;

	progname = argv[0];
	while((opt = getopt(argc, argv, "i:o:E:I:d:")) != -1)
		{
			switch(opt) 
				{
				case 'i':
					if (ifnam) {
						free(ifnam);
						ifnam = NULL;
					}
					if ((strcmp(optarg, "-") == 0) || (strcmp(optarg, "stdin") == 0))
						{
							inf = stdin;
						}
					else
						{
							ifnam = strndup(optarg, strlen(optarg));
						}
					break;
				case 'o':
					if (ofnam)
						{
							free(ofnam);
							ofnam = NULL;
						}
					if ((strcmp(optarg, "-") == 0) || (strcmp(optarg, "stdout") == 0))
						{
							outf = stdout;
						}
					else
						{
							ofnam = strndup(optarg, strlen(optarg));
						}
					break;
				case 'E':
					token = strndup(optarg,strlen(optarg));
					if (ignoreToks == NULL) 
						{
							ignoreToks = (tokenlist *)malloc(sizeof(tokenlist));
							if (ignoreToks == NULL) goto memerr;
							ignoreToks->token = token;
							ignoreToks->next = NULL;
							lastIgnoreToken = ignoreToks;
						}
					else 
						{
							lastIgnoreToken->next = (tokenlist *)malloc(sizeof(tokenlist));
							if (lastIgnoreToken->next == NULL) goto memerr;
							lastIgnoreToken = lastIgnoreToken->next;
							lastIgnoreToken->next = NULL;
							lastIgnoreToken->token = token;
						}
					break;
				case 'I':
					token = strndup(optarg, strlen(optarg));
					if (includeToks == NULL)
						{
							includeToks = (tokenlist *)malloc(sizeof(tokenlist));
							if (includeToks == NULL) goto memerr;
							includeToks->token = token;
							includeToks->next = NULL;
							lastIncludeToken = includeToks;
						}
					else
						{
							lastIncludeToken->next = (tokenlist *)malloc(sizeof(tokenlist));
							if (lastIncludeToken->next == NULL) goto memerr;
							lastIncludeToken = lastIncludeToken->next;
							lastIncludeToken->next = NULL;
							lastIncludeToken->token = token;
						}
					break;
				case 'd': /* Default handling: include/exclude */
					if (strcmp(optarg, "i") == 0)
						{
							default_include = 1;
						}
					else if (strcmp(optarg, "e") == 0)
						{
							default_include = 0;
						}
					else
						{
							usage(1);
						}
					break;
				default:
					usage(1);
					exit(1);
				}
		}
	if (ifnam)
		{
			inf = fopen(ifnam, "r");
			if (inf == NULL)
				{
					fprintf(stderr, "Cannot open input file %s, %s\n", ifnam, strerror(errno));
					exit(1);
				}
		}
	inf_lno = 0;
	if (ofnam)
		{
			outf = fopen(ofnam, "w");
			if (outf == NULL)
				{
					fprintf(stderr, "Cannot open output file %s, %s\n", ofnam, strerror(errno));
					exit(1);
				}
		}
	make_sgml(1);
	exit(0);

 memerr:
	fprintf(stderr, "Memory not available.\n");
	exit(1);
}

int my_getline(char *buf)
{
	int c;

	c = getc(inf);
	if (c == EOF)
		{
			*buf = 0;
			return(EOF);
		}
	else 
		{
			ungetc(c, inf);
		}
	for(;;) {
		c = getc(inf);
		switch(c) 
			{
			case '\n':
				*buf++ = c;
				*buf = 0;
				inf_lno++;
				return(1);
			case EOF:
				*buf = 0;
				inf_lno++;
				return(1);
			default:
				*buf++ = c;
				continue;
			}
	}
	exit(1);
}


int find_match(char *token, tokenlist *toks)
{
	tokenlist *currToks;

	for (currToks = toks; currToks; currToks = currToks->next)
		{
			if (strcmp(token, currToks->token) == 0)
				return(1);
		}
	return(0);
}

int find_match_exclude(char *token)
{
	return(find_match(token, ignoreToks));
}

int find_match_include(char *token)
{
	return(find_match(token, includeToks));
}

void format_err(int lno)
{
	fprintf(stderr, "Input file format error. Line %d.\n", lno);
	exit(1);
}
	

void make_sgml(int writeflag)
{
	int rv;
	char inputline[4096];

	for(;;) {
		char *curr;
		char *token;

		rv = my_getline(inputline);
		if (rv == EOF)
			return;
		curr = inputline;
		for (;;curr++) {
			if (*curr == ' ' || *curr == '\t')
				continue;
			else
				break;
		}
		if (memcmp(curr, STARTTOKEN, strlen(STARTTOKEN)) == 0)
			{
				curr += strlen(STARTTOKEN);
				if (*curr != ' ' && *curr != '\t') {
					format_err(inf_lno);
				}
				for (curr++;;curr++) {
					if (*curr == '\n' || *curr == 0) {
						format_err(inf_lno);
					}
					if (*curr == ' ' || *curr == '\t') {
						continue;
					}
					else {
						break;
					}
				}
				token = curr;
				for (;;curr++) {
					if (*curr == '\n' || *curr == 0) {
						format_err(inf_lno);
					}
					if (*curr == ' ' || *curr == '\t') {
						*curr = 0;
						curr++;
						break;
					}
					else if (*curr == '>') {
						*curr = 0;
						curr++;
						*curr = '>';
						break;
					}
					else {
						continue;
					}
				}
				for (;;curr++) {
					if (*curr == '\n' || *curr == 0) {
						format_err(inf_lno);
					}
					if (*curr == ' ' || *curr == '\t') {
						continue;
					}
					else if (*curr == '>') {
						break;
					}
					else {
						format_err(inf_lno);
					}
				}
				/* You can write anything after clsing '>' */
				fputc('\n', outf);
				if (strcmp(token, "end") == 0)
					return;
				if (find_match_exclude(token)) {
					make_sgml(0);
				}
				else if (find_match_include(token)) {
					if (writeflag)
						make_sgml(1);
					else
						make_sgml(0);
				}
				else {
					make_sgml(0);
				}
			}
		else
			{
				if (writeflag)
					fputs(inputline, outf);
				else
					fputc('\n', outf);
			}
	}
	exit(1);
}

void usage(int exitcode)
{
	fprintf(stderr, 
			"%s -i infile -o outfile [-d i|e ] -D exclude_token -D ... -U include_token -U ...\n", 
			progname);
	exit(exitcode);
}
