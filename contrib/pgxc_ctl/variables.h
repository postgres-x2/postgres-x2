/*-------------------------------------------------------------------------
 *
 * variables.h
 *
 *    Variable handling module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef VARIABLES_H
#define VARIABLES_H

#include <stdio.h>
#include <stdlib.h>
#define NUM_HASH_BUCKET 128

typedef struct pgxc_ctl_var {
	struct pgxc_ctl_var *next;
	struct pgxc_ctl_var *prev;
	char	*varname;
	int		val_size;
	int		val_used;
	char	**val;
} pgxc_ctl_var;


extern pgxc_ctl_var *var_head;
extern pgxc_ctl_var *var_tail;

typedef struct pgxc_var_hash {
	int		el_size;
	int		el_used;
	pgxc_ctl_var **el;
} pgxc_var_hash;


pgxc_var_hash var_hash[NUM_HASH_BUCKET];

void init_var_hash(void);
void add_var_hash(pgxc_ctl_var *var);
pgxc_ctl_var *new_var(char *name);
void add_val(pgxc_ctl_var *var, char *val);
pgxc_ctl_var *find_var(char *name);
char *sval(char *name);
char **aval(char *name);
int arraySizeName(char *name);
int arraySize(pgxc_ctl_var *var);
void print_vars(void);
void print_var(char *vname);
void reset_value(pgxc_ctl_var *var);
void assign_val(char *dest, char *src);
void assign_sval(char *name, char *val);
pgxc_ctl_var *confirm_var(char *name);
void reset_var_val(char *name, char *val);
void reset_var(char *name);
void remove_var(pgxc_ctl_var *var);
void reset_value(pgxc_ctl_var *var);
void log_var(char *name);
char **add_member(char **array, char *val);
void var_assign(char **dest, char *src);
char  *listValue(char *name);

#define AddMember(a, b) do{if((a) == NULL) (a) = Malloc0(sizeof(char *)); add_member((a), (b));}while(0)
void clean_array(char **array);
#define CleanArray(a) do{clean_array(a); (a) = NULL;}while(0)
#define VAR(a) find_var(a)

#endif /* VARIABLES _H */
