/*-------------------------------------------------------------------------
 *
 * gtm_avl.h
 *
 *
 * Portions Copyright (c) 2015 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_AVL_H
#define _GTM_AVL_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_list.h"
#include "gtm/palloc.h"


typedef struct gtm_avl_node *AVL_tree_node;

typedef union
{
	void	*ptr_value;
	int		int_value;
} avl_node_data;
 
struct gtm_avl_node {
	avl_node_data	data;
	uint32			count;
	uint32			depth;
	AVL_tree_node	parent;
	AVL_tree_node	lchild;
	AVL_tree_node 	rchild;
};

typedef struct gtm_tree_stat *gtm_AVL_tree_stat;

struct gtm_tree_stat
{
	AVL_tree_node		root; /* root of the AVL tree */
	MemoryContext		avl_Context; /* local memcontext */
	/* Result slot, where we put scan results after scan */
	avl_node_data		scan_result[GTM_MAX_GLOBAL_TRANSACTIONS];
	int					scan_result_NO; /* Result number */

	/* If true we store  GTM_TransactionInfo* in avl_node_data else store xmin */
	bool				need_ext_data;
};

void	gtm_avl_delete_value(gtm_AVL_tree_stat gtm_tree, void *data);
void	gtm_avl_delete_value_int(gtm_AVL_tree_stat gtm_tree, int data);

void	gtm_avl_insert_value(gtm_AVL_tree_stat gtm_tree, void *data);
void	gtm_avl_insert_value_int(gtm_AVL_tree_stat gtm_tree, int data);

void*	gtm_avl_find_min_value(gtm_AVL_tree_stat gtm_tree);
void*	gtm_avl_find_max_value(gtm_AVL_tree_stat gtm_tree);

int		gtm_avl_find_min_value_int(gtm_AVL_tree_stat gtm_tree);
int		gtm_avl_find_max_value_int(gtm_AVL_tree_stat gtm_tree);

int		gtm_avl_find_value_above(gtm_AVL_tree_stat gtm_tree, void *data);
int		gtm_avl_find_value_bellow(gtm_AVL_tree_stat gtm_tree, void *data);
void*	gtm_avl_find_value_equal(gtm_AVL_tree_stat gtm_tree, int key);

int		gtm_avl_find_value_int_above(gtm_AVL_tree_stat gtm_tree, int data);
int		gtm_avl_find_value_int_bellow(gtm_AVL_tree_stat gtm_tree, int data);

#define	avl_node_data_pnt(node_data)		((node_data).ptr_value)
#define	get_gxid(gtm_txninfo)               (((GTM_TransactionInfo*)gtm_txninfo)->gti_gxid)                    
#define	avl_tree_data_pnt(tree_node)		((tree_node)->data.ptr_value)
#define	avl_tree_data_int(tree_node)		((tree_node)->data.int_value)
#define	tree_depth(root)					(((root) != NULL) ? ((root)->depth) : (0))
#define	gtm_avl_reset_scan_result(gtm_tree_stat)			((gtm_tree_stat)->scan_result_NO = 0)

#endif // _GTM_AVL_H
