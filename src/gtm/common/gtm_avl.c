#include "gtm/gtm_avl.h"
#include "gtm/gtm.h"
#include "gtm/gtm_list.h"

static  void    travel(AVL_tree_node root, gtm_AVL_tree_stat gtm_tree_stat);
static  void    update_root_depth(AVL_tree_node root); 
static  void    insert_node_to_nonempty_tree(AVL_tree_node sub_tree,
											gtm_AVL_tree_stat gtm_tree,
											AVL_tree_node new_node);
static  AVL_tree_node   rebalance_avl(AVL_tree_node new_node);
static  AVL_tree_node	insert_leaf(gtm_AVL_tree_stat root, void* value);
static  AVL_tree_node	insert_leaf_int(gtm_AVL_tree_stat root, int value);
static  AVL_tree_node	left_single_rotate(AVL_tree_node root);
static  AVL_tree_node	left_double_rotate(AVL_tree_node root);
static  AVL_tree_node	right_single_rotate(AVL_tree_node root);
static  AVL_tree_node	right_double_rotate(AVL_tree_node root);
static  AVL_tree_node	find_node_from_nonempty_tree(AVL_tree_node sub_tree,
													gtm_AVL_tree_stat gtm_tree,
													int value);

static  AVL_tree_node	delete_leaf(AVL_tree_node root,
									gtm_AVL_tree_stat gtm_tree_stat, int value);

static  int find_value_bellow_intel(gtm_AVL_tree_stat gtm_tree, int value);

static  int find_value_above_intel(gtm_AVL_tree_stat gtm_tree, int value);

static  int tree_depth_diff(AVL_tree_node root);

/*
  * insertGTM_TransactionInfo* pointer to AVL tree
 *
 */
void
gtm_avl_insert_value(gtm_AVL_tree_stat gtm_tree, void *data)
{
	AVL_tree_node leaf;
	AVL_tree_node new_root;

	leaf = insert_leaf(gtm_tree, data);
	update_root_depth(leaf);

	if (gtm_tree->root == NULL) {
		gtm_tree->root = leaf;
	} else {
		new_root = rebalance_avl(leaf);
		if (new_root != NULL)
			gtm_tree->root = new_root;
	}
}

/*
 * insert int value to AVL tree, for xmin
 *
 */
void
gtm_avl_insert_value_int(gtm_AVL_tree_stat gtm_tree, int data)
{
	AVL_tree_node leaf;
	AVL_tree_node new_root;

	leaf = insert_leaf_int(gtm_tree, data);
	update_root_depth(leaf);

	if (gtm_tree->root == NULL) {
    	gtm_tree->root = leaf;
	} else {
		new_root = rebalance_avl(leaf);
		if (new_root != NULL)
			gtm_tree->root = new_root;
	}
}

/*
 * Delete GTM_TransactionInfo* pointer from AVL tree
 *
 */
void 
gtm_avl_delete_value(gtm_AVL_tree_stat gtm_tree, void *data)
{
	AVL_tree_node leaf;
	int value;
	AVL_tree_node new_root;

	Assert(gtm_tree->need_ext_data); 
	value = get_gxid(data);
	/* delete node and get the parent of target */
	leaf = delete_leaf(gtm_tree->root, gtm_tree, value);
	if (leaf != NULL) {
		update_root_depth(leaf);
		/* rebalance the AVL tree, from the leaf to the root*/
		new_root = rebalance_avl(leaf);
		// root changed, need to update
		if (new_root != NULL)
			gtm_tree->root = new_root;
    }

}

/*
 * Delete int value from AVL tree
 *
 */
void 
gtm_avl_delete_value_int(gtm_AVL_tree_stat gtm_tree, int value)
{
	AVL_tree_node leaf;
	AVL_tree_node new_root;

	/* delete node and get the parent of target */
	leaf = delete_leaf(gtm_tree->root, gtm_tree, value);
	if (leaf != NULL) {
		update_root_depth(leaf);
		/* rebalance the AVL tree, from the leaf to the root*/
		new_root = rebalance_avl(leaf);
		if (new_root != NULL)
			// root changed, need to update
		gtm_tree->root = new_root;
	}
}

/*
 * Find the min value from AVL tree, acutually gtm call
 * it to find the min gxid
 *
 */
void* 
gtm_avl_find_min_value(gtm_AVL_tree_stat gtm_tree)
{
	AVL_tree_node child = gtm_tree->root;
	Assert(child != NULL);

	while (child->lchild != NULL)
		child = child->lchild;

	Assert(gtm_tree->need_ext_data);
	return avl_tree_data_pnt(child);
}

/*
 * Find the min value from AVL tree, acutually gtm call
 * it to find the min xmin
 *
 */
int 
gtm_avl_find_min_value_int(gtm_AVL_tree_stat gtm_tree)
{
	AVL_tree_node child = gtm_tree->root;
	Assert(child != NULL);

	while (child->lchild != NULL)
		child = child->lchild;

	return avl_tree_data_int(child);

}

/*
 * Find the max value from AVL tree, acutually gtm call
 * it to find the max gxid
 *
 */
void* 
gtm_avl_find_max_value(gtm_AVL_tree_stat gtm_tree)
{
	AVL_tree_node child = gtm_tree->root;
	Assert(child != NULL);

	while (child->rchild != NULL)
		child = child->rchild;

	Assert(gtm_tree->need_ext_data);
	return avl_tree_data_pnt(child);
}

/*
 * Find the max value from AVL tree, acutually gtm call
 * it to find the max xmin
 *
 */
int 
gtm_avl_find_max_value_int(gtm_AVL_tree_stat gtm_tree)
{
	AVL_tree_node child = gtm_tree->root;
	Assert(child != NULL);

	while (child->rchild != NULL)
		child = child->rchild;

	return avl_tree_data_int(child);
}

/*
 *  Collect the items, data of  which larger than specified value
 *  Return the number of items qualified
 * */
static int 
find_value_above_intel(gtm_AVL_tree_stat gtm_tree, int value)
{
	int  tmp;
	AVL_tree_node child = gtm_tree->root;
	if (gtm_tree->root == NULL) {
		return 0;
	}

	gtm_avl_reset_scan_result(gtm_tree);

	do {
		if (gtm_tree->need_ext_data) {
			tmp = get_gxid(avl_tree_data_pnt(child));
		} else {
			tmp =  avl_tree_data_int(child);
		}

		if (tmp > value) {
			// Find the target node, then put data into the result slot
			gtm_tree->scan_result[gtm_tree->scan_result_NO] = child->data;
			gtm_tree->scan_result_NO++;
			
			// children of rchild should be larger than value, so collect them 
			travel(child->rchild, gtm_tree);
            // But no sure about the lchild, so check it next loop
			child = child->lchild;
		} else {
			child = child->rchild;
		}

	} while (child != NULL);

	return gtm_tree->scan_result_NO;
}

/*
 * Find the value large than specified data
 * return the number of qualified items
 *
 */
int 
gtm_avl_find_value_above(gtm_AVL_tree_stat gtm_tree, void*data) 
{

	if (gtm_tree->root == NULL) {
		return 0;
	}

	Assert(gtm_tree->need_ext_data);
	return find_value_above_intel(gtm_tree, get_gxid(data));
}

/*
 * Find the value large than specified data
 * return the number of qualified items
 *
 */
int 
gtm_avl_find_value_int_above(gtm_AVL_tree_stat gtm_tree, int value) 
{
	return find_value_above_intel(gtm_tree, value);
}

/*
 *  Collect the items, data of  which less than specified value
 *  Return the number of items qualified
 * */
static int
find_value_bellow_intel(gtm_AVL_tree_stat gtm_tree, int value)
{
	AVL_tree_node child = gtm_tree->root;
	int tmp;
	if (gtm_tree->root == NULL) {
		return 0;
	}

	gtm_avl_reset_scan_result(gtm_tree);

	do {
		if (gtm_tree->need_ext_data) {
			tmp = get_gxid(avl_tree_data_pnt(child));
		} else {
			tmp = avl_tree_data_int(child);
		}

		if (tmp >= value) {
			child = child->lchild;
		} else {
			// so tmp < value, the subtree of lchild will be less than value,
			// But not sure about rchild, so check rchild after travel rchild
			gtm_tree->scan_result[gtm_tree->scan_result_NO] = child->data;
			gtm_tree->scan_result_NO++;

			travel(child->lchild, gtm_tree);
			child = child->rchild;
		}

	} while (child != NULL);

	return gtm_tree->scan_result_NO;
}

/*
 * Find the value less than specified data
 * return the number of qualified items
 *
 */
int 
gtm_avl_find_value_bellow(gtm_AVL_tree_stat gtm_tree, void*data) 
{

	if (gtm_tree->root == NULL) {
		return 0;
	}

	Assert(gtm_tree->need_ext_data);

	return find_value_bellow_intel(gtm_tree, get_gxid(data));
}

/*
 * Find the value less than specified data
 * return the number of qualified items
 *
 */
int
gtm_avl_find_value_int_bellow(gtm_AVL_tree_stat gtm_tree, int value) 
{
	return find_value_bellow_intel(gtm_tree, value);
}

/*
 * Find the value equals specified data
 * return item
 * gtm call it to find some GTM_TransactionInfo* pointer according to gxid
 *
 */
void*
gtm_avl_find_value_equal(gtm_AVL_tree_stat gtm_tree, int value)
{
	AVL_tree_node child = gtm_tree->root;
	int tmp;

	if (gtm_tree->root == NULL) {
		return NULL;
	}

	Assert(gtm_tree->need_ext_data);

	do {
		tmp = get_gxid(avl_tree_data_pnt(child));
		if (tmp > value) {
			child = child->lchild;
		} else if (tmp < value){
			child = child->rchild;
		} else {
            /* Found the target, return */
			break;
		}

	} while (child != NULL);

	if (child == NULL) {
		return NULL;
	} else {
		return avl_tree_data_pnt(child);
	}
}

static void
travel(AVL_tree_node root, gtm_AVL_tree_stat gtm_tree_stat)
{
	if (root == NULL)
		return;

	/* collect the data of current root and put it into result slot */
	gtm_tree_stat->scan_result[gtm_tree_stat->scan_result_NO] = root->data;
	gtm_tree_stat->scan_result_NO++;

	if (root->lchild != NULL) {
		travel(root->lchild, gtm_tree_stat);
	}

	if (root->rchild != NULL) {
		travel(root->rchild, gtm_tree_stat);
	}
}

/*
 * Get the difference of rchild->depth and lchild->depth
 */
static int
tree_depth_diff(AVL_tree_node root)
{
	if (root == NULL) {
		return 0;
	} else {
		return tree_depth(root->rchild) - tree_depth(root->lchild);
	}
}

/* 
 * traverse the path from new node to root node
 * make one rotation, rebalance AVL and stop
 */
static AVL_tree_node
rebalance_avl( AVL_tree_node np) 
{
	int depth_diff;
	AVL_tree_node tr = NULL;
	while (np != NULL) {
		update_root_depth(np);
		depth_diff = tree_depth_diff(np);
		if (depth_diff > 1 || depth_diff < -1) {
			if (depth_diff > 1) {
				/* left rotate needed */
				if(tree_depth_diff(np->rchild) > 0) {
					np = left_single_rotate(np);
				} else {
					np = left_double_rotate(np);
				}
			}

			if (depth_diff < -1) {
				if(tree_depth_diff(np->lchild) < 0) {
					np = right_single_rotate(np);
				} else {
					np = right_double_rotate(np);
				}
			}

			/* if rotation changes root node */
			if (np->parent == NULL)
				tr = np;

			break;
		}

		np = np->parent;
	}
    
	return tr;
}


/* 
 * left single rotation 
 * return the new root
 */
static AVL_tree_node
left_single_rotate(AVL_tree_node tr) 
{
	AVL_tree_node newRoot, parent;
	parent  = tr->parent;
	newRoot = tr->rchild;
	/* detach & attach */ 
	if (newRoot->lchild != NULL)
		newRoot->lchild->parent = tr;

	tr->rchild = newRoot->lchild;
	update_root_depth(tr);
   
	/* raise new root node */
	newRoot->lchild = tr;
	newRoot->parent = parent;
	if (parent != NULL) {
		if (parent->lchild == tr) {
			parent->lchild = newRoot;
    	} else {
			parent->rchild = newRoot;
		}
	}

	tr->parent = newRoot;
	update_root_depth(newRoot);
	return newRoot;
}

/* 
 * right single rotation 
 * return the new root
 */
static AVL_tree_node
right_single_rotate(AVL_tree_node tr) 
{
	AVL_tree_node newRoot, parent;
	parent  = tr->parent;
	newRoot = tr->lchild;

	/* detach & attach */
	if (newRoot->rchild != NULL)
		newRoot->rchild->parent = tr;

	tr->lchild = newRoot->rchild;
	update_root_depth(tr);
  
	/* raise new root node */
	newRoot->rchild = tr;
	newRoot->parent = parent;
	if (parent != NULL) {
		if (parent->lchild == tr) {
			parent->lchild = newRoot;
    	} else {
			parent->rchild = newRoot;
		}
	}

	tr->parent = newRoot;
	update_root_depth(newRoot);
	return newRoot;
}

/*
 * left double rotation
 */
static AVL_tree_node
left_double_rotate(AVL_tree_node tr) 
{
	right_single_rotate(tr->rchild);
	return left_single_rotate(tr);
}

/*
 * right double rotation
 */
static AVL_tree_node
right_double_rotate(AVL_tree_node tr) 
{
	left_single_rotate(tr->lchild);
	return right_single_rotate(tr);
}

/*
 * update tr->depth
 * assume lchild->depth and rchild->depth are correct
 */
static void
update_root_depth(AVL_tree_node tr) 
{
	int maxChildDepth; 
	int depLChild, depRChild;
	if (tr==NULL) {
		return;
	} else {
		depLChild = tree_depth(tr->lchild);
		depRChild = tree_depth(tr->rchild);
		maxChildDepth = depLChild > depRChild ? depLChild : depRChild;
		tr->depth = maxChildDepth + 1;
	}
}

/* 
 * insert a new value into the tree as a leaf
 * return address of the new node
 */
static AVL_tree_node
insert_leaf(gtm_AVL_tree_stat gtm_tree, void* data) 
{
	AVL_tree_node np;
	uint32 value;
	MemoryContext oldContext;

	Assert(gtm_tree->need_ext_data);
	value = get_gxid(data);

	/* prepare the node */
	np = find_node_from_nonempty_tree(gtm_tree->root, gtm_tree, value);

	if (np != NULL) {
		np->count++;
		return np;
	}

	oldContext = MemoryContextSwitchTo(gtm_tree->avl_Context);
	np = (AVL_tree_node) palloc(sizeof(struct gtm_avl_node));
	avl_tree_data_pnt(np) = data;
	np->count = 1;
	np->parent  = NULL;
	np->lchild  = NULL;
	np->rchild  = NULL;
	MemoryContextSwitchTo(oldContext);

	if (gtm_tree->root != NULL) {
		insert_node_to_nonempty_tree(gtm_tree->root, gtm_tree, np);
	}

	return np;
}

/* 
 * insert a new int value into the tree as a leaf
 * return address of the new node
 */
static AVL_tree_node
insert_leaf_int(gtm_AVL_tree_stat gtm_tree, int value) 
{
	AVL_tree_node np;
    MemoryContext oldContext;

	np = find_node_from_nonempty_tree(gtm_tree->root, gtm_tree, value);

	if (np != NULL) {
		np->count++;
		return np;
	}

	oldContext = MemoryContextSwitchTo(gtm_tree->avl_Context);
	np = (AVL_tree_node) palloc(sizeof(struct gtm_avl_node));
	avl_tree_data_int(np) = value;
	np->count = 1;
	np->parent  = NULL;
	np->lchild  = NULL;
	np->rchild  = NULL;
 	MemoryContextSwitchTo(oldContext);

	if (gtm_tree->root != NULL) {
		insert_node_to_nonempty_tree(gtm_tree->root, gtm_tree, np);
	}

	return np;
}

/* 
 * Delete value from the leaf of tree
 * return parent address of the node if necessary 
 */
static AVL_tree_node
delete_leaf(AVL_tree_node tr,gtm_AVL_tree_stat gtm_tree, int value) 
{
	// From where to adjust the tree, the parent of target node
	AVL_tree_node np = NULL;

	AVL_tree_node newChild = NULL;
	AVL_tree_node tp = NULL;

	if (tr != NULL) {
		/* find the target node via binary search */
    	tp = find_node_from_nonempty_tree(tr, gtm_tree, value);
	}

	if (tp != NULL) {
		tp->count--;
		if (tp->count > 0) {
			// no need to update AVL tree, so return NULL
			return NULL;
		}

		if (tp->rchild != NULL){
			/* target node has rchild */
			newChild = tp->rchild;
			/* get the smallest child of target node */
			while (newChild->lchild != NULL)
				newChild = newChild->lchild;

			if (newChild != tp->rchild) {
				/* pull up the lchild of the smallest child*/
				newChild->parent->lchild = newChild->rchild;
				if (newChild->rchild != NULL) {
					/* update the parent of smallest child*/
					newChild->rchild->parent = newChild->parent; 
            	}
				/* OK, we need to save the parent of target node
 				 * since we need to rebalance AVL tree from this node
 				 * */
				np = newChild->parent;
			} else {
				np = newChild;
			} 

			/* update the lchild of smallest child */
			newChild->lchild = tp->lchild;
			if (tp->lchild != NULL) {
				tp->lchild->parent = newChild;
			}

			/* update the rchild of smallest child */
			if (newChild != tp->rchild){
				newChild->rchild = tp->rchild;
				tp->rchild->parent = newChild;
			}

		} else if (tp->lchild != NULL){
			/*no rchild, but has lchild*/
			newChild = tp->lchild;
			np = tp->parent;
       	} else {
			/* no child */
			newChild = NULL;
			np = tp->parent;
		}

		if (tp->parent != NULL) {
			// update the child of the target node's parent 
			if (tp->parent->lchild == tp) {
				tp->parent->lchild = newChild;
			} else {
				tp->parent->rchild = newChild;
			}

		} else {
			// we delete the root of tree, so set root to be NULL
			if (tp == gtm_tree->root) {
				gtm_tree->root = newChild;
			}
		}

		if (newChild != NULL) {
			// update the parent
			newChild->parent = tp->parent;
		}

		pfree(tp); 
	}

	return np;
}

/*
 * Insert a node to a non-empty tree
 */
static void
insert_node_to_nonempty_tree(AVL_tree_node sub_tree,
							gtm_AVL_tree_stat gtm_tree,
							AVL_tree_node new_node)
{
	int value_st, value_nn;

	if (gtm_tree->need_ext_data) {
		value_st = get_gxid(avl_tree_data_pnt(sub_tree));
		value_nn = get_gxid(avl_tree_data_pnt(new_node));
	} else {
		value_st = avl_tree_data_int(sub_tree);
		value_nn = avl_tree_data_int(new_node);
	}

	/* insert the node */
	if(value_nn <= value_st) {
		if (sub_tree->lchild == NULL) {
			sub_tree->lchild = new_node;
			new_node->parent = sub_tree;
			return;
		} else {
			insert_node_to_nonempty_tree(sub_tree->lchild, gtm_tree, new_node);
		}

	} else if(value_nn > value_st) {
		if (sub_tree->rchild == NULL) {
			sub_tree->rchild = new_node;
			new_node->parent = sub_tree;
			return;
		} else {
			insert_node_to_nonempty_tree(sub_tree->rchild, gtm_tree, new_node);
		}

	}
}

/*
 * Find a node from a non-empty tree
 */
static AVL_tree_node
find_node_from_nonempty_tree(AVL_tree_node sub_tree,
							gtm_AVL_tree_stat gtm_tree,
							int value)
{
	int data;
	if (sub_tree == NULL) {
		return NULL;
	}
	
	/* extract data from sub_tree */
	if (gtm_tree->need_ext_data) {
		data = get_gxid(avl_tree_data_pnt(sub_tree));
	} else {
		data = avl_tree_data_int(sub_tree);
	}

	if (data == value) {
	    /* find the node */
		return sub_tree;
	} else if (value < data) {
		return find_node_from_nonempty_tree(sub_tree->lchild, gtm_tree, value);
	} else {
		return find_node_from_nonempty_tree(sub_tree->rchild, gtm_tree, value);
	}
}
