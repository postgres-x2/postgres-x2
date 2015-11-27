#include "gtm/gtm_avl.h"
#include "gtm/gtm.h"
#include "gtm/gtm_list.h"

static	void	travel(AVL_tree_node root, gtm_AVL_tree_stat gtm_tree_stat);
static	uint32	depth(AVL_tree_node root);
static 	int		depth_diff(AVL_tree_node root); 
static	void	update_root_depth(AVL_tree_node root); 
static 	void	insert_node_to_nonempty_tree(AVL_tree_node tr,
											gtm_AVL_tree_stat gtm_tree,
											AVL_tree_node np);
static	AVL_tree_node	rebalance_avl(AVL_tree_node np);
static	AVL_tree_node	insert_leaf(gtm_AVL_tree_stat root, void* value);
static	AVL_tree_node	insert_leaf_int(gtm_AVL_tree_stat root, int value);
static 	AVL_tree_node	left_single_rotate(AVL_tree_node root);
static 	AVL_tree_node	left_double_rotate(AVL_tree_node root);
static 	AVL_tree_node	right_single_rotate(AVL_tree_node root);
static	AVL_tree_node	right_double_rotate(AVL_tree_node root);
static 	AVL_tree_node	find_node_from_nonempty_tree(AVL_tree_node tr,
													gtm_AVL_tree_stat gtm_tree,
													uint32 value);

static 	AVL_tree_node	delete_leaf(AVL_tree_node root,
									gtm_AVL_tree_stat gtm_tree_stat, uint32 value);

static gtm_List* find_value_bellow_intel(gtm_AVL_tree_stat gtm_tree, int value);

static gtm_List* find_value_above_intel(gtm_AVL_tree_stat gtm_tree, int value);

/*
 * insert value
 *
 */
void
avl_insert_value(gtm_AVL_tree_stat gtm_tree, void *data)
{
	AVL_tree_node leaf;
    AVL_tree_node new_root;

	/* insert a value to a binary search tree */
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
 * insert value
 *
 */
void
avl_insert_value_int(gtm_AVL_tree_stat gtm_tree, int data)
{
	AVL_tree_node leaf;
    AVL_tree_node new_root;

	/* insert a value to a binary search tree */
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
 * delete value
 *
 */
void 
avl_delete_value(gtm_AVL_tree_stat gtm_tree, void *data)
{
	AVL_tree_node leaf;
    uint32 value;
    AVL_tree_node new_root;

	Assert(gtm_tree->ext_data != NULL); 
	value = gtm_tree->ext_data(data);
	/* insert a value to a binary search tree */
	leaf = delete_leaf(gtm_tree->root, gtm_tree, value);
    if (leaf != NULL) {
	    update_root_depth(leaf);
        new_root = rebalance_avl(leaf);
        if (new_root != NULL)
		    gtm_tree->root = new_root;
    }
}

/*
 * delete value
 *
 */
void 
avl_delete_value_int(gtm_AVL_tree_stat gtm_tree, int value)
{
	AVL_tree_node leaf;
    AVL_tree_node new_root;

	/* insert a value to a binary search tree */
	leaf = delete_leaf(gtm_tree->root, gtm_tree, value);
    if (leaf != NULL) {
	    update_root_depth(leaf);
        new_root = rebalance_avl(leaf);
        if (new_root != NULL)
		    gtm_tree->root = new_root;
    }
}

void* 
avl_find_min_value(gtm_AVL_tree_stat gtm_tree)
{
	AVL_tree_node child = gtm_tree->root;
	while(child->lchild != NULL)
		child = child->lchild;

	Assert(gtm_tree->ext_data != NULL);
	return avl_tree_data_pnt(child);
}

int 
avl_find_min_value_int(gtm_AVL_tree_stat gtm_tree)
{
	AVL_tree_node child = gtm_tree->root;
	while(child->lchild != NULL)
		child = child->lchild;

	return avl_tree_data_int(child);

}

void* 
avl_find_max_value(gtm_AVL_tree_stat gtm_tree)
{
	//assert(tr != NULL);
	AVL_tree_node child = gtm_tree->root;
	while(child->rchild != NULL)
		child = child->rchild;

	Assert(gtm_tree->ext_data != NULL);
	return avl_tree_data_pnt(child);
}

int 
avl_find_max_value_int(gtm_AVL_tree_stat gtm_tree)
{
	//assert(tr != NULL);
	AVL_tree_node child = gtm_tree->root;
	while(child->rchild != NULL)
		child = child->rchild;

	return avl_tree_data_int(child);
}


static gtm_List* 
find_value_above_intel(gtm_AVL_tree_stat gtm_tree, int value)
{
	int  tmp;
	MemoryContext oldContext;
    AVL_tree_node child = gtm_tree->root;

	if (gtm_tree->root == NULL) {
		return gtm_NIL;
	}

	avl_reset_scan_result(gtm_tree);

	oldContext = MemoryContextSwitchTo(gtm_tree->avl_Context);

	do {
		if (gtm_tree->ext_data != NULL) {
			tmp = gtm_tree->ext_data(avl_tree_data_pnt(child));
		} else {
			tmp =  avl_tree_data_int(child);
		}

		if (tmp > value) {
			gtm_tree->scan_result = gtm_lappend_int(gtm_tree->scan_result,
													tmp);
			travel(child->rchild, gtm_tree);
			child = child->lchild;
		} else {
			child = child->rchild;
		}

	} while(child != NULL);

	MemoryContextSwitchTo(oldContext);
	return gtm_tree->scan_result;
}

gtm_List* 
avl_find_value_above(gtm_AVL_tree_stat gtm_tree, void*data) 
{
	int value;

	if (gtm_tree->root == NULL) {
		return gtm_NIL;
	}
	Assert (gtm_tree->ext_data != NULL);
	value = gtm_tree->ext_data(data);
	
    return find_value_above_intel(gtm_tree, value);
}

gtm_List* 
avl_find_value_int_above(gtm_AVL_tree_stat gtm_tree, int value) 
{
    return find_value_above_intel(gtm_tree, value);
}

static gtm_List*
find_value_bellow_intel(gtm_AVL_tree_stat gtm_tree, int value)
{
	MemoryContext oldContext;
	AVL_tree_node child = gtm_tree->root;
	int tmp;
	if (gtm_tree->root == NULL) {
		return gtm_NIL;
	}

	avl_reset_scan_result(gtm_tree);

	oldContext = MemoryContextSwitchTo(gtm_tree->avl_Context);
	do {
		if (gtm_tree->ext_data != NULL) {
			tmp = gtm_tree->ext_data(avl_tree_data_pnt(child));
		} else {
			tmp = avl_tree_data_int(child);
		}
		if (tmp >= value) {
			child = child->lchild;
		} else {
			// so tmp < value, the subtree of lchild will be less than value,
			// But not sure about rchild, so check rchild after travel rchild
			gtm_tree->scan_result = gtm_lappend(gtm_tree->scan_result,
													avl_tree_data_pnt(child));

			travel(child->lchild, gtm_tree);
			child = child->rchild;
		}
	} while(child != NULL) ;

	MemoryContextSwitchTo(oldContext);
    return gtm_tree->scan_result;
}

gtm_List* 
avl_find_value_bellow(gtm_AVL_tree_stat gtm_tree, void*data) 
{
	int value;

	if (gtm_tree->root == NULL) {
		return gtm_NIL;
	}
	Assert (gtm_tree->ext_data != NULL);
	value = gtm_tree->ext_data(data);
	
    return find_value_bellow_intel(gtm_tree, value);
}

gtm_List* 
avl_find_value_int_bellow(gtm_AVL_tree_stat gtm_tree, int value) 
{
    return find_value_bellow_intel(gtm_tree, value);
}

void*
avl_find_value_equal(gtm_AVL_tree_stat gtm_tree, int value)
{
	AVL_tree_node child = gtm_tree->root;
 	uint32 tmp;

	if (gtm_tree->root == NULL) {
		return NULL;
	}
	Assert(gtm_tree->ext_data != NULL);

	do {
		tmp = gtm_tree->ext_data(avl_tree_data_pnt(child));
		if (tmp > value) {
			child = child->lchild;
		} else if (tmp < value){
			child = child->rchild;
		} else {
            /* Found the target, return*/
            break;
        }
	} while(child != NULL) ;

	if (child == NULL) {
		return NULL;
	} else {
		return avl_tree_data_pnt(child);
	}
}

void
avl_reset_scan_result(gtm_AVL_tree_stat gtm_tree)
{
    if (gtm_tree->scan_result != gtm_NIL) {
		gtm_list_free(gtm_tree->scan_result);
		gtm_tree->scan_result =  gtm_NIL; 
    }
}

static void
travel(AVL_tree_node root, gtm_AVL_tree_stat gtm_tree_stat)
{
    if (root == NULL)
        return;

	if (gtm_tree_stat->ext_data != NULL) {
		gtm_tree_stat->scan_result = gtm_lappend(gtm_tree_stat->scan_result,
												avl_tree_data_pnt(root));
	} else {
		gtm_tree_stat->scan_result = gtm_lappend_int(gtm_tree_stat->scan_result,
												avl_tree_data_int(root));
	}

	if (root->lchild != NULL) {
		travel(root->lchild, gtm_tree_stat);
	}

	if (root->rchild != NULL) {
		travel(root->rchild, gtm_tree_stat);
	}
}

/*
 *  * get the depth of the tree
 *   * use this function to access depth
 *    */
static uint32
depth(AVL_tree_node root)
{
	if (root == NULL) {
		return 0;
	} else {
		return root->depth;
	}
}

/* 
 *  * traverse the path from new node to root node
 *   * make one rotation, rebalance AVL and stop
 *    */
static AVL_tree_node
rebalance_avl( AVL_tree_node np) 
{
	int myDiff;
    AVL_tree_node tr = NULL;
	while (np != NULL) {
		update_root_depth(np);
		myDiff = depth_diff(np);
		if (myDiff > 1 || myDiff < -1) {
			if (myDiff > 1) {
				/* left rotate needed */
				if(depth_diff(np->rchild) > 0) {
					np = left_single_rotate(np);
				} else {
					np = left_double_rotate(np);
				}
			}

			if (myDiff < -1) {
				if(depth_diff(np->lchild) < 0) {
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
 * difference of rchild->depth and lchild->depth
 */
static int
depth_diff(AVL_tree_node tr) 
{
	if (tr == NULL) {
		return 0;
	} else {
		return depth(tr->rchild) - depth(tr->lchild);
	}
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
 * return
 */
static AVL_tree_node
left_double_rotate(AVL_tree_node tr) 
{
	right_single_rotate(tr->rchild);
	return left_single_rotate(tr);
}

/*
 * right double rotation
 * return
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
		depLChild = depth(tr->lchild);
		depRChild = depth(tr->rchild);
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

	Assert(gtm_tree->ext_data != NULL);
	value = gtm_tree->ext_data(data);

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

static AVL_tree_node
insert_leaf_int(gtm_AVL_tree_stat gtm_tree, int value) 
{
	AVL_tree_node np;
    MemoryContext oldContext;

	/* prepare the node */
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
 * delete value from the leaf of tree
 * return address of the node
 */
static AVL_tree_node
delete_leaf(AVL_tree_node tr,gtm_AVL_tree_stat gtm_tree, uint32 value) 
{
    // From where to adjust the tree 
	AVL_tree_node np = NULL;
	/* prepare the node */
	AVL_tree_node newChild = NULL;
	AVL_tree_node tp = NULL;

	if (tr != NULL) {
    	tp = find_node_from_nonempty_tree(tr, gtm_tree, value);
	}

	if (tp != NULL) {
		tp->count--;
        if (tp->count > 0) {
			// no need to update AVL tree, so return NULL
            return NULL;
        }

		if (tp->rchild != NULL){
			newChild = tp->rchild;
			while (newChild->lchild != NULL)
				newChild = newChild->lchild;

            if (newChild != tp->rchild) {
                newChild->parent->lchild = newChild->rchild;
				if (newChild->rchild != NULL) {
               		newChild->rchild->parent = newChild->parent; 
            	}

                np = newChild->parent;
            } else {
                np = newChild;
            } 

			newChild->lchild = tp->lchild;
            if (tp->lchild != NULL) {
                tp->lchild->parent = newChild;
            }

            if (newChild != tp->rchild){
                newChild->rchild = tp->rchild;
                tp->rchild->parent = newChild;
            }

		} else if (tp->lchild != NULL){
			newChild = tp->lchild;
			np = tp->parent;
       	} else {
			newChild = NULL;
			np = tp->parent;
            /*if (tp == gtm_tree->root) {
                gtm_tree->root = NULL;
            }*/
		}

        if (tp->parent != NULL) { 
		    if (tp->parent->lchild == tp) {
			    tp->parent->lchild = newChild;
		    } else {
			    tp->parent->rchild = newChild;
		    }
        } else {
            // delete the root of tree
            if (tp == gtm_tree->root) {
                gtm_tree->root = newChild;
            }
        }

		if (newChild != NULL) {
			newChild->parent = tp->parent;
		} 

		pfree(tp); 
	}

	return np;
}
/*
 * insert a node to a non-empty tree
 * called by insert_value()
 */
static void
insert_node_to_nonempty_tree(AVL_tree_node tr, gtm_AVL_tree_stat gtm_tree, AVL_tree_node np)
{
	uint32 value_tr, value_np;

	if (gtm_tree->ext_data != NULL) {
		value_tr = gtm_tree->ext_data(avl_tree_data_pnt(tr));
		value_np = gtm_tree->ext_data(avl_tree_data_pnt(np));
	} else {
		value_tr = avl_tree_data_int(tr);
		value_np = avl_tree_data_int(np);
	}

	/* insert the node */
	if(value_np <= value_tr) {
		if (tr->lchild == NULL) {
			/* then tr->lchild is the proper place */
			tr->lchild = np;
			np->parent = tr;
			return;
		} else {
			insert_node_to_nonempty_tree(tr->lchild, gtm_tree, np);
		}

	} else if(value_np > value_tr) {
		if (tr->rchild == NULL) {
			tr->rchild = np;
			np->parent = tr;
			return;
		} else {
			insert_node_to_nonempty_tree(tr->rchild, gtm_tree, np);
		}

	}
}

/*
 * find a node from a non-empty tree
 * called by delete_value()
 */
static AVL_tree_node
find_node_from_nonempty_tree(AVL_tree_node tr, gtm_AVL_tree_stat gtm_tree, uint32 value)
{
	/* find the node */
	uint32 data;
	if (tr == NULL) {
		return NULL;
	}

	if (gtm_tree->ext_data != NULL) {
		data = gtm_tree->ext_data(avl_tree_data_pnt(tr));
	} else {
		data = avl_tree_data_int(tr);
	}

	if (data == value) {
		return tr;
	} else if (value < data) {
		return find_node_from_nonempty_tree(tr->lchild, gtm_tree, value);
    } else {
		return find_node_from_nonempty_tree(tr->rchild, gtm_tree, value);
	}
}
