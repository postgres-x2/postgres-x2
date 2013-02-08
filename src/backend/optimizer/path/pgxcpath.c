/*-------------------------------------------------------------------------
 *
 * rquerypath.c
 *	  Routines to find possible remote query paths for various relations and
 *	  their costs.
 *
 * Portions Copyright (c) 2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/rquerypath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/tablecmds.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/pgxcship.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"

static RemoteQueryPath *pgxc_find_remotequery_path(RelOptInfo *rel);
static RemoteQueryPath *create_remotequery_path(PlannerInfo *root, RelOptInfo *rel,
								ExecNodes *exec_nodes, RemoteQueryPath *leftpath,
								RemoteQueryPath *rightpath, JoinType jointype,
								List *join_restrictlist);
/*
 * create_remotequery_path
 *	  Creates a path for given RelOptInfo (for base rel or a join rel) so that
 *	  the results corresponding to this RelOptInfo are obtained by querying
 *	  datanode/s. When RelOptInfo represents a JOIN, we leftpath and rightpath
 *	  represents the RemoteQuery paths for left and right relations resp,
 *	  jointype gives the type of JOIN and join_restrictlist gives the
 *	  restrictinfo list for the JOIN. For a base relation, these should be
 *	  NULL.
 *	  ExecNodes is the set of datanodes to which the query should be sent to.
 *	  This function also marks the path with shippability of the quals.
 *	  If any of the relations involved in this path is a temporary relation,
 *	  record that fact.
 */
static RemoteQueryPath *
create_remotequery_path(PlannerInfo *root, RelOptInfo *rel, ExecNodes *exec_nodes,
						RemoteQueryPath *leftpath, RemoteQueryPath *rightpath,
						JoinType jointype, List *join_restrictlist)
{
	RemoteQueryPath	*rqpath = makeNode(RemoteQueryPath);
	bool			unshippable_quals;

	if (rel->reloptkind == RELOPT_JOINREL && (!leftpath || !rightpath))
		elog(ERROR, "a join rel requires both the left path and right path");

	rqpath->path.pathtype = T_RemoteQuery;
	rqpath->path.parent = rel;
	/* PGXC_TODO: do we want to care about it */
	rqpath->path.param_info = NULL;
	rqpath->path.pathkeys = NIL;	/* result is always unordered */
	rqpath->rqpath_en = exec_nodes;
	rqpath->leftpath = leftpath;
	rqpath->rightpath = rightpath;
	rqpath->jointype = jointype;
	rqpath->join_restrictlist = join_restrictlist;

	switch (rel->reloptkind)
	{
		case RELOPT_BASEREL:
		case RELOPT_OTHER_MEMBER_REL:
		{
			RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
			if (rte->rtekind != RTE_RELATION)
				elog(ERROR, "can not create remote path for ranges of type %d",
							rte->rtekind);
			rqpath->rqhas_temp_rel = IsTempTable(rte->relid);
			unshippable_quals = !pgxc_is_expr_shippable((Expr *)extract_actual_clauses(rel->baserestrictinfo, false),
														NULL);
		}
		break;

		case RELOPT_JOINREL:
		{
			rqpath->rqhas_temp_rel = leftpath->rqhas_temp_rel ||
									rightpath->rqhas_temp_rel;
			unshippable_quals = !pgxc_is_expr_shippable((Expr *)extract_actual_clauses(join_restrictlist, false),
														NULL);
		}
		break;

		default:
			elog(ERROR, "can not create remote path for relation of type %d",
							rel->reloptkind);
	}
	rqpath->rqhas_unshippable_qual = unshippable_quals;

	/* PGXCTODO - set cost properly */
	cost_remotequery(rqpath, root, rel);

	return rqpath;
}

/*
 * create_plainrel_rqpath
 * Create a RemoteQuery path for a plain relation residing on datanode/s and add
 * it to the pathlist in corresponding RelOptInfo. The function returns true, if
 * it creates a remote query path and adds it, otherwise it returns false.
 * The caller can decide whether to add the scan paths depending upon the return
 * value.
 */
extern bool
create_plainrel_rqpath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	RelationLocInfo	*rel_loc_info;
	List			*quals;
	ExecNodes		*exec_nodes;

	/*
	 * If we are on the Coordinator, we always want to use
	 * the remote query path unless relation is local to coordinator or the
	 * query is to entirely executed on coordinator.
	 */
	if (!IS_PGXC_COORDINATOR || IsConnFromCoord() || root->parse->is_local)
		return false;

	rel_loc_info = GetRelationLocInfo(rte->relid);
	quals = extract_actual_clauses(rel->baserestrictinfo, false);
	exec_nodes = GetRelationNodesByQuals(rte->relid, rel->relid,
														(Node *)quals,
														RELATION_ACCESS_READ);
	if (!exec_nodes)
		return false;
	/*
	 * If the table is replicated, we need to get all the node, and let the
	 * JOIN reduction pick up the appropriate.
	 * PGXC_TODO: This code is duplicated in
	 * pgxc_FQS_get_relation_nodes(). We need to move it inside
	 * GetRelationNodes().
	 * create_remotequery_plan would reduce the number of nodes to 1.
	 */
	if (IsRelationReplicated(rel_loc_info))
	{
		list_free(exec_nodes->nodeList);
		exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
	}

	/* We don't have subpaths for a plain base relation */
	add_path(rel, (Path *)create_remotequery_path(root, rel, exec_nodes,
													NULL, NULL, 0, NULL));
	return true;
}

/*
 * pgxc_find_remotequery_path
 * Search the path list for the rel for existence of a RemoteQuery path, return
 * if one found, NULL otherwise. There should be only one RemoteQuery path for
 * each rel, but we don't check for this.
 */
static RemoteQueryPath *
pgxc_find_remotequery_path(RelOptInfo *rel)
{
	ListCell *cell;

	foreach (cell, rel->pathlist)
	{
		Path *path = (Path *)lfirst(cell);
		if (IsA(path, RemoteQueryPath))
			return (RemoteQueryPath *)path;
	}
	return NULL;
}

/*
 * See if the nodelists corresponding to the RemoteQuery paths being joined can
 * be merged.
 */
ExecNodes *
pgxc_is_join_reducible(ExecNodes *inner_en, ExecNodes *outer_en, Relids in_relids,
						Relids out_relids, JoinType jointype, List *join_quals,
						List *rtables)
{
	ExecNodes 	*join_exec_nodes;
	bool		merge_dist_equijoin = false;
	bool		merge_replicated_only;
	ListCell	*cell;

	/*
	 * If either of inner_en or outer_en is NULL, return NULL. We can't ship the
	 * join when either of the sides do not have datanodes to ship to.
	 */
	if (!outer_en || !inner_en)
		return NULL;
	/*
	 * We only support reduction of INNER, LEFT [OUTER] and FULL [OUTER] joins.
	 * RIGHT [OUTER] join is converted to LEFT [OUTER] join during join tree
	 * deconstruction.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT && jointype != JOIN_FULL)
		return NULL;
	/*
	 * When join type is other than INNER, we will get the unmatched rows on
	 * either side. The result will be correct only in case both the sides of
	 * join are replicated. In case one of the sides is replicated, and the
	 * unmatched results are not coming from that side, it might be possible to
	 * ship such join, but this needs to be validated from correctness
	 * perspective.
	 */
	merge_replicated_only = (jointype != JOIN_INNER);

	/*
	 * If both the relations are distributed with similar distribution strategy
	 * walk through the restriction info for this JOIN to find if there is an
	 * equality condition on the distributed columns of both the relations. In
	 * such case, we can reduce the JOIN if the distribution nodelist is also
	 * same.
	 */
	if (IsExecNodesDistributedByValue(inner_en) &&
		inner_en->baselocatortype == outer_en->baselocatortype &&
		!merge_replicated_only)
	{
		foreach(cell, join_quals)
		{
			Node	*qual = (Node *)lfirst(cell);
			if (pgxc_qual_has_dist_equijoin(in_relids,
											out_relids, InvalidOid,
											qual, rtables) &&
				pgxc_is_expr_shippable((Expr *)qual, NULL))
			{
				merge_dist_equijoin = true;
				break;
			}
		}
	}
	/*
	 * If the ExecNodes of inner and outer nodes can be merged, the JOIN is
	 * shippable
	 * PGXCTODO: Can we take into consideration the JOIN conditions to optimize
	 * further?
	 */
	join_exec_nodes = pgxc_merge_exec_nodes(inner_en, outer_en,
											merge_dist_equijoin,
											merge_replicated_only);
	return join_exec_nodes;
}

/*
 * pgxc_ship_remotejoin
 * If there are RemoteQuery paths for the rels being joined, check if the join
 * is shippable to the datanodes, and if so, create a remotequery path for this
 * JOIN.
 */
extern void
create_joinrel_rqpath(PlannerInfo *root, RelOptInfo *joinrel,
						RelOptInfo *outerrel, RelOptInfo *innerrel,
						List *restrictlist, JoinType jointype,
						SpecialJoinInfo *sjinfo)
{
	RemoteQueryPath	*innerpath;
	RemoteQueryPath	*outerpath;
	ExecNodes		*inner_en;
	ExecNodes		*outer_en;
	ExecNodes		*join_en;
	List			*join_quals = NIL;
	List			*other_quals = NIL;

	/* If GUC does not allow remote join optimization, so be it */
	if (!enable_remotejoin)
		return;

	innerpath = pgxc_find_remotequery_path(innerrel);
	outerpath = pgxc_find_remotequery_path(outerrel);
	/*
	 * If one of the relation does not have RemoteQuery path, the join can not
	 * be shipped to the datanodes.
	 * If one of the relation has an unshippable qual, it needs to be evaluated
	 * before joining the two relations. Hence this JOIN is not shippable.
	 * PGXC_TODO: In case of INNER join above condition can be relaxed by
	 * attaching the unshippable qual to the join itself, and thus shipping join
	 * but evaluating the qual on join result. But we don't attempt it for now
	 */
	if (!innerpath || !outerpath ||
		innerpath->rqhas_unshippable_qual || outerpath->rqhas_unshippable_qual)
		return;

	inner_en = innerpath->rqpath_en;
	outer_en = outerpath->rqpath_en;

	if (!inner_en || !outer_en)
		elog(ERROR, "No node list provided for remote query path");
	/*
	 * Collect quals from restrictions so as to check the shippability of a JOIN
	 * between distributed relations.
	 */
	extract_actual_join_clauses(restrictlist, &join_quals, &other_quals);
	/*
	 * If the joining qual is not shippable and it's an OUTER JOIN, we can not
	 * ship the JOIN, since that would impact JOIN result.
	 */
	if (jointype != JOIN_INNER &&
		!pgxc_is_expr_shippable((Expr *)join_quals, NULL))
		return;
	/*
	 * For INNER JOIN there is no distinction between JOIN and non-JOIN clauses,
	 * so let the JOIN reduction algorithm take all of them into consideration
	 * to decide whether a JOIN is reducible or not based on quals (if
	 * required).
	 */
	if (jointype == JOIN_INNER)
		join_quals = list_concat(join_quals, other_quals);

	/*
	 * If the nodelists on both the sides of JOIN can be merged, the JOIN is
	 * shippable.
	 */
	join_en = pgxc_is_join_reducible(inner_en, outer_en,
										innerrel->relids, outerrel->relids,
										jointype, join_quals, root->parse->rtable);
	if (join_en)
		add_path(joinrel, (Path *)create_remotequery_path(root, joinrel, join_en,
													outerpath, innerpath, jointype,
													restrictlist));
	return;
}
