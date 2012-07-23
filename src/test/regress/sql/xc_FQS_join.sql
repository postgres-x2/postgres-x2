-- This file contains testcases for JOINs, it does not test the expressions
-- create the tables first
select cr_table('tab1_rep (val int, val2 int)', '{1, 2, 3}'::int[], 'replication', NULL);
insert into tab1_rep (select * from generate_series(1, 5) a, generate_series(1, 5) b);
select cr_table('tab2_rep', '{2, 3, 4}'::int[], 'replication', 'as select * from tab1_rep');
select cr_table('tab3_rep', '{1, 3}'::int[], 'replication', 'as select * from tab1_rep');
select cr_table('tab4_rep', '{2, 4}'::int[], 'replication', 'as select * from tab1_rep');
select cr_table('tab1_mod', '{1, 2, 3}'::int[], 'modulo(val)', 'as select * from tab1_rep');
select cr_table('tab2_mod', '{2, 4}'::int[], 'modulo(val)', 'as select * from tab1_rep');
select cr_table('tab3_mod', '{1, 2, 3}'::int[], 'modulo(val)', 'as select * from tab1_rep');

-- Join involving replicated tables only, all of them should be shippable
select * from tab1_rep, tab2_rep where tab1_rep.val = tab2_rep.val and
										tab1_rep.val2 = tab2_rep.val2 and
										tab1_rep.val > 1 and tab1_rep.val < 4;
explain (costs off, num_nodes on, nodes off, verbose on) select * from tab1_rep, tab2_rep where tab1_rep.val = tab2_rep.val and
										tab1_rep.val2 = tab2_rep.val2 and
										tab1_rep.val > 3 and tab1_rep.val < 5;
select * from tab1_rep natural join tab2_rep
			where tab2_rep.val > 2 and tab2_rep.val < 5;
explain (costs off, num_nodes on, nodes off, verbose on) select * from tab1_rep natural join tab2_rep
			where tab2_rep.val > 2 and tab2_rep.val < 5;
select * from tab1_rep join tab2_rep using (val, val2) join tab3_rep using (val, val2)
									where tab1_rep.val > 0 and tab2_rep.val < 3;
explain (costs off, num_nodes on, nodes off, verbose on) select * from tab1_rep join tab2_rep using (val, val2) join tab3_rep using (val, val2)
							where tab1_rep.val > 0 and tab2_rep.val < 3;
select * from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
explain (costs off, num_nodes on, nodes off, verbose on) select * from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
-- make sure in Joins which are shippable and involve only one node, aggregates
-- are shipped to
select avg(tab1_rep.val) from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
explain (costs off, num_nodes on, nodes off, verbose on) select avg(tab1_rep.val) from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
-- the two replicated tables being joined do not have any node in common, the
-- query is not shippable
select * from tab3_rep natural join tab4_rep
			where tab3_rep.val > 2 and tab4_rep.val < 5;
explain (costs off, num_nodes on, nodes off, verbose on) select * from tab3_rep natural join tab4_rep
			where tab3_rep.val > 2 and tab4_rep.val < 5;

-- Join involving one distributed and one replicated table, with replicated
-- table existing on all nodes where distributed table exists. should be
-- shippable
select * from tab1_mod natural join tab1_rep
			where tab1_mod.val > 2 and tab1_rep.val < 4;
explain (costs off, verbose on, nodes off) select * from tab1_mod natural join tab1_rep
			where tab1_mod.val > 2 and tab1_rep.val < 4;

-- Join involving one distributed and one replicated table, with replicated
-- table existing on only some of the nodes where distributed table exists.
-- should not be shippable
select * from tab1_mod natural join tab4_rep
			where tab1_mod.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on, nodes off) select * from tab1_mod natural join tab4_rep
			where tab1_mod.val > 2 and tab4_rep.val < 4;

-- Join involving two distributed tables, never shipped
select * from tab1_mod natural join tab2_mod
			where tab1_mod.val > 2 and tab2_mod.val < 4;
explain (costs off, verbose on, nodes off) select * from tab1_mod natural join tab2_mod
			where tab1_mod.val > 2 and tab2_mod.val < 4;

-- Join involving a distributed table and two replicated tables, such that the
-- distributed table exists only on nodes common to replicated tables, try few
-- permutations
select * from tab2_rep natural join tab4_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on, nodes off) select * from tab2_rep natural join tab4_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
select * from tab4_rep natural join tab2_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on, nodes off) select * from tab4_rep natural join tab2_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
select * from tab2_rep natural join tab2_mod natural join tab4_rep
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on, nodes off) select * from tab2_rep natural join tab2_mod natural join tab4_rep
			where tab2_rep.val > 2 and tab4_rep.val < 4;

-- qualifications on distributed tables
-- In case of 2,3,4 datanodes following join should get shipped completely
select * from tab1_mod natural join tab4_rep where tab1_mod.val = 1 order by tab1_mod.val2;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_mod natural join tab4_rep where tab1_mod.val = 1 order by tab1_mod.val2;
-- following join between distributed tables should get FQSed because both of
-- them reduce to a single node
select * from tab1_mod join tab2_mod using (val2)
		where tab1_mod.val = 1 and tab2_mod.val = 2 order by tab1_mod.val2;
explain (costs off, verbose on, nodes off, num_nodes on) select * from tab1_mod join tab2_mod using (val2)
		where tab1_mod.val = 1 and tab2_mod.val = 2 order by tab1_mod.val;

-- JOIN involving the distributed table with equi-JOIN on the distributed column
-- with same kind of distribution on same nodes.
select * from tab1_mod, tab3_mod where tab1_mod.val = tab3_mod.val and tab1_mod.val = 1;
explain (costs off, verbose on, nodes off) select * from tab1_mod, tab3_mod
			where tab1_mod.val = tab3_mod.val and tab1_mod.val = 1;
-- DMLs involving JOINs are not FQSed
-- We need to just make sure that FQS is not kicking in. But the JOINs can still
-- be reduced by JOIN reduction optimization. Turn this optimization off so as
-- to generate plans independent of number of nodes in the cluster.
set enable_remotejoin to false;
explain (costs off, verbose on, nodes off) update tab1_mod set val2 = 1000 from tab2_mod
		where tab1_mod.val = tab2_mod.val and tab1_mod. val2 = tab2_mod.val2;
explain (costs off, verbose on, nodes off) delete from tab1_mod using tab2_mod
		where tab1_mod.val = tab2_mod.val and tab1_mod.val2 = tab2_mod.val2;
explain (costs off, verbose on, nodes off) update tab1_rep set val2 = 1000 from tab2_rep
		where tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2;
explain (costs off, verbose on, nodes off) delete from tab1_rep using tab2_rep
		where tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2;
reset enable_remotejoin;

drop table tab1_rep;
drop table tab2_rep;
drop table tab3_rep;
drop table tab4_rep;
drop table tab1_mod;
drop table tab2_mod;
