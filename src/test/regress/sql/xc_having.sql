-- this file contains tests for HAVING clause with combinations of following
-- 1. enable_hashagg = on/off (to force the grouping by sorting)
-- 2. distributed or replicated tables across the datanodes
-- If a testcase is added to any of the combinations, please check if it's
-- applicable in other combinations as well.

-- Combination 1: enable_hashagg on and distributed tables
set enable_hashagg to on;
-- create required tables and fill them with data
create table tab1 (val int, val2 int);
create table tab2 (val int, val2 int);
insert into tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
-- having clause not containing any aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
-- having clause containing aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
-- joins and group by and having
select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
explain verbose select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
-- group by and having, without aggregate in the target list
select val2 from tab1 group by val2 having sum(val) > 8;
explain verbose select val2 from tab1 group by val2 having sum(val) > 8;
select val + val2 from tab1 group by val + val2 having sum(val) > 5;
explain verbose select val + val2 from tab1 group by val + val2 having sum(val) > 5;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
explain verbose select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
drop table tab1;
drop table tab2;

-- Combination 2, enable_hashagg on and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table tab1 (val int, val2 int) distribute by replication;
create table tab2 (val int, val2 int) distribute by replication;
insert into tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
-- having clause not containing any aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
-- having clause containing aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
-- joins and group by and having
select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
explain verbose select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
-- group by and having, without aggregate in the target list
select val2 from tab1 group by val2 having sum(val) > 8;
explain verbose select val2 from tab1 group by val2 having sum(val) > 8;
select val + val2 from tab1 group by val + val2 having sum(val) > 5;
explain verbose select val + val2 from tab1 group by val + val2 having sum(val) > 5;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
explain verbose select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
drop table tab1;
drop table tab2;

-- Combination 3 enable_hashagg off and distributed tables
set enable_hashagg to off;
-- create required tables and fill them with data
create table tab1 (val int, val2 int);
create table tab2 (val int, val2 int);
insert into tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
-- having clause not containing any aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
-- having clause containing aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
-- joins and group by and having
select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
explain verbose select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
-- group by and having, without aggregate in the target list
select val2 from tab1 group by val2 having sum(val) > 8;
explain verbose select val2 from tab1 group by val2 having sum(val) > 8;
select val + val2 from tab1 group by val + val2 having sum(val) > 5;
explain verbose select val + val2 from tab1 group by val + val2 having sum(val) > 5;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
explain verbose select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
drop table tab1;
drop table tab2;

-- Combination 4 enable_hashagg off and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table tab1 (val int, val2 int) distribute by replication;
create table tab2 (val int, val2 int) distribute by replication;
insert into tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
-- having clause not containing any aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having val2 + 1 > 3;
-- having clause containing aggregate
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 or val2 > 2;
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2 having avg(val) > 3.75 and val2 > 2;
-- joins and group by and having
select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
explain verbose select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2 having tab1.val2 + tab2.val2 > 2;
-- group by and having, without aggregate in the target list
select val2 from tab1 group by val2 having sum(val) > 8;
explain verbose select val2 from tab1 group by val2 having sum(val) > 8;
select val + val2 from tab1 group by val + val2 having sum(val) > 5;
explain verbose select val + val2 from tab1 group by val + val2 having sum(val) > 5;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
explain verbose select count(*) + sum(val) + avg(val), val2 from tab1 group by val2 having min(val) < val2;
drop table tab1;
drop table tab2;

reset enable_hashagg;
