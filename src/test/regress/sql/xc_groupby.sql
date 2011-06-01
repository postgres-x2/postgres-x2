-- create required tables and fill them with data
create table tab1 (val int, val2 int);
create table tab2 (val int, val2 int);
insert into tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2;
-- joins and group by
select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2;
explain verbose select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from tab1 group by val2) q1 group by x;
explain verbose select sum(y) from (select sum(val) y, val2%2 x from tab1 group by val2) q1 group by x;
-- group by without aggregate
set enable_hashagg to off;
select val2 from tab1 group by val2;
explain verbose select val2 from tab1 group by val2;
select val + val2 from tab1 group by val + val2;
explain verbose select val + val2 from tab1 group by val + val2;
select val + val2, val, val2 from tab1 group by val, val2;
explain verbose select val + val2, val, val2 from tab1 group by val, val2;
select tab1.val + tab2.val2, tab1.val, tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val, tab2.val2;
explain verbose select tab1.val + tab2.val2, tab1.val, tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val, tab2.val2;
select tab1.val + tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val + tab2.val2;
explain verbose select tab1.val + tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val + tab2.val2;
reset enable_hashagg;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from tab1 group by val2;
explain verbose select count(*) + sum(val) + avg(val), val2 from tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from tab1 group by 2 * val2;
explain verbose select sum(val), avg(val), 2 * val2 from tab1 group by 2 * val2;
drop table tab1;
drop table tab2;

-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table tab1 (val int, val2 int) distribute by replication;
create table tab2 (val int, val2 int) distribute by replication;
insert into tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2;
explain verbose select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from tab1 group by val2;
-- joins and group by
select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2;
explain verbose select count(*), sum(tab1.val * tab2.val), avg(tab1.val*tab2.val), sum(tab1.val*tab2.val)::float8/count(*), tab1.val2, tab2.val2 from tab1 full outer join tab2 on tab1.val2 = tab2.val2 group by tab1.val2, tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from tab1 group by val2) q1 group by x;
explain verbose select sum(y) from (select sum(val) y, val2%2 x from tab1 group by val2) q1 group by x;
-- group by without aggregate
set enable_hashagg to off;
select val2 from tab1 group by val2;
explain verbose select val2 from tab1 group by val2;
select val + val2 from tab1 group by val + val2;
explain verbose select val + val2 from tab1 group by val + val2;
select val + val2, val, val2 from tab1 group by val, val2;
explain verbose select val + val2, val, val2 from tab1 group by val, val2;
select tab1.val + tab2.val2, tab1.val, tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val, tab2.val2;
explain verbose select tab1.val + tab2.val2, tab1.val, tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val, tab2.val2;
select tab1.val + tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val + tab2.val2;
explain verbose select tab1.val + tab2.val2 from tab1, tab2 where tab1.val = tab2.val group by tab1.val + tab2.val2;
reset enable_hashagg;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from tab1 group by val2;
explain verbose select count(*) + sum(val) + avg(val), val2 from tab1 group by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from tab1 group by 2 * val2;
explain verbose select sum(val), avg(val), 2 * val2 from tab1 group by 2 * val2;
drop table tab1;
drop table tab2;

-- some tests involving nulls, characters, float type etc.
create table def(a int, b varchar(25)); 
insert into def VALUES (NULL, NULL);
insert into def VALUES (1, NULL);
insert into def VALUES (NULL, 'One');
insert into def VALUES (2, 'Two');
insert into def VALUES (2, 'Two');
insert into def VALUES (3, 'Three');
insert into def VALUES (4, 'Three');
insert into def VALUES (5, 'Three');
insert into def VALUES (6, 'Two');
insert into def VALUES (7, NULL);
insert into def VALUES (8, 'Two');
insert into def VALUES (9, 'Three');
insert into def VALUES (10, 'Three');

select a,count(a) from def group by a order by a;
explain verbose select a,count(a) from def group by a order by a;
select avg(a) from def group by a; 
select avg(a) from def group by a;
explain verbose select avg(a) from def group by a;
select avg(a) from def group by b;
explain verbose select avg(a) from def group by b;
select sum(a) from def group by b;
explain verbose select sum(a) from def group by b;
select count(*) from def group by b;
explain verbose select count(*) from def group by b;
select count(*) from def where a is not null group by a;
explain verbose select count(*) from def where a is not null group by a;

select b from def group by b;
explain verbose select b from def group by b;
select b,count(b) from def group by b;
explain verbose select b,count(b) from def group by b;
select count(*) from def where b is null group by b;
explain verbose select count(*) from def where b is null group by b;

create table g(a int, b float, c numeric);
insert into g values(1,2.1,3.2);
insert into g values(1,2.1,3.2);
insert into g values(2,2.3,5.2);

select sum(a) from g group by a;
explain verbose select sum(a) from g group by a;
select sum(b) from g group by b;
explain verbose select sum(b) from g group by b;
select sum(c) from g group by b;
explain verbose select sum(c) from g group by b;

select avg(a) from g group by b;
explain verbose select avg(a) from g group by b;
select avg(b) from g group by c;
explain verbose select avg(b) from g group by c;
select avg(c) from g group by c;
explain verbose select avg(c) from g group by c;

drop table def;
drop table g;

-- same test with replicated tables
create table def(a int, b varchar(25)) distribute by replication; 
insert into def VALUES (NULL, NULL);
insert into def VALUES (1, NULL);
insert into def VALUES (NULL, 'One');
insert into def VALUES (2, 'Two');
insert into def VALUES (2, 'Two');
insert into def VALUES (3, 'Three');
insert into def VALUES (4, 'Three');
insert into def VALUES (5, 'Three');
insert into def VALUES (6, 'Two');
insert into def VALUES (7, NULL);
insert into def VALUES (8, 'Two');
insert into def VALUES (9, 'Three');
insert into def VALUES (10, 'Three');

select a,count(a) from def group by a order by a;
explain verbose select a,count(a) from def group by a order by a;
select avg(a) from def group by a; 
explain verbose select avg(a) from def group by a; 
select avg(a) from def group by a;
explain verbose select avg(a) from def group by a;
select avg(a) from def group by b;
explain verbose select avg(a) from def group by b;
select sum(a) from def group by b;
explain verbose select sum(a) from def group by b;
select count(*) from def group by b;
explain verbose select count(*) from def group by b;
select count(*) from def where a is not null group by a;
explain verbose select count(*) from def where a is not null group by a;

select b from def group by b;
explain verbose select b from def group by b;
select b,count(b) from def group by b;
explain verbose select b,count(b) from def group by b;
select count(*) from def where b is null group by b;
explain verbose select count(*) from def where b is null group by b;

create table g(a int, b float, c numeric) distribute by replication;
insert into g values(1,2.1,3.2);
insert into g values(1,2.1,3.2);
insert into g values(2,2.3,5.2);

select sum(a) from g group by a;
explain verbose select sum(a) from g group by a;
select sum(b) from g group by b;
explain verbose select sum(b) from g group by b;
select sum(c) from g group by b;
explain verbose select sum(c) from g group by b;

select avg(a) from g group by b;
explain verbose select avg(a) from g group by b;
select avg(b) from g group by c;
explain verbose select avg(b) from g group by c;
select avg(c) from g group by c;
explain verbose select avg(c) from g group by c;

drop table def;
drop table g;
