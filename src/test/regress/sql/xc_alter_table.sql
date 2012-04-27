--
-- XC_ALTER_TABLE
--

-- Check on dropped columns
CREATE TABLE xc_alter_table_1 (id serial, name varchar(80), code varchar(80)) DISTRIBUTE BY HASH(id);
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_1(name) VALUES ('aaa'),('bbb'),('ccc');
INSERT INTO xc_alter_table_1(name) VALUES ('aaa'),('bbb'),('ccc');
SELECT id, name, code FROM xc_alter_table_1 ORDER BY 1;
-- Cannot drop distribution column
ALTER TABLE xc_alter_table_1 DROP COLUMN id;
-- Drop 1st column
ALTER TABLE xc_alter_table_1 DROP COLUMN code;
-- Check for query generation of remote INSERT
INSERT INTO xc_alter_table_1(name) VALUES('ddd'),('eee'),('fff');
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_1(name) VALUES('ddd'),('eee'),('fff');
SELECT id, name FROM xc_alter_table_1 ORDER BY 1;
-- Check for query generation of remote INSERT SELECT
INSERT INTO xc_alter_table_1(name) SELECT 'ggg';
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_1(name) SELECT 'ggg';
SELECT id, name FROM xc_alter_table_1 ORDER BY 1;
-- Check for query generation of remote UPDATE
EXPLAIN (VERBOSE true, COSTS false, NODES false) UPDATE xc_alter_table_1 SET name = 'zzz' WHERE id = currval('xc_alter_table_1_id_seq');
UPDATE xc_alter_table_1 SET name = 'zzz' WHERE id = currval('xc_alter_table_1_id_seq');
SELECT id, name FROM xc_alter_table_1 ORDER BY 1;
DROP TABLE xc_alter_table_1;

-- Check for multiple columns dropped and created
CREATE TABLE xc_alter_table_2 (a int, b varchar(20), c boolean, d text, e interval) distribute by replication;
INSERT INTO xc_alter_table_2 VALUES (1, 'John', true, 'Master', '01:00:10');
INSERT INTO xc_alter_table_2 VALUES (2, 'Neo', true, 'Slave', '02:34:00');
INSERT INTO xc_alter_table_2 VALUES (3, 'James', false, 'Cascading slave', '00:12:05');
SELECT a, b, c, d, e FROM xc_alter_table_2 ORDER BY a;
-- Go through standard planner
SET enable_fast_query_shipping TO false;
-- Drop a couple of columns
ALTER TABLE xc_alter_table_2 DROP COLUMN a;
ALTER TABLE xc_alter_table_2 DROP COLUMN d;
ALTER TABLE xc_alter_table_2 DROP COLUMN e;
-- Check for query generation of remote INSERT
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_2 VALUES ('Kodek', false);
INSERT INTO xc_alter_table_2 VALUES ('Kodek', false);
SELECT b, c FROM xc_alter_table_2 ORDER BY b;
-- Check for query generation of remote UPDATE
EXPLAIN (VERBOSE true, COSTS false, NODES false) UPDATE xc_alter_table_2 SET b = 'Morphee', c = false WHERE b = 'Neo';
UPDATE xc_alter_table_2 SET b = 'Morphee', c = false WHERE b = 'Neo';
SELECT b, c FROM xc_alter_table_2 ORDER BY b;
-- Add some new columns
ALTER TABLE xc_alter_table_2 ADD COLUMN a int;
ALTER TABLE xc_alter_table_2 ADD COLUMN a2 varchar(20);
-- Check for query generation of remote INSERT
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_2 (a, a2, b, c) VALUES (100, 'CEO', 'Gordon', true);
INSERT INTO xc_alter_table_2 (a, a2, b, c) VALUES (100, 'CEO', 'Gordon', true);
SELECT a, a2, b, c FROM xc_alter_table_2 ORDER BY b;
-- Check for query generation of remote UPDATE
EXPLAIN (VERBOSE true, COSTS false, NODES false) UPDATE xc_alter_table_2 SET a = 200, a2 = 'CTO' WHERE b = 'John';
UPDATE xc_alter_table_2 SET a = 200, a2 = 'CTO' WHERE b = 'John';
SELECT a, a2, b, c FROM xc_alter_table_2 ORDER BY b;
DROP TABLE xc_alter_table_2;
