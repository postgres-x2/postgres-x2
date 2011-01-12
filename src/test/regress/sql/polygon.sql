--
-- POLYGON
--
-- polygon logic
--
-- 3	      o
--	      |
-- 2	    + |
--	   /  |
-- 1	  # o +
--       /    |
-- 0	#-----o-+
--
--	0 1 2 3 4
--

CREATE TABLE POLYGON_TBL(ID int, f1 polygon);


INSERT INTO POLYGON_TBL(ID,f1) VALUES (1, '(2.0,0.0),(2.0,4.0),(0.0,0.0)');

INSERT INTO POLYGON_TBL(ID,f1) VALUES (2, '(3.0,1.0),(3.0,3.0),(1.0,0.0)');

-- degenerate polygons 
INSERT INTO POLYGON_TBL(ID,f1) VALUES (3, '(0.0,0.0)');

INSERT INTO POLYGON_TBL(ID,f1) VALUES (4, '(0.0,1.0),(0.0,1.0)');

-- bad polygon input strings 
INSERT INTO POLYGON_TBL(ID,f1) VALUES (5, '0.0');

INSERT INTO POLYGON_TBL(ID,f1) VALUES (6, '(0.0 0.0');

INSERT INTO POLYGON_TBL(ID,f1) VALUES (7, '(0,1,2)');

INSERT INTO POLYGON_TBL(ID,f1) VALUES (8, '(0,1,2,3');

INSERT INTO POLYGON_TBL(ID,f1) VALUES (9, 'asdf');


SELECT '' AS four, * FROM POLYGON_TBL ORDER BY ID;

-- overlap 
SELECT '' AS three, p.*
   FROM POLYGON_TBL p
   WHERE p.f1 && '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

-- left overlap 
SELECT '' AS four, p.* 
   FROM POLYGON_TBL p
   WHERE p.f1 &< '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

-- right overlap 
SELECT '' AS two, p.* 
   FROM POLYGON_TBL p
   WHERE p.f1 &> '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

-- left of 
SELECT '' AS one, p.*
   FROM POLYGON_TBL p
   WHERE p.f1 << '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

-- right of 
SELECT '' AS zero, p.*
   FROM POLYGON_TBL p
   WHERE p.f1 >> '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

-- contained 
SELECT '' AS one, p.* 
   FROM POLYGON_TBL p
   WHERE p.f1 <@ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

-- same 
SELECT '' AS one, p.*
   FROM POLYGON_TBL p
   WHERE p.f1 ~= polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

-- contains 
SELECT '' AS one, p.*
   FROM POLYGON_TBL p
   WHERE p.f1 @> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' ORDER BY ID;

--
-- polygon logic
--
-- 3	      o
--	      |
-- 2	    + |
--	   /  |
-- 1	  / o +
--       /    |
-- 0	+-----o-+
--
--	0 1 2 3 4
--
-- left of 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' << polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS false;

-- left overlap 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' << polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS true;

-- right overlap 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' &> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS false;

-- right of 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' >> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS false;

-- contained in 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' <@ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS false;

-- contains 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' @> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS false;

-- same 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' ~= polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS false;

-- overlap 
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' && polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS true;

