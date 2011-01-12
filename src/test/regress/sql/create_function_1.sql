--
-- CREATE_FUNCTION_1
--

CREATE FUNCTION widget_in(cstring)
   RETURNS widget
   AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so'
   LANGUAGE C STRICT;

CREATE FUNCTION widget_out(widget)
   RETURNS cstring
   AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so'
   LANGUAGE C STRICT;

CREATE FUNCTION int44in(cstring)
   RETURNS city_budget
   AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so'
   LANGUAGE C STRICT;

CREATE FUNCTION int44out(city_budget)
   RETURNS cstring
   AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so'
   LANGUAGE C STRICT;

CREATE FUNCTION check_primary_key ()
	RETURNS trigger
	AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/refint.so'
	LANGUAGE C;

CREATE FUNCTION check_foreign_key ()
	RETURNS trigger
	AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/refint.so'
	LANGUAGE C;

CREATE FUNCTION autoinc ()
	RETURNS trigger
	AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/autoinc.so'
	LANGUAGE C;

CREATE FUNCTION funny_dup17 ()
        RETURNS trigger
        AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so'
        LANGUAGE C;

CREATE FUNCTION ttdummy ()
        RETURNS trigger
        AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so'
        LANGUAGE C;

CREATE FUNCTION set_ttdummy (int4)
        RETURNS int4
        AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so'
        LANGUAGE C STRICT;

-- Things that shouldn't work:

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'SELECT ''not an integer'';';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'not even SQL';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'SELECT 1, 2, 3;';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'SELECT $2;';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL
    AS 'a', 'b';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE C
    AS 'nosuchfile';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE C
    AS '/Users/masonsharp/dev/pgxc/postgres-xc/inst/lib/regress.so', 'nosuchsymbol';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE internal
    AS 'nosuch';
