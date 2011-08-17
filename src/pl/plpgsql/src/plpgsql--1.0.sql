/* src/pl/plpgsql/src/plpgsql--1.0.sql */

/*
 * Currently, all the interesting stuff is done by CREATE LANGUAGE.
 * Later we will probably "dumb down" that command and put more of the
 * knowledge into this script.
 */

CREATE PROCEDURAL LANGUAGE plpgsql;

/*
 * PGXC system view to look for prepared transaction GID list in a cluster
 */
CREATE FUNCTION pgxc_prepared_xact()
RETURNS setof text
AS $$
DECLARE
	num_nodes integer;
	i integer;
	num_nodes_text text;
	text_output text;
	row_data record;
	query_str text;
	BEGIN
		--Get total number of nodes
		SELECT INTO num_nodes_text setting FROM pg_settings WHERE name = 'num_data_nodes';
		num_nodes = num_nodes_text::integer;
		i := 1;
		WHILE i <= num_nodes LOOP
			query_str := 'EXECUTE DIRECT ON NODE ' || i || ' ''SELECT gid FROM pg_prepared_xact()''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data.gid;
			END LOOP;
			i := i + 1;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql';

CREATE VIEW pgxc_prepared_xacts AS
    SELECT DISTINCT * from pgxc_prepared_xact();
