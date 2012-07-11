--
-- XC_CREATE_FUNCTIONS
--

-- Create a couple of functions used by Postgres-XC tests
-- A function to create table on specified nodes
create or replace function create_table_nodes(tab_schema varchar, nodenums int[], distribution varchar, cmd_suffix varchar)
returns void language plpgsql as $$
declare
	cr_command	varchar;
	nodes		varchar[];
	nodename	varchar;
	nodenames_query varchar;
	nodenames 	varchar;
	node 		int;
	sep			varchar;
	tmp_node	int;
	num_nodes	int;
begin
	nodenames_query := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D''';
	cr_command := 'CREATE TABLE ' || tab_schema || ' DISTRIBUTE BY ' || distribution || ' TO NODE ';
	for nodename in execute nodenames_query loop
		nodes := array_append(nodes, nodename);
	end loop;
	nodenames := '(';
	sep := '';
	num_nodes := array_length(nodes, 1);
	foreach node in array nodenums loop
		tmp_node := node;
		if (tmp_node < 1 or tmp_node > num_nodes) then
			tmp_node := tmp_node % num_nodes;
			if (tmp_node < 1) then
				tmp_node := num_nodes; 
			end if;
		end if;
		nodenames := nodenames || sep || nodes[tmp_node];
		sep := ', ';
	end loop;
	nodenames := nodenames || ')';
	cr_command := cr_command || nodenames;
	if (cmd_suffix is not null) then
		cr_command := cr_command  || ' ' || cmd_suffix;
	end if;
	execute cr_command;
end;
$$;

-- A function to return data node name given a node number
CREATE OR REPLACE FUNCTION get_xc_node_name(node_num int) RETURNS varchar LANGUAGE plpgsql AS $$
DECLARE
	r		pgxc_node%rowtype;
	node		int;
	nodenames_query	varchar;
BEGIN
	nodenames_query := 'SELECT * FROM pgxc_node  WHERE node_type = ''D'' ORDER BY xc_node_id';

	node := 1;
	FOR r IN EXECUTE nodenames_query LOOP
		IF node = node_num THEN
			RETURN r.node_name;
		END IF;
		node := node + 1;
	END LOOP;
	RETURN 'NODE_?';
END;
$$;

-- A function to check whether a certain transaction was prepared on a specific data node given its number
CREATE OR REPLACE FUNCTION is_prepared_on_node(txn_id varchar, nodenum int) RETURNS bool LANGUAGE plpgsql AS $$
DECLARE
	nodename	varchar;
	qry		varchar;
	r		pg_prepared_xacts%rowtype;
BEGIN
	nodename := (SELECT get_xc_node_name(nodenum));
	qry := 'EXECUTE DIRECT ON (' || nodename || ') ' || chr(39) || 'SELECT * FROM pg_prepared_xacts' || chr(39);

	FOR r IN EXECUTE qry LOOP
		IF r.gid = txn_id THEN
			RETURN true;
		END IF;
	END LOOP;
	RETURN false;
END;
$$;
