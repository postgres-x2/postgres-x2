/*
 * Postgres-XC Cluster information
 *
 * Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * src/backend/catalog/cluster_nodes.sql
 */

-- PGXC default catalog node entries
CREATE NODE COORD_1 WITH (HOSTIP = 'localhost', COORDINATOR MASTER, NODEPORT = 5432);
CREATE NODE DATA_NODE_1 WITH (HOSTIP = 'localhost', NODE MASTER, NODEPORT = 15432);
CREATE NODE DATA_NODE_2 WITH (HOSTIP = 'localhost', NODE MASTER, NODEPORT = 25432);
