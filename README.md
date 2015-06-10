# Postgres-X2
Postgres-XC is an advanced object-relational cluster database management system that supports an extended subset of the SQL standard, including transactions, foreign keys, user-defined types and functions.  This distribution also contains C language bindings.

# Project information
Please refer to the [Charter](http://postgres-x2.github.io/charter.html) for the project information

#License
Project license same as PostgreSQL, meaining [PostgreSQL license](http://www.postgresql.org/about/licence/) (like BSD).

#Features
* **Share nothing architecture**
* **Write scalable PostgreSQL cluster** : More than 3× scalability performance speedup with five servers, compared with pure PostgreSQL (DBT‐1). Result as of 1.0 release. Ways to improve more scalability are known.
* **Synchronous multi‐master configuration** : Any update to any master is visible from other masters immediately.
* **Table location transparent** : Can continue to use the same applications.No change in transaction handling.
* **Based upon PostgreSQL** :Same API to Apps as PostgreSQL

# Status
* **Latest release**
The latest release is Postgres-XC 1.2.1 based on PostgreSQL 9.3, you can download it from here [Postgres-XC 1.2.1](https://github.com/postgres-x2/postgres-x2/releases/tag/XC1_2_1_PG9_3)
* **The stable development version**
The stable development version is here [REL1_2_STABLE](https://github.com/postgres-x2/postgres-x2/tree/REL1_2_STABLE)
* **Next release**
Postgres-X2 1.2.2 is planned to be released at August 2015, please refer to the [1.2.2 milestone] (https://github.com/postgres-x2/postgres-x2/milestones/1.2.2)

# How to contribute
Subscribing grougle groups, issueing pull request and creating issues are the way to begin with. For more information please refer to [our development page](http://postgres-x2.github.io/developer.html)

* **MailList**

Subscribe Postgres-X2-dev group(postgres-x2-dev@googlegroups.com) from [google groups](https://groups.google.com/) titled "postgres-x2-dev". You can post your issues and ideas here.

* **Pull request and create issues**

If you have codes for new feature or fix, you can issue pull request. This page contains Postgres-X2 development community information. Posgres-X2 repositories will be found at [postgres-x2 developer page](https://github.com/postgres-x2).
You can also create issue to report bugs, to raise discussion, to post your ideas, etc.

* **Report Bugs**

[Report Issues] (https://github.com/postgres-x2/postgres-x2/issues)

# Keep in touch
For any project information please contact the project repensentive [Koichi Suzuki](mailto:koichi.dbms@gmail.com) or [Galy Lee](mailto:galylee@gmail.com)

# How to run
* **Download**

you can download the stable release  from here [Postgres-XC 1.2.1](https://github.com/postgres-x2/postgres-x2/releases/tag/XC1_2_1_PG9_3)

or you can download the stable development version, it is here [REL1_2_STABLE](https://github.com/postgres-x2/postgres-x2/tree/REL1_2_STABLE)
<pre><code>wget https://github.com/postgres-x2/postgres-x2/archive/REL1_2_STABLE.zip
</code></pre>

* **Installation**

*install depedency packages*
<pre><code>yum -y install gcc* libtool* libxml2-devel readline-devel flex bison crypto* perl-ExtUtils-Embed zlib-devel pam-devel libxslt-devel openldap-devel python-devel openssl-devel cmake</code></pre>

*Unzip*
<pre><code>unzip REL1_2_STABLE.zip</code></pre>

*Configure*
<pre><code>cd /home/galy/pgxc/stable (your source code place)
./configure --prefix=/home/galy/pgxc/stable </code></pre>
Please change the installation path to the location you want to install.

*make*
<pre><code>cd /home/galy/pgxc/stable (your source code place)
make install</code></pre>

* **Setup & Run**

The following is a quick example to setup one coordinator, two data nodes and one GTM
 
<pre><code>Init gtm, datanode, coordinator
  * initgtm -Z gtm -D gtm
  * initdb -D datanode1 --nodename dn1 #Initialize Datanode 1
  * initdb -D datanode2 --nodename dn2 #Initialize Datanode 2
  * initdb -D coord1 --nodename co1 # Initialize Coordinator 1

  Change configuration
  *Show and check gtm.conf and each postgresql.conf, change port values for Datanodes => 15432 for Dn1, 15433 for Dn2

  Node start-up
  * gtm -D gtm &
  * postgres -X -D datanode1 -i & # -X for a Datanode
  * postgres -X -D datanode2 -i & # -X for a Datanode
  * postgres -C -D coord1 -i & # -C for a Coordinator

  connect to coordinator
  *psql postgres

  launch that to set up cluster:
  * CREATE NODE dn1 WITH (TYPE='datanode', PORT=15432);
  * CREATE NODE dn2 WITH (TYPE='datanode', PORT=15433);
  * select * from pgxc_node;
  * select pgxc_pool_reload();

  Then you can connect to Coordinator 1 and test your newly-made cluster

</code></pre>

# Architecture

