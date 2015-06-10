# Postgres-X2
Postgres-X2 is an open source project to provide horizontal scalability including write-scalability,
synchronous multi-master, and transparent PostgreSQL interface. It is a collection of tightly
coupled database components which can be installed in more than one hardware or virtual
machines.

# Project information
Please refer to the [Charter](http://postgres-x2.github.io/charter.html) for the project information

#License
Project license same as PostgreSQL, meaining [PostgreSQL license](http://www.postgresql.org/about/licence/) (like BSD).

#What is Postgres-X2
* **Share nothing Cluster** :Multiple collaborating PostgreSQL-like servers, No resources shared, Scaling by adding commodity hardware
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

* Before you start

Before you start to configure Postgres-XC cluster, you need to determine several things and make hardware/software ready.

**Components**

Postgres-XC database cluster consists of the following components:

*gtm*

gtm stands for "global transaction manager", which provides core of transaction management feature needed to run all the Postgres-XC component in a integrated way. Coordinators and datanodes connect to GTM to run transactions consisitently at different servers.

*gtm_proxy*

This is a proxy of a connection form coordinators/datanodes to GTM to reduce the amount of interaction and data.

*coordinator*
This is a connection point to Postgres-XC applications. A coordinator accepts SQL statements from applications, analyze and determines where the data is stored and handles SQL statements to each datanode. You can configure as many coordinators in Postgres-XC.

*datanode*

This node stores user data Datanode reads its local SQL statements from coordinators and handle them.

**How many servers you need?**

If you are just testing Postgres-XC, you need only one server. This can even be a virtual machine. You can run sufficient component on this machine but it's not a good idea to run slave of each components. If you're in this stage, please read this page carefully. Pgxc_ctl will provide configuration file template but it is not suitable for this purpose. You may have to rewrite many of the template to fit to your single-server configuration.
If you are deploying Postgres-XC for more serious use, you should consider how many servers you need to store your data. [[Scalability|The scalability data of XC] will help to determine this. Additionally, you may want to run GTM at a separate server mainly for availability purpose and you may want another server to run GTM slave for hight availability.It is highly advised to install gtm_proxy, coordinator and datanode at the rest of the servers. This simplifies the configuration and maintains worload of each servers nearly even.

* **Setup & Run**

The following is a quick example to setup one coordinator, two data nodes and one GTM
 
<pre><code>
 Init gtm, datanode, coordinator
  >initgtm -Z gtm -D gtm
  >initdb -D datanode1 --nodename dn1 #Initialize Datanode 1
  >initdb -D datanode2 --nodename dn2 #Initialize Datanode 2
  >initdb -D coord1 --nodename co1 # Initialize Coordinator 1

  Change configuration
  *Show and check gtm.conf and each postgresql.conf, change port values for Datanodes => 15432 for Dn1, 15433 for Dn2

  Node start-up
  >gtm -D gtm &
  >postgres -X -D datanode1 -i & # -X for a Datanode
  >postgres -X -D datanode2 -i & # -X for a Datanode
  >postgres -C -D coord1 -i & # -C for a Coordinator

  connect to coordinator
  >psql postgres

  launch that to set up cluster:
  >CREATE NODE dn1 WITH (TYPE='datanode', PORT=15432);
  >CREATE NODE dn2 WITH (TYPE='datanode', PORT=15433);
  >select * from pgxc_node;
  >select pgxc_pool_reload();

  Then you can connect to Coordinator 1 and test your newly-made cluster

</code></pre>

# Roadmap

Please refer to [roadmap](https://github.com/postgres-x2/postgres-x2/wiki/Roadmap) in our wiki page.

# Docs
* online reference

Postgres-X2 online reference is [here](http://postgres-x2.github.io/reference.html)

* Talks

There are a lot of talks materials [here](https://github.com/postgres-x2/postgres-x2.github.io/tree/master/presentation_docs)

# Architecture

* Architecture design 

 The architecture design and implementation detail is in this [document] (http://postgres-x2.github.io/presentation_docs/2014-07-PGXC-Implementation/pgxc.pdf)

* Internal Overview

 The offical document has an internal of Postgres-X2 [section] (http://postgres-x2.github.io/reference/1.2/html/xc-overview.html)
