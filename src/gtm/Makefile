#----------------------------------------------------------------------------
#
# Postgres-XC GTM makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/Makefile
#
#-----------------------------------------------------------------------------
PGFILEDESC = "gtm - Global Transaction Manager for Postgres-XC"
subdir = src/gtm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = client common config libpq main path proxy recovery

# There are interdependencies between main and proxy, so
# don't attempt parallel make here.
.NOTPARALLEL:

$(recurse)
