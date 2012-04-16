#----------------------------------------------------------------------------
#
# Postgres-XC documentation tool makefile
#
# Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
#
# doc-xc/Makefile
#
#----------------------------------------------------------------------------

subdir = doc-xc/tools
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = makesgml

all distprep html man install installdirs uninstall clean distclean maintainer-clean maintainer-check:
	$(MAKE) -C $(SUBDIRS) $@
