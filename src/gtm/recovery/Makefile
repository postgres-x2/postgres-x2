# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_builddir=../../..
include $(top_builddir)/src/Makefile.global


NAME=gtmrecovery
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

OBJS=register_common.o register_gtm.o replication.o standby_utils.o

OTHERS=../client/libgtmclient.a

all:all-lib

include $(top_builddir)/src/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libgtmrecovery.a libgtmrecovery.so libgtmrecovery.so.1 libgtmrecovery.so.1.0 

distclean: clean

maintainer-clean: distclean

