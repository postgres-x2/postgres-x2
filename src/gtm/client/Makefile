# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_builddir=../../..
include $(top_builddir)/src/Makefile.global


NAME=gtmclient
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

OBJS=fe-misc.o fe-connect.o pqexpbuffer.o ip.o strlcpy.o gtm_client.o fe-protocol.o
LDFLAGS=-L$(top_build_dir)/common -L$(top_build_dir)/libpq

LIBS=-lpthread

all:all-lib

include $(top_builddir)/src/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libgtmclient.a libgtmclient.so libgtmclient.so.1 libgtmclient.so.1.0

distclean: clean

maintainer-clean: distclean
