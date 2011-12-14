# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_builddir=../../..
include $(top_builddir)/src/Makefile.global

OBJS=gtm_ctl.o 

OTHERS=../common/libgtm.a ../libpq/libpqcomm.a ../client/libgtmclient.a ../path/libgtmpath.a

LDFLAGS=-L$(top_builddir)/common -L$(top_builddir)/libpq


LIBS=-lpthread

gtm_ctl:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ $(OTHERS) -o gtm_ctl

all:gtm_ctl

clean:
	rm -f $(OBJS)
	rm -f gtm_ctl

distclean: clean

maintainer-clean: distclean
