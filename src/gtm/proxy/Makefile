# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_builddir=../../..
include $(top_builddir)/src/Makefile.global

OBJS=proxy_main.o proxy_thread.o proxy_utils.o gtm_proxy_opt.o

OTHERS= ../libpq/libpqcomm.a ../path/libgtmpath.a ../recovery/libgtmrecovery.a ../client/libgtmclient.a ../common/libgtm.a

LDFLAGS=-L$(top_builddir)/common -L$(top_builddir)/libpq

LIBS=-lpthread

gtm_proxy:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ $(OTHERS) ../../port/libpgport_srv.a -o gtm_proxy

all:gtm_proxy

clean:
	rm -f $(OBJS)
	rm -f gtm_proxy

distclean: clean

maintainer-clean: distclean
