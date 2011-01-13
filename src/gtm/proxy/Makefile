# Copyright (c) 2010 Nippon Telegraph and Telephone Corporation

top_build_dir=../..
include $(top_build_dir)/gtm/Makefile.global

OBJS=proxy_main.o proxy_thread.o  ../common/libgtm.a ../libpq/libpqcomm.a ../client/libgtmclient.a ../path/libgtmpath.a ../recovery/libgtmrecovery.a ../client/libgtmclient.a
LDFLAGS=-L$(top_build_dir)/common -L$(top_build_dir)/libpq

LIBS=-lpthread

gtm_proxy:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ -o gtm_proxy

all:gtm_proxy

clean:
	rm -f $(OBJS)
	rm -f gtm_proxy

distclean: clean

maintainer-clean: distclean
