# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_build_dir=../..
include $(top_build_dir)/gtm/Makefile.global

OBJS=gtm_ctl.o ../common/libgtm.a ../libpq/libpqcomm.a ../client/libgtmclient.a ../path/libgtmpath.a
LDFLAGS=-L$(top_build_dir)/common -L$(top_build_dir)/libpq

LIBS=-lpthread

gtm_ctl:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ -o gtm_ctl

all:gtm_ctl

clean:
	rm -f $(OBJS)
	rm -f gtm_ctl

distclean: clean

maintainer-clean: distclean
