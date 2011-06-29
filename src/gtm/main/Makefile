# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_build_dir=../..
include $(top_build_dir)/gtm/Makefile.global

OBJS=main.o gtm_thread.o gtm_txn.o gtm_seq.o gtm_snap.o gtm_time.o gtm_standby.o ../libpq/libpqcomm.a ../path/libgtmpath.a ../recovery/libgtmrecovery.a ../client/libgtmclient.a ../common/libgtm.a
LDFLAGS=-L$(top_build_dir)/common -L$(top_build_dir)/libpq

LIBS=-lpthread

gtm:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ -o gtm

all:gtm

clean:
	rm -f $(OBJS)
	rm -f gtm

distclean: clean

maintainer-clean: distclean
