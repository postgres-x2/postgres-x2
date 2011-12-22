# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_builddir=../../..
include $(top_builddir)/src/Makefile.global

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

OBJS=main.o gtm_thread.o gtm_txn.o gtm_seq.o gtm_snap.o gtm_time.o gtm_standby.o gtm_opt.o

OTHERS= ../libpq/libpqcomm.a ../path/libgtmpath.a ../recovery/libgtmrecovery.a ../client/libgtmclient.a ../common/libgtm.a ../../port/libpgport.a 

LDFLAGS=-L$(top_builddir)/common -L$(top_builddir)/libpq

LIBS=-lpthread

gtm:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ $(OTHERS) -o gtm

all:gtm

clean:
	rm -f $(OBJS)
	rm -f gtm

distclean: clean

maintainer-clean: distclean
