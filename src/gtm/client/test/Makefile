#----------------------------------------------------------------------------
#
# Postgres-XC GTM client test makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/client/test/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(top_builddir)/src/gtm/client $(CPPFLAGS)

OBJS=test_seq.o test_txn.o test_snap.o test_txnperf.o test_snapperf.o
LIBS =-lpthread
LOADLIBES=-lpthread
CFLAGS=-g -O0

all:test_txn test_seq test_snap test_txnperf test_snapperf

test_txn:test_txn.o $(top_builddir)/src/gtm/client/libgtmclient.a

test_seq:test_seq.o $(top_builddir)/src/gtm/client/libgtmclient.a

test_snap:test_snap.o $(top_builddir)/src/gtm/client/libgtmclient.a

test_txnperf:test_txnperf.o $(top_builddir)/src/gtm/client/libgtmclient.a

test_snapperf:test_snapperf.o $(top_builddir)/src/gtm/client/libgtmclient.a

clean:
	rm -f $(OBJS)
	rm -f test_txn test_seq test_snap test_txnperf test_snapperf

distclean: clean

maintainer-clean: distclean
