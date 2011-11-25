# Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation

top_builddir=../../..
subdir=src/gtm/common

include $(top_builddir)/src/Makefile.global

NAME=gtm

SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

LDFLAGS=-L$(top_builddir)/common -L$(top_builddir)/libpq
LIBS=-lpthread

OBJS=aset.o mcxt.o gtm_utils.o elog.o assert.o stringinfo.o gtm_lock.o gtm_list.o gtm_serialize.o gtm_serialize_debug.o

all:all-lib

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libgtm.so libgtm.so.1 libgtm.so.1.0

distclean: clean

maintainer-clean: distclean
