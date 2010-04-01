# Copyright (c) 2010 Nippon Telegraph and Telephone Corporation

top_build_dir=../..
include $(top_build_dir)/gtm/Makefile.global

NAME=gtm


SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0


OBJS=aset.o mcxt.o elog.o assert.o stringinfo.o gtm_lock.o gtm_list.o

all:all-lib

include $(top_build_dir)/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libgtm.so libgtm.so.1 libgtm.so.1.0

distclean: clean

maintainer-clean: distclean
