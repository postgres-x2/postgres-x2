# Copyright (c) 2010 Nippon Telegraph and Telephone Corporation

top_build_dir=../..
include $(top_build_dir)/gtm/Makefile.global


NAME=gtmrecovery
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

OBJS=register.o ../client/libgtmclient.a

all:all-lib

include $(top_build_dir)/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libgtmrecovery.a libgtmrecovery.so libgtmrecovery.so.1 libgtmrecovery.so.1.0 

distclean: clean

maintainer-clean: distclean

