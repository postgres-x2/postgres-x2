top_build_dir=../..
include $(top_build_dir)/gtm/Makefile.global

NAME=gtmpath
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

OBJS=path.o

all:all-lib

include $(top_build_dir)/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libgtmpath.so libgtmpath.so.1 libgtmpath.so.1.0 

distclean: clean

maintainer-clean: distclean

