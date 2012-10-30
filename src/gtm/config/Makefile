#----------------------------------------------------------------------------
#
# Postgres-XC GTM conf makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/config/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gtm/config

override CPPFLAGS := -I. -I$(libpq_srcdir) $(CPPFLAGS)

NAME = gtmconfig

SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

OBJS = gtm_opt_handler.o

all: all-lib

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

gtm_opt_handler.o: gtm_opt_scanner.c

gtm_opt_scanner.c: gtm_opt_scanner.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

# Note that gtm_opt_scanner.c is not deleted by make clean as we want it in distribution tarballs
clean:
	rm -f $(OBJS)
	rm -f rm -f libgtmconfig.so libgtmconfig.so.1 libgtmconfig.so.1.0

distclean: clean

maintainer-clean: distclean
