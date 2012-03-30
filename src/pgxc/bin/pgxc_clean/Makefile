#-------------------------------------------------------------------------
#
# Makefile for src/pgxc/bin/pgxc_clean
#
# Portions Copyright (c) 2011 Nippon Telegraph and Telephone Corporation
#
# $PostgreSQL$
#
#-------------------------------------------------------------------------

PGFILEDESC = "pgxc_clean - Abort prepared transaction for a Postgres-XC Coordinator"
subdir = src/pgxc/bin/pgxc_clean
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

gtm_builddir = $(top_builddir)/src/gtm

OBJS= pgxc_clean.o txninfo.o
EX_OBJS =	$(gtm_builddir)/common/assert.o \
			$(gtm_builddir)/client/libgtmclient.a

override LDFLAGS +=	-L$(top_builddir)/src/gtm/client \

# Switch when libxcm and libgtmclient are installed via 'make install'
# LIBS= -lpthread -lxcm -lgtmclient
LIBS= -lpthread

all: pgxc_clean

pgxc_clean: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) $(EX_OBJS) $(libpq_pgport) $(LDFLAGS) $(LIBS) -o $@$(X)

install: $(all)
	$(INSTALL_PROGRAM) pgxc_clean$(X) '$(DESTDIR)$(bindir)'/pgxc_clean$(X)

installdirs:
	$(mkinstalldirs) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, pgxc_clean$(X))

clean distclean maintainer-clean:
	rm -f pgxc_clean$(X) $(OBJS) pgxc_clean.o

