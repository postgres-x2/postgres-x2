#----------------------------------------------------------------------------
#
# Postgres-XC makesgml tool makefile
#
# Copyright(c) 2010-2012 Nippon Telegraph and Telephone Corporation
#
# doc-xc/tools/makesgml/Makefile
#
#-----------------------------------------------------------------------------

PGFILEDESC = "makesgml - convert .sgmlin to .sgml"
PGAPPICON=win32

subdir = doc-xc/tools/makesgml
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = makesgml.o

all distprep html man: makesgml

makesgml: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) makesgml$(X) '$(DESTDIR)$(bindir)/makesgml$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -rf '$(DESTDIR)$(bindir)/makesgml$(X)'

clean distclean maintenance-clean maintainer-clean:
	rm -rf makesgml$(X) $(OBJS)

makesgml.o: makesgml.c $(top_builddir)/src/Makefile.global
