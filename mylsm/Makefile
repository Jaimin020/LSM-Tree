MODULE_big = mylsm
OBJS = \
	$(WIN32RES) \
	mylsm.o

EXTENSION = test
DATA = mylsm.sql
PGFILEDESC = "test_checking"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/mylsm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
