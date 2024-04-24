EXTENSION = vector
EXTVERSION = remote0.1.0

MODULE_big = vector
DATA = $(wildcard sql/*--*.sql)

C_SOURCES = $(wildcard src/*.c) $(wildcard src/remote/*.c) 
CPP_SOURCES = 

# providers
USE_MILVUS ?= 0
USE_PINECONE ?= 0
# milvus
ifeq ($(USE_MILVUS), 1)
	SHLIB_LINK += -lgrpc++ -lgrpc -lgrpc++_reflection -lprotobuf -ldl
	SHLIB_LINK += -lmilvus
	C_SOURCES += $(wildcard src/remote/clients/milvus/*.c)
	CPP_SOURCES += $(wildcard src/remote/clients/milvus/*.cpp)
	PG_CFLAGS += -DUSE_MILVUS
endif
# pinecone
ifeq ($(USE_PINECONE), 1)
	SHLIB_LINK += -lcurl
	C_SOURCES += $(wildcard src/remote/clients/pinecone/*.c)
	PG_CFLAGS += -DUSE_PINECONE
endif

C_OBJS = $(C_SOURCES:.c=.o)
CPP_OBJS = $(CPP_SOURCES:.cpp=.o)
SOURCES = $(C_SOURCES) $(CPP_SOURCES)
OBJS = $(C_OBJS) $(CPP_OBJS)

HEADERS = src/halfvec.h src/sparsevec.h src/vector.h 

TESTS = $(wildcard test/sql/*.sql)
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-extension=$(EXTENSION)

OPTFLAGS = -march=native -O0 -fno-strict-aliasing -DREMOTE_MOCK -g
PG_CFLAGS += -I$(srcdir)/src

PG_CPPFLAGS = $(shell $(PG_CONFIG) --cppflags)
PG_CPPFLAGS += -fno-exceptions

# Mac ARM doesn't always support -march=native
ifeq ($(shell uname -s), Darwin)
	ifeq ($(shell uname -p), arm)
		# no difference with -march=armv8.5-a
		OPTFLAGS = -O0 -fno-strict-aliasing -DREMOTE_MOCK -g
	endif
endif

# PowerPC doesn't support -march=native
ifneq ($(filter ppc64%, $(shell uname -m)), )
	OPTFLAGS =
endif

# For auto-vectorization:
# - GCC (needs -ftree-vectorize OR -O3) - https://gcc.gnu.org/projects/tree-ssa/vectorization.html
# - Clang (could use pragma instead) - https://llvm.org/docs/Vectorizers.html
PG_CFLAGS += $(OPTFLAGS) -ftree-vectorize -fassociative-math -fno-signed-zeros -fno-trapping-math

# Debug GCC auto-vectorization
# PG_CFLAGS += -fopt-info-vec

# Debug Clang auto-vectorization
# PG_CFLAGS += -Rpass=loop-vectorize -Rpass-analysis=loop-vectorize

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# for Mac
ifeq ($(PROVE),)
	PROVE = prove
endif

# for Postgres 15
PROVE_FLAGS += -I ./test/perl

prove_installcheck:
	rm -rf $(CURDIR)/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),test/t/*.pl)

.PHONY: dist

dist:
	mkdir -p dist
	git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ --output dist/$(EXTENSION)-$(EXTVERSION).zip master

# for Docker
PG_MAJOR ?= 16

.PHONY: docker

docker:
	docker build --pull --no-cache --build-arg PG_MAJOR=$(PG_MAJOR) -t pgvector/pgvector:pg$(PG_MAJOR) -t pgvector/pgvector:$(EXTVERSION)-pg$(PG_MAJOR) .

.PHONY: docker-release

docker-release:
	docker buildx build --push --pull --no-cache --platform linux/amd64,linux/arm64 --build-arg PG_MAJOR=$(PG_MAJOR) -t pgvector/pgvector:pg$(PG_MAJOR) -t pgvector/pgvector:$(EXTVERSION)-pg$(PG_MAJOR) .
