PREFIX=/usr/local
DESTDIR=
GOFLAGS=
BINDIR=${PREFIX}/bin
DATADIR=${PREFIX}/share

NSQD_SRCS = $(wildcard nsqd/*.go nsq/*.go util/*.go util/pqueue/*.go)
NSQLOOKUPD_SRCS = $(wildcard nsqlookupd/*.go nsq/*.go util/*.go)
NSQADMIN_SRCS = $(wildcard nsqadmin/*.go util/*.go)
NSQ_PUBSUB_SRCS = $(wildcard examples/nsq_pubsub/*.go nsq/*.go util/*.go)
NSQ_TO_FILE_SRCS = $(wildcard examples/nsq_to_file/*.go nsq/*.go util/*.go)
NSQ_TO_HTTP_SRCS = $(wildcard examples/nsq_to_http/*.go nsq/*.go util/*.go)
NSQ_TAIL_SRCS = $(wildcard examples/nsq_tail/*.go nsq/*.go util/*.go)
NSQ_STAT_SRCS = $(wildcard examples/nsq_stat/*.go util/*.go util/lookupd/*.go)

BINARIES = nsqd nsqlookupd nsqadmin
EXAMPLES = nsq_pubsub nsq_to_file nsq_to_http nsq_tail nsq_stat
BLDDIR = build

all: $(BINARIES) $(EXAMPLES)

$(BLDDIR)/%:
	mkdir -p $(dir $@)
	cd $* && go build ${GOFLAGS} -o $(abspath $@)

$(BINARIES): %: $(BLDDIR)/%
$(EXAMPLES): %: $(BLDDIR)/examples/%

# Dependencies
$(BLDDIR)/nsqd: $(NSQD_SRCS)
$(BLDDIR)/nsqlookupd: $(NSQLOOKUPD_SRCS)
$(BLDDIR)/nsqadmin: $(NSQADMIN_SRCS)
$(BLDDIR)/examples/nsq_pubsub: $(NSQ_PUBSUB_SRCS)
$(BLDDIR)/examples/nsq_to_file: $(NSQ_TO_FILE_SRCS)
$(BLDDIR)/examples/nsq_to_http: $(NSQ_TO_HTTP_SRCS)
$(BLDDIR)/examples/nsq_tail: $(NSQ_TAIL_SRCS)
$(BLDDIR)/examples/nsq_stat: $(NSQ_STAT_SRCS)

clean:
	rm -fr $(BLDDIR)

# Targets
.PHONY: install clean all
# Programs
.PHONY: $(BINARIES)
# Examples
.PHONY: $(EXAMPLES)

install: $(BINARIES) $(EXAMPLES)
	install -m 755 -d ${DESTDIR}${BINDIR}
	install -m 755 $(BLDDIR)/nsqd ${DESTDIR}${BINDIR}/nsqd
	install -m 755 $(BLDDIR)/nsqlookupd ${DESTDIR}${BINDIR}/nsqlookupd
	install -m 755 $(BLDDIR)/nsqadmin ${DESTDIR}${BINDIR}/nsqadmin
	install -m 755 $(BLDDIR)/examples/nsq_pubsub ${DESTDIR}${BINDIR}/nsq_pubsub
	install -m 755 $(BLDDIR)/examples/nsq_to_file ${DESTDIR}${BINDIR}/nsq_to_file
	install -m 755 $(BLDDIR)/examples/nsq_to_http ${DESTDIR}${BINDIR}/nsq_to_http
	install -m 755 $(BLDDIR)/examples/nsq_tail ${DESTDIR}${BINDIR}/nsq_tail
	install -m 755 $(BLDDIR)/examples/nsq_stat ${DESTDIR}${BINDIR}/nsq_stat
	install -m 755 -d ${DESTDIR}${DATADIR}
	install -d ${DESTDIR}${DATADIR}/nsqadmin
	cp -r nsqadmin/templates ${DESTDIR}${DATADIR}/nsqadmin
