TARGZ = libevent.tar.gz
all: $(TARGZ)

$(TARGZ):
	git archive HEAD | gzip > $@

clean:
	rm -f $(TARGZ)