all: seafevents.tar.gz

seafevents.tar.gz:
	git archive --prefix=seafevents/ -o $@ HEAD

clean:
	rm -f seafevents.tar.gz