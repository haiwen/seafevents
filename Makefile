all: seafevents.tar.gz

seafevents.tar.gz:
	mkdir seafevents
	cp *.py seafevents/
	tar czvf seafevents.tar.gz seafevents
	rm -rf seafevents

clean:
	rm -rf seafevents seafevents.tar.gz