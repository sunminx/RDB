.PHONY: build clean test

refresh: clean build

run:
	@./RDB

build:
	go build

test:
	python3 -m unittest tests/*_test.py

clean:
	rm -rf RDB
