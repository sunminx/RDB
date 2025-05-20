.PHONY: build clean test

refresh: clean build

run:
	@./RDB

build:
	go build -tags debug

test:
	python3 tests/run_tests.py

clean:
	rm -rf RDB
