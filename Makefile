.PHONY: build clean test

refresh: clean build

run:
	@./RDB

kill:
	@ps -elf | grep RDB | grep -v grep | awk '{print $$4}' | xargs kill -9
	@echo "the RDB process has exit."

build:
	go build -tags debug

test:
	python3 tests/run_tests.py

clean:
	rm -rf RDB
