WORKERS := 30
PATH := $(PATH):/home/luigi-worker/bin

.PHONY: all
all:
	@PYTHONPATH='.:../..' luigi --module ems CincinnatiEMS --workers $(WORKERS)

