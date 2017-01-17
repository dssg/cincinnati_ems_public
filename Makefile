########################################
##            Variables               ##
########################################

##NOTE: Taken (shamelessly) from https://gist.github.com/prwhite/8168133, specifically from the comment of @nowox
HELP_FUN = \
	%help; \
	while(<>) { push @{$$help{$$2 // 'options'}}, [$$1, $$3] if /^(\w+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/ }; \
	print "uso: make [target]\n\n"; \
	for (keys %help) { \
		print "$$_:\n"; $$sep = " " x (20 - length $$_->[0]); \
		print "  $$_->[0]$$sep$$_->[1]\n" for @{$$help{$$_}}; \
		print  "\n"; }

########################################
##            Help                    ##
########################################
.PHONY: help
help:   ##@help    Prints help
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)

########################################
##      Python dependencies           ##
########################################

prepare:   ##@dependencies Prepare the environment for this project
	@pip install -r requirements-dev.txt
	@pip install -r requirements.txt

########################################
##      Execution tasks               ##
########################################
.PHONY: deploy
deploy: ##@deploy  Executes the pipeline
	$(MAKE) --directory=etl/pipeline

########################################
##      Documentation tasks           ##
########################################
.PHONY: docs
docs:   prepare ##@docs    Generate the documentation of the project
	@sphinx-apidoc -f -o docs utils
	@sphinx-apidoc -f -o docs etl/pipeline
	@sphinx-build -b html -d docs/_build/doctrees   docs docs/_build/html
