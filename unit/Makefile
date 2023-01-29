# -*- Mode: makefile-gmake -*-

all:
%:
	@$(MAKE) -C test_peer $*
	@$(MAKE) -C test_util $*

clean: unitclean
	rm -f coverage/*.gcov
	rm -fr coverage/report
