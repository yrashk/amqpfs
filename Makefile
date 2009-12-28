GIT_BRANCH=$(shell git branch|egrep "^\*."|sed s/\*\ //)
GIT_BRANCH_REMOTE=$(shell git config branch.${GIT_BRANCH}.remote)
GIT_BASE_RAW=$(shell echo `git config remote.${GIT_BRANCH_REMOTE}.url` | sed s/amqpfs.git$$//)
GIT_BASE=$(strip ${GIT_BASE_RAW})

ifeq ($(shell uname),Darwin)
FUSE_CFLAGS = -D__DARWIN_64_BIT_INO_T=0 -mmacosx-version-min=10.5
ifeq ($(shell which erl),/opt/local/bin/erl)
FUSE_CFLAGS += -I/opt/local/lib/erlang/usr/include/
endif
endif

all: main

main: submodules
	erl -pa ebin vendor/fuserl/fuserl/src -make

submodules: init-submodules erabbitmq fuserl fuserldrv erlang-ossp-uuid

init-submodules:
	@cat gitmodules | awk '{ gsub(/%BASE%/,"$(GIT_BASE)"); print }' > .gitmodules
	@git submodule init
	@git submodule update

erlang-ossp-uuid: $(dir vendor/erlang-ossp-uuid)
	@cd vendor/erlang-ossp-uuid ; git submodule init
	@cd vendor/erlang-ossp-uuid ; git submodule update
	cd vendor/erlang-ossp-uuid ; $(MAKE)

erabbitmq: rabbitmq-erlang-client
	cd vendor/erabbitmq ; $(MAKE)

rabbitmq-erlang-client: vendor/rabbitmq-erlang-client/dist/amqp_client.ez

vendor/rabbitmq-erlang-client/dist/amqp_client.ez: $(dir vendor/rabbitmq-erlang-client/src)
	cd vendor/rabbitmq-codegen ; $(MAKE)
	cd vendor/rabbitmq-server ; $(MAKE)
	cd vendor/rabbitmq-erlang-client ; $(MAKE)

fuserl: vendor/fuserl/fuserl/Makefile $(dir vendor/fuserl/fuserl/src)
	cd vendor/fuserl/fuserl ; $(MAKE)

vendor/fuserl/fuserl/Makefile: $(dir vendor/fuserl/fuserl)
	cd vendor/fuserl/fuserl ; ./configure

fuserldrv: vendor/fuserl/fuserldrv/src/fuserldrv
	@true

vendor/fuserl/fuserldrv/src/fuserldrv: vendor/fuserl/fuserldrv/Makefile
	cd vendor/fuserl/fuserldrv ; $(MAKE)

vendor/fuserl/fuserldrv/Makefile: $(dir vendor/fuserl/fuserldrv)
	cd vendor/fuserl/fuserldrv ; CFLAGS="$(FUSE_CFLAGS)" ./configure

run: main
	@erl -sname amqpfs -pa samples ebin vendor/erlang-ossp-uuid/ebin vendor/fuserl/fuserl/src/ vendor/erabbitmq/ebin/ vendor/rabbitmq-erlang-client/ebin/ vendor/rabbitmq-server/ebin/ +A 32 -eval "application:load(fuserl)" -config configs/development -s erabbitmq -s amqpfs 

run-simple: main
	@erl -sname fstest -pa tests ebin vendor/erlang-ossp-uuid/ebin vendor/fuserl/fuserl/src/ vendor/erabbitmq/ebin/ vendor/rabbitmq-erlang-client/ebin/ vendor/rabbitmq-server/ebin/ +A 32 -eval "application:load(fuserl)" -config configs/development -s erabbitmq -s amqpfs -eval "amqpfs_provider:start(simple_amqpfs_provider)"

run-simple-sasl: main
	@erl -sname fstest -pa tests ebin vendor/erlang-ossp-uuid/ebin vendor/fuserl/fuserl/src/ vendor/erabbitmq/ebin/ vendor/rabbitmq-erlang-client/ebin/ vendor/rabbitmq-server/ebin/ -boot start_sasl +A 32 -eval "application:load(fuserl)" -config configs/development -s erabbitmq -s amqpfs -eval "amqpfs_provider:start(simple_amqpfs_provider)"

clean:
	rm -rf tmp
	rm -rf ebin/*.beam
	rm -rf tests/*.beam

rabbit:
	RABBITMQ_MNESIA_BASE=tmp RABBITMQ_LOG_BASE=tmp ./vendor/rabbitmq-server/scripts/rabbitmq-server

