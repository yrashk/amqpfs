all: main

main: submodules
	erl -pa ebin -make

submodules: erabbitmq fuserl fuserldrv

erabbitmq: rabbitmq-erlang-client
	cd vendor/erabbitmq ; $(MAKE)

rabbitmq-erlang-client: $(dir vendor/rabbitmq-erlang-client)
	cd vendor/rabbitmq-codegen ; $(MAKE)
	cd vendor/rabbitmq-server ; $(MAKE)
	cd vendor/rabbitmq-erlang-client ; $(MAKE)

fuserl: vendor/fuserl/fuserl/Makefile
	cd vendor/fuserl/fuserl ; $(MAKE)

vendor/fuserl/fuserl/Makefile: $(dir vendor/fuserl/fuserl)
	cd vendor/fuserl/fuserl ; ./configure

fuserldrv: vendor/fuserl/fuserldrv/src/fuserldrv
	@true

vendor/fuserl/fuserldrv/src/fuserldrv: vendor/fuserl/fuserldrv/Makefile
	cd vendor/fuserl/fuserldrv ; $(MAKE)

vendor/fuserl/fuserldrv/Makefile: $(dir vendor/fuserl/fuserldrv)
	cd vendor/fuserl/fuserldrv ; ./configure

run: main
	@erl -sname fstest -pa tests ebin vendor/fuserl/fuserl/src/ vendor/erabbitmq/ebin/ vendor/rabbitmq-erlang-client/ebin/ vendor/rabbitmq-server/ebin/ -boot start_sasl -eval "application:load(fuserl)" -config configs/development -s erabbitmq -s amqpfs

run-simple: main
	@erl -sname fstest -pa tests ebin vendor/fuserl/fuserl/src/ vendor/erabbitmq/ebin/ vendor/rabbitmq-erlang-client/ebin/ vendor/rabbitmq-server/ebin/ -boot start_sasl -eval "application:load(fuserl)" -config configs/development -s erabbitmq -s amqpfs -eval "amqpfs_provider:start(simple_amqpfs_provider)"


clean:
	rm -rf tmp
	rm -rf ebin/*.beam
	rm -rf tests/*.beam

rabbit:
	RABBITMQ_MNESIA_BASE=tmp RABBITMQ_LOG_BASE=tmp ./vendor/rabbitmq-server/scripts/rabbitmq-server
