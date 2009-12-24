all: main

main: submodules
	erl -pa ebin vendor/fuserl/fuserl/src -make

submodules: init-submodules erabbitmq fuserl fuserldrv erlang-ossp-uuid

init-submodules:
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
	cd vendor/fuserl/fuserldrv ; ./configure

run: main
	@erl -sname fstest -pa tests ebin vendor/erlang-ossp-uuid/ebin vendor/fuserl/fuserl/src/ vendor/erabbitmq/ebin/ vendor/rabbitmq-erlang-client/ebin/ vendor/rabbitmq-server/ebin/ -boot start_sasl -eval "application:load(fuserl)" -config configs/development -s erabbitmq -s amqpfs

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
