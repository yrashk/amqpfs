all: main

main: submodules
	erl -make

submodules: erabbitmq fuserl

erabbitmq: rabbitmq-erlang-client
	cd vendor/erabbitmq ; $(MAKE)

rabbitmq-erlang-client: $(dir vendor/rabbitmq-erlang-client)
	cd vendor/rabbitmq-codegen ; $(MAKE)
	cd vendor/rabbitmq-server ; $(MAKE)
	cd vendor/rabbitmq-erlang-client ; $(MAKE)

fuserl: $(dir vendor/fuserl)
	cd vendor/fuserl/fuserldrv ; ./configure ; $(MAKE)
	cd vendor/fuserl/fuserl ; ./configure ; $(MAKE)

run:
	@erl -pa tests ebin vendor/fuserl/fuserl/src/ vendor/erabbitmq/ebin/ vendor/rabbitmq-erlang-client/ebin/ vendor/rabbitmq-server/ebin/ -boot start_sasl -eval "application:load(fuserl)" -config configs/development -s erabbitmq -s amqpfs
