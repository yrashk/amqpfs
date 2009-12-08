all: submodules

submodules: rabbitmq-erlang-client fuserl

rabbitmq-erlang-client: $(dir vendor/rabbitmq-erlang-client)
	cd vendor/rabbitmq-codegen ; $(MAKE)
	cd vendor/rabbitmq-server ; $(MAKE)
	cd vendor/rabbitmq-erlang-client ; $(MAKE)

fuserl: $(dir vendor/fuserl)
	cd vendor/fuserl/fuserldrv ; ./configure ; $(MAKE)
	cd vendor/fuserl/fuserl ; ./configure ; $(MAKE)
