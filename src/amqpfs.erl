-module(amqpfs).
-export([start/0, stop/0]).

start() ->
    application:start(amqpfs).

stop() ->
    application:stop(amqpfs).
