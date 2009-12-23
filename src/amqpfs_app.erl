-module(amqpfs_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, StartArgs) ->
    amqpfs_util:print_banner(),
    case amqpfs_sup:start_link(StartArgs) of
        {ok, Pid} ->
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.
