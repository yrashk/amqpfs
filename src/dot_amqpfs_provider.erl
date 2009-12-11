-module(dot_amqpfs_provider). 
-behaviour(amqpfs_provider).

-export([amqp_credentials/0, init/1, list_dir/2, open/2, read/4, getattr/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

amqp_credentials() ->
    []. % default

init(State) ->
    amqpfs_provider:announce(directory, "/.amqpfs", [], State),
    amqpfs_provider:announce(file, "/.amqpfs/version", State),
    State.


list_dir(_, _State) ->
    [].

open("/.amqpfs/version", _State) ->
    ok.

read("/.amqpfs/version", Size, Offset, _State) ->
    list_to_binary(string:substr(?AMQPFS_VERSION, Offset + 1, Size)).

getattr("/.amqpfs/version", _State) ->
    #stat{ st_mode = ?S_IFREG bor 8#0444, 
           st_size = length(?AMQPFS_VERSION) }.
