-module(dot_amqpfs_provider). 

-export([init/1, 
         open/2,
         object/2,
         allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

init(State) ->
    amqpfs_provider:announce(directory, "/.amqpfs", [], State),
    amqpfs_provider:announce(file, "/.amqpfs/version", State),
    State.

open("/.amqpfs/version", _State) ->
    ok.

object("/.amqpfs/version", _State) ->
    ?AMQPFS_VERSION.

allow_request(#amqpfs_provider_state{request_headers = Headers}) ->
    {ok, Hostname} = inet:gethostname(),
    HostnameBin = list_to_binary(Hostname),
    case lists:keysearch(<<"hostname">>, 1, Headers) of 
        {value, {<<"hostname">>, _, HostnameBin}} ->
            true;
        _ ->
            false
    end.
