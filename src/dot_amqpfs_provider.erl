-module(dot_amqpfs_provider). 

-export([init/1, 
         open/2, read/4, getattr/2,
         allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

init(State) ->
    amqpfs_provider:announce(directory, "/.amqpfs", [], State),
    amqpfs_provider:announce(file, "/.amqpfs/version", State),
    State.

open("/.amqpfs/version", _State) ->
    ok.

read("/.amqpfs/version", Size, Offset, _State) ->
    list_to_binary(string:substr(?AMQPFS_VERSION, Offset + 1, Size)).

getattr("/.amqpfs/version", _State) ->
    #stat{ st_mode = ?S_IFREG bor 8#0444, 
           st_size = length(?AMQPFS_VERSION) }.


allow_request(#amqpfs_provider_state{request_headers = Headers}) ->
    {ok, Hostname} = inet:gethostname(),
    HostnameBin = list_to_binary(Hostname),
    case lists:keysearch(<<"hostname">>, 1, Headers) of 
        {value, {<<"hostname">>, _, HostnameBin}} ->
            true;
        _ ->
            false
    end.
