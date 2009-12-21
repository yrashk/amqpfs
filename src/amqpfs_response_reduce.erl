-module(amqpfs_response_reduce).
-export([first/1, concat/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

first([H|_]) ->
    H.

concat([{Thing,Ttl}|_]=L) when is_binary(Thing) ->
    L1 = erlang:list_to_binary(lists:map(fun({Thing1, _}) -> Thing1 end, L)),
    {L1, Ttl};

concat([{_Thing,Ttl}|_]=L) ->
    L1 = lists:concat(lists:map(fun({Thing1, _}) -> Thing1 end, L)),
    {L1, Ttl}.
