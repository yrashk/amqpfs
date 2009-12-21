-module(amqpfs_response_reduce).
-export([first/1, concat/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

first([H|_]) ->
    H.

concat([{Thing,_Ttl}|_]=L) when is_binary(Thing) ->
    L1 = erlang:list_to_binary(lists:map(fun({Thing1, _}) -> Thing1 end, L)),
    {L1, max_ttl(L)};

concat([{_Thing,_Ttl}|_]=L) ->
    L1 = lists:concat(lists:map(fun({Thing1, _}) -> Thing1 end, L)),
    {L1, max_ttl(L)}.

max_ttl(Things) ->
    lists:foldl(fun(ItemTtl, MaxTtl) -> if ItemTtl > MaxTtl -> ItemTtl; true -> MaxTtl end end, 0, lists:map(fun ({_,TtlVal}) -> TtlVal end, Things)).
