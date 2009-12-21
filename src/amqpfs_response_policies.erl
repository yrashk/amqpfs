-module(amqpfs_response_policies).
-export([new/0,new/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

new() ->
    new([]).

new(PropList) ->
    lists:ukeysort(1,PropList ++ ?DEFAULT_RESPONSE_POLICIES).
