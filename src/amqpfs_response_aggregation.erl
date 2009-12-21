-module(amqpfs_response_aggregation).
-export([first/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

first([H|_]) ->
    H.
