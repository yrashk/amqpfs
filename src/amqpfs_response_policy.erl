-module(amqpfs_response_policy).
-export([first/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

first(_Response, _State) ->
    last_response.

