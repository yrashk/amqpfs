-module(amqpfs_provider_base). 

-export([amqp_credentials/0, init/1, list_dir/2, open/2, read/4, getattr/2,
        allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

amqp_credentials() ->
    []. % default

init(State) ->
    State.


list_dir(_, _State) ->
    [].
    
open(_, _State) ->
    ok.

read(_Path, _Size, _Offset, _State) ->
    <<>>.

getattr(_,_State) ->
    #stat{ st_mode = ?S_IFREG bor 8#0444, 
           st_size = 0 }.

allow_request(_State) ->
    true.
