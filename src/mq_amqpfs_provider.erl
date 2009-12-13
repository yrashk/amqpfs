-module(mq_amqpfs_provider). 
-behaviour(amqpfs_provider).

-export([amqp_credentials/0, init/1, list_dir/2, open/2, read/4, getattr/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

amqp_credentials() ->
    []. % default

init(State) ->
    amqpfs_provider:announce(directory, "/mq", [], State),
    amqpfs_provider:announce(directory, "/mq/exchanges", State),
    State.


list_dir("/mq/exchanges", State) ->
    Exchanges = rpc:call('rabbit@hq', rabbit_exchange, info_all, [<<"/">>,[name]]),
    lists:filter(fun ({Name, _}) -> Name /= [] end, lists:map(fun ([{name, {resource, _, exchange, BinName}}]) -> {binary_to_list(BinName), {directory, on_demand}} end, Exchanges)).
    
open(_, _State) ->
    ok.



read(_, Size, Offset, _State) ->
    <<>>.

getattr("/mq/exchanges",_State) ->
    #stat{ st_mode = ?S_IFDIR bor 8#0555, 
           st_nlink = 1,
           st_size = 0 };

getattr(_,_State) ->
    #stat{ st_mode = ?S_IFREG bor 8#0444, 
           st_size = 0 }.
