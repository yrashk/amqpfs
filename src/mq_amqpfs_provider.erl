-module(mq_amqpfs_provider). 

-export([init/1, list_dir/2, open/3, getattr/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

init(State) ->
    amqpfs_provider:announce(directory, "/mq", State),
    State.


list_dir("/mq", _State) ->
    [{"exchanges", {directory, on_demand}}];

list_dir("/mq/exchanges", _State) ->
    Exchanges = rpc:call('rabbit@hq', rabbit_exchange, info_all, [<<"/">>,[name]]),
    lists:filter(fun ({Name, _}) -> Name /= [] end, lists:map(fun ([{name, {resource, _, exchange, BinName}}]) -> {binary_to_list(BinName), {directory, on_demand}} end, Exchanges)).
    
open(_, _Fi, _State) ->
    ok.


getattr("/mq/exchanges",_State) ->
    #stat{ st_mode = ?S_IFDIR bor 8#0555, 
           st_nlink = 1,
           st_size = 0 }.
