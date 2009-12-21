-module(mq_amqpfs_provider). 

-export([init/1, list_dir/2, object/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

init(State) ->
    amqpfs_provider:announce(directory, "/mq", State),
    State.


list_dir("/mq", _State) ->
    [{"exchanges", {directory, on_demand}},{"connections", {directory, on_demand}}];

list_dir("/mq/exchanges", State) ->
    lists:map(fun ([{name, {resource, _, exchange, BinName}}]) -> {binary_to_list(BinName), {directory, on_demand}} end, exchanges([name], State));


list_dir("/mq/exchanges/" ++ ExchangeKey, State) ->
    list_exchange(string:tokens(ExchangeKey,"/"), State).

list_exchange([_Exchange], _State) ->
    [{"type", {file, on_demand}},{"bindings", {directory, on_demand}}];

list_exchange([Exchange, "bindings"], State) ->
    ExchangeBin = list_to_binary(Exchange),
    lists:map(fun ({{resource, <<"/">>, exchange, _ExchangeBin},
                    {resource, <<"/">>, queue, QueueBin},
                    _,_}) ->
                      {binary_to_list(QueueBin), {directory, on_demand}}
              end, bindings(ExchangeBin, State));

list_exchange([_Exchange, "bindings", _], _State) ->
    [{"routing_keys", {directory, on_demand}}];

list_exchange([Exchange, "bindings", Binding, "routing_keys"], State) ->
    ExchangeBin = list_to_binary(Exchange),
    QueueBin = list_to_binary(Binding),
    lists:map(fun ({_,_,RoutingKey,_}) ->
                      {binary_to_list(RoutingKey),{directory, on_demand}}
              end,
              lists:filter(fun ({{resource, <<"/">>, exchange, _ExchangeBinName},
                                 {resource, <<"/">>, queue, QueueBinName},
                                 RoutingKey, _}) ->
                                   (QueueBinName =:= QueueBin) and (RoutingKey =/= <<>>)
                           end,
                           bindings(ExchangeBin, State)));
list_exchange([_Exchange, "bindings", _Binding, "routing_keys", _RoutingKey], _State) ->
    [].

object("/mq/exchanges/" ++ ExchangeKey, State) ->
    render_exchange(string:tokens(ExchangeKey,"/"), State).

render_exchange([Exchange, "type"], State) ->
    ExchangeBin = list_to_binary(Exchange),
    [ExchangePropList] = lists:filter(fun (PropList) -> proplists:get_value(name, PropList) == {resource,<<"/">>,exchange,ExchangeBin} end, exchanges([name,type], State)),
    atom_to_list(proplists:get_value(type, ExchangePropList));

render_exchange(_, _State) ->
    <<>>.

exchanges(Info, #amqpfs_provider_state{ args = Args } = _State) ->
    Exchanges = rpc:call(proplists:get_value(amqp_broker_node, Args), rabbit_exchange, info_all, [<<"/">>,Info]),
    lists:filter(fun (PropList) -> proplists:get_value(name, PropList) =/= {resource,<<"/">>,exchange,<<>>} end, Exchanges).

bindings(ExchangeBin, #amqpfs_provider_state{ args = Args } = _State) ->
    Bindings = rpc:call(proplists:get_value(amqp_broker_node, Args), rabbit_exchange, list_bindings, [<<"/">>]),
    lists:filter(fun ({{resource, <<"/">>, exchange, ExchangeName},_,_RoutingKey,_}) -> ExchangeName =:= ExchangeBin end, Bindings).
