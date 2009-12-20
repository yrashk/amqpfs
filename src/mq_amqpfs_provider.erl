-module(mq_amqpfs_provider). 

-export([init/1, list_dir/2, object/2]).

-include_lib("amqpfs/include/amqpfs.hrl").

init(State) ->
    amqpfs_provider:announce(directory, "/mq", State),
    State.


list_dir("/mq", _State) ->
    [{"exchanges", {directory, on_demand}}];

list_dir("/mq/exchanges", State) ->
    lists:map(fun ([{name, {resource, _, exchange, BinName}}]) -> {binary_to_list(BinName), {directory, on_demand}} end, exchanges([name], State));


list_dir("/mq/exchanges/" ++ _Exchange, _State) ->
    [{"type", {file, on_demand}}].

object("/mq/exchanges/" ++ ExchangeKey, State) ->
    render_exchange(string:tokens(ExchangeKey,"/"), State).

render_exchange([Exchange, "type"], State) ->
    ExchangeBin = list_to_binary(Exchange),
    [ExchangePropList] = lists:filter(fun (PropList) -> proplists:get_value(name, PropList) == {resource,<<"/">>,exchange,ExchangeBin} end, exchanges([name,type], State)),
    atom_to_list(proplists:get_value(type, ExchangePropList));
render_exchange(_, State) ->
    <<>>.

exchanges(Info, #amqpfs_provider_state{ args = Args } = _State) ->
    Exchanges = rpc:call(proplists:get_value(amqp_broker_node, Args), rabbit_exchange, info_all, [<<"/">>,Info]),
    lists:filter(fun (PropList) -> proplists:get_value(name, PropList) =/= {resource,<<"/">>,exchange,<<>>} end, Exchanges).

