-module(amqpfs_util).
-export([path_to_matching_routing_key/1,path_to_routing_key/1, setup/1, setup_provider_queue/2, provider_queue_name/1, announce_queue_name/0, announce_queue_name/1, response_queue_name/0, response_queue_name/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

setup(Channel) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{
                                                                              exchange = <<"amqpfs.announce">>,
                                                                              type = <<"fanout">>,
                                                                              passive = false, durable = true,
                                                                              auto_delete = false, internal = false,
                                                                              nowait = false, arguments = []}),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{
                                                                              exchange = <<"amqpfs.response">>,
                                                                              type = <<"direct">>,
                                                                              passive = false, durable = false,
                                                                              auto_delete = false, internal = false,
                                                                              nowait = false, arguments = []}),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{
                                                                              exchange = <<"amqpfs">>,
                                                                              type = <<"topic">>,
                                                                              passive = false, durable = false,
                                                                              auto_delete = false, internal = false,
                                                                              nowait = false, arguments = []}).

setup_provider_queue(Channel, Name) ->
    Queue = provider_queue_name(Name),
    #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{
                                                                        queue = Queue,
                                                                        passive = false, durable = false,
                                                                        exclusive = false, auto_delete = false,
                                                                        nowait = false, arguments = []}).

announce_queue_name() ->
    announce_queue_name(node()).

announce_queue_name(Node) ->
    list_to_binary("amqpfs.announce:" ++ atom_to_list(Node)).

response_queue_name() ->
    response_queue_name(node()).

response_queue_name(Node) ->
    list_to_binary("amqpfs.response:" ++ atom_to_list(Node)).

provider_queue_name(Name) ->
    list_to_binary(term_to_string(Name) ++ ":" ++ atom_to_list(node())).
    

path_to_matching_routing_key(Path) ->
    list_to_binary(string:join(lists:map(fun base64:encode_to_string/1, string:tokens(Path,"/")),".") ++ [".#"]).

path_to_routing_key(Path) ->
    list_to_binary(string:join(lists:map(fun base64:encode_to_string/1, string:tokens(Path,"/")),".")).

term_to_string(T) when is_list(T) ->
    T;
term_to_string(T) when is_atom(T) ->
    atom_to_list(T);
term_to_string(T) when is_binary(T) ->
    binary_to_list(T).
