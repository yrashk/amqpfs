-module(amqpfs_util).
-export([path_to_matching_routing_key/1,path_to_routing_key/1, setup/1, setup_provider_queue/2, 
         provider_queue_name/1, announce_queue_name/0, announce_queue_name/1, announce_queue_name/2, response_queue_name/0, response_queue_name/1,
         response_routing_key/0,
         decode_payload/2,
         datetime_to_unixtime/1,
         concat_path/1,

         term_to_string/1,

         mount_point/0,
         mount_options/0,

         print_banner/0
        ]).

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
                                                                              nowait = false, arguments = []}),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{
                                                                              exchange = <<"amqpfs.provider">>,
                                                                              type = <<"direct">>,
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

announce_queue_name(Prefix) when is_list(Prefix) ->
    announce_queue_name(Prefix, node());

announce_queue_name(Node) when is_atom(Node) ->
    announce_queue_name("",node()).
   
announce_queue_name(Prefix, Node) ->
    list_to_binary("amqpfs.announce:" ++ Prefix ++ "/" ++ atom_to_list(Node)).

response_queue_name() ->
    response_queue_name(node()).

response_queue_name(Node) ->
    list_to_binary("amqpfs.response:" ++ atom_to_list(Node)).

provider_queue_name(Name) ->
    list_to_binary(term_to_string(Name) ++ ":" ++ atom_to_list(node())).

response_routing_key() ->    
    list_to_binary(atom_to_list(node()) ++ ":" ++ mount_point()).


path_to_matching_routing_key("/") ->
   <<"ROOT">>;

path_to_matching_routing_key(Path) ->
    list_to_binary(string:join(lists:map(fun base64:encode_to_string/1, string:tokens(Path,"/")),".") ++ [".#"]).

path_to_routing_key("/") ->
    <<"ROOT">>;

path_to_routing_key(Path) ->
    list_to_binary(string:join(lists:map(fun base64:encode_to_string/1, string:tokens(Path,"/")),".")).

term_to_string(T) when is_list(T) ->
    T;
term_to_string(T) when is_atom(T) ->
    atom_to_list(T);
term_to_string(T) when is_binary(T) ->
    binary_to_list(T);
term_to_string(T) ->
    lists:flatten(io_lib:format("~w",[T])).

decode_payload(ContentType, Payload) ->    
    case ContentType of
        ?CONTENT_TYPE_BERT ->
            binary_to_term(Payload);
        ?CONTENT_TYPE_BIN ->
            Payload;
        _ ->
            binary_to_term(Payload) % by default, attempt BERT, but FIXME: it might be a bad idea in a long run
    end.

-define(EPOCH_START, {{1970,1,1},{0,0,0}}).

datetime_to_unixtime(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime)-calendar:datetime_to_gregorian_seconds(?EPOCH_START).

concat_path(["/"|_]=P) ->
    lists:concat(P);
concat_path([H|T]) ->
    T1 = "/" ++ T,
    lists:flatten([H|T1]).

mount_point() ->
    filename:absname(proplists:get_value(mount_point, application:get_all_env(amqpfs),"/amqpfs")).

mount_options() ->
    proplists:get_value(mount_options, application:get_all_env(amqpfs), "").

print_banner() ->
    io:format("~n
                                ___    __  _______   ____  ___________
AMQPFS v0.1                    /   |  /  |/  / __ \\ / __ \\/ ____/ ___/
Copyright (c) 2009,           / /| | / /|_/ / / / // /_/ / /_   \\__ \\ 
 0840536 B.C. Ltd            / ___ |/ /  / / /_/ // ____/ __/  ___/ / 
``Filesystem as API``       /_/  |_/_/  /_/\\___\\_|_/   /_/    /____/  

Available under MIT License. 

").
                                          
