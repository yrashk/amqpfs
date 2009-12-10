-module(amqpfs_provider).
-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start/1]).

-export([init/1, handle_info/2, handle_call/3, handle_cast/2, code_change/3, terminate/2]).

-export([announce/3, announce/4]).

-include("rabbitmq-erlang-client/include/amqp_client.hrl").

behaviour_info(callbacks) ->
    [ 
      {amqp_credentials, 0},
      {init, 1},
      {list_dir, 2},
      {open, 2},
      {read, 4}
     ].

-record(amqpfs_provider,
        {
          module,
          connection,
          ticket,
          channel
         }).

start(Module) ->
    gen_server:start(?MODULE, [Module], []).

init([Module]) ->
    State0 = #amqpfs_provider{ module = Module },
    State1 = setup(State0),
    State2 = Module:init(State1),
    {ok, State2}.


handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    spawn(fun () -> handle_info_async(Msg, State) end),
    {noreply, State}.

handle_info_async({#'basic.deliver'{consumer_tag=ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs">>, routing_key=RoutingKey}, Content}, #amqpfs_provider{module = Module, channel = Channel, ticket = Ticket} = State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = Headers, message_id = MessageId} = Content#amqp_msg.props,
    Command =
        case ContentType of
            _ ->
                binary_to_term(Payload)
                end,
    case Command of
        {list, directory, Path} ->
            spawn(fun () -> amqp_channel:call(Channel, #'basic.publish'{ticket=Ticket, exchange= <<"amqpfs.response">>}, {amqp_msg, #'P_basic'{reply_to = MessageId}, term_to_binary(Module:list_dir(Path, State))}) end);
        {open, Path} ->
            spawn(fun () -> amqp_channel:call(Channel, #'basic.publish'{ticket=Ticket, exchange= <<"amqpfs.response">>}, {amqp_msg, #'P_basic'{reply_to = MessageId}, term_to_binary(Module:open(Path, State))}) end);
        {read, Path, Size, Offset} ->
            spawn(fun () -> amqp_channel:call(Channel, #'basic.publish'{ticket=Ticket, exchange= <<"amqpfs.response">>}, {amqp_msg, #'P_basic'{reply_to = MessageId}, term_to_binary(Module:read(Path, Size, Offset, State))}) end);
        Other ->
            io:format("Unknown request ~p~n",[Other])
    end;
handle_info_async(_, State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

terminate(_Reason, State) ->
    ok.
%%%%

announce(directory, Name, Contents, #amqpfs_provider{ module = Module, channel = Channel, ticket = Ticket} = State) ->
    setup_listener(Name, State),
    amqpfs_announce:directory(Channel, Ticket, Name, Contents).

announce(directory, Name, #amqpfs_provider{ module = Module, channel = Channel, ticket = Ticket} = State) ->
    setup_listener(Name, State),
    amqpfs_announce:directory(Channel, Ticket, Name);

announce(file, Name, #amqpfs_provider{ module = Module, channel = Channel, ticket = Ticket} = State) ->
    setup_listener(Name, State),
    amqpfs_announce:file(Channel, Ticket, Name).

%%%% 

setup(#amqpfs_provider{ module = Module }=State) ->
    Credentials = Module:amqp_credentials(),
    
    {ok, Connection} = erabbitmq_connections:start(#amqp_params{username = list_to_binary(proplists:get_value(username, Credentials, "guest")),
                                                                password = list_to_binary(proplists:get_value(password, Credentials, "guest")),
                                                                virtual_host = list_to_binary(proplists:get_value(virtual_host, Credentials, "/")),
                                                                host = proplists:get_value(host, Credentials, "localhost"),
                                                                port = proplists:get_value(port, Credentials, ?PROTOCOL_PORT),
                                                                ssl_options = proplists:get_value(ssl_options, Credentials, none)
                                                               }),
    {ok, Channel} = erabbitmq_channels:open(Connection),
    #'access.request_ok'{ticket = Ticket} = amqp_channel:call(Channel, #'access.request'{realm = list_to_binary(proplists:get_value(virtual_host, Credentials, "/")),  %% FIXME: is this right?
                                                                                         exclusive = false,
                                                                                         passive = true,
                                                                                         active = true,
                                                                                         write = true,
                                                                                         read = true}),
    amqpfs_util:setup(Channel, Ticket),
    amqpfs_util:setup_provider_queue(Channel, Ticket, Module),
    State#amqpfs_provider { connection = Connection, channel = Channel, ticket = Ticket }.
    


setup_listener(Name, #amqpfs_provider{ module = Module, channel = Channel, ticket = Ticket}) ->
    Queue = amqpfs_util:provider_queue_name(Module),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{ticket = Ticket,
                                                                  queue = Queue, exchange = <<"amqpfs">>,
                                                                  routing_key = amqpfs_util:path_to_matching_routing_key(Name),
                                                                  nowait = false, arguments = []}),
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:subscribe(Channel, #'basic.consume'{ticket = Ticket,
                                                                                                       queue = Queue,
                                                                                                       consumer_tag = <<"">>,
                                                                                                       no_local = false,
                                                                                                       no_ack = true,
                                                                                                       exclusive = false,
                                                                                                       nowait = false}, self()),
    receive
          #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end.

